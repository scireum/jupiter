//! In charge of starting and running Python kernels.
//!
//! A kernel is a Python process which is started in a separate process and communicates with the
//! main process via `stdin` and `stdout`. The kernel is loaded from a ZIP archive which contains
//! the kernel code and all required resources.
use crate::average::Average;
use crate::commands::{queue, Queue, ResultExt};
use crate::commands::{Call, CommandResult, Endpoint};
use crate::fmt::format_short_duration;
use crate::spawn;
use anyhow::Context;
use arc_swap::ArcSwap;
use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdin, ChildStdout};

/// Enumerates the states a kernel can be in.
#[derive(PartialEq, Clone)]
pub enum KernelState {
    /// The kernel is starting up (last log message is contained as string)
    Starting(String),
    /// The kernel is idle and ready to accept new calls.
    Idle,
    /// The kernel is currently executing a call.
    Running(Instant, String),
    /// The kernel has been terminated.
    Terminated,
}

impl Display for KernelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KernelState::Starting(state) => write!(f, "Starting: {}", state),
            KernelState::Idle => write!(f, "Idle"),
            KernelState::Running(watch, command) => write!(
                f,
                "Executing {} ({})",
                command,
                format_short_duration(watch.elapsed().as_micros() as i32)
            ),
            KernelState::Terminated => write!(f, "Terminated"),
        }
    }
}

/// Describes the state of a kernel instance.
pub struct Kernel {
    name: String,
    kernel_id: u8,
    pid: ArcSwap<Option<u32>>,
    version: ArcSwap<Option<String>>,
    state: ArcSwap<KernelState>,
    duration: Average,
    forks: AtomicUsize,
    work_dir: Arc<TempDir>,
}

impl Kernel {
    /// Returns the name of the kernel.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the instance ID of the kernel.
    pub fn kernel_id(&self) -> u8 {
        self.kernel_id
    }

    /// Returns the PID of the kernel process.
    pub fn pid(&self) -> u32 {
        self.pid.load().unwrap_or(0)
    }

    /// Returns the version of the kernel.
    ///
    /// This is usually read from the global VERSION variable in the kernel code.
    pub fn version(&self) -> String {
        if let Some(version) = self.version.load().as_ref() {
            version.to_owned()
        } else {
            "-".to_owned()
        }
    }

    /// Yields the avarage call duration in microseconds.
    pub fn avg_duration(&self) -> i32 {
        self.duration.avg()
    }

    /// Returns the number of calls executed by the kernel.
    pub fn calls(&self) -> u64 {
        self.duration.count()
    }

    /// Returns how many times the kernel has been restarted due to failuers
    /// or crashes.
    pub fn forks(&self) -> usize {
        self.forks.load(Ordering::Relaxed)
    }

    /// Returns the current state of the kernel.
    pub fn state(&self) -> KernelState {
        self.state.load().as_ref().clone()
    }

    /// Returns the path to the working directory of the kernel.
    pub fn work_dir(&self) -> String {
        self.work_dir.path().to_string_lossy().to_string()
    }
}

/// Starts the given number of kernel instances within the given working directory.
pub async fn start_kernels(
    name: String,
    num_kernels: u8,
    temp_dir: Arc<TempDir>,
    kernel_states: &mut Vec<Arc<Kernel>>,
) -> Vec<Queue> {
    let mut kernels = Vec::with_capacity(num_kernels as usize);
    for kernel_id in 0..num_kernels {
        let (queue, kernel) = kernal_actor(name.clone(), kernel_id, temp_dir.clone());
        kernels.push(queue);
        kernel_states.push(kernel);
    }

    kernels
}

fn kernal_actor(name: String, kernel_id: u8, work_dir: Arc<TempDir>) -> (Queue, Arc<Kernel>) {
    let (cmd_queue, mut cmd_endpoint) = queue();
    let kernel = Arc::new(Kernel {
        name: name.clone(),
        kernel_id,
        pid: ArcSwap::new(Arc::new(None)),
        version: ArcSwap::new(Arc::new(None)),
        state: ArcSwap::new(Arc::new(KernelState::Starting("Booting...".to_owned()))),
        duration: Default::default(),
        forks: AtomicUsize::new(0),
        work_dir: work_dir.clone(),
    });

    let kernel_state = kernel.clone();

    spawn!(async move {
        loop {
            kernel_state.forks.store(
                kernel_state.forks.load(Ordering::Relaxed) + 1,
                Ordering::Relaxed,
            );

            match run_kernel(&name, &mut cmd_endpoint, &work_dir, &kernel_state).await {
                Ok(_) => {
                    return;
                }
                Err(error) => {
                    log::error!(
                        "Kernel {} ({}) crashed: {}",
                        kernel_state.name,
                        kernel_state.kernel_id,
                        error
                    );

                    log::info!("Waiting 10s before restarting kernel...");
                    // Wait 10 seconds before restarting, so that we don't fork a bazillion
                    // processes for a faulty script...
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }
    });

    (cmd_queue, kernel)
}

async fn run_kernel(
    name: &str,
    cmd_endpoint: &mut Endpoint,
    work_dir: &Arc<TempDir>,
    kernel: &Arc<Kernel>,
) -> anyhow::Result<()> {
    log::info!(
        "Starting kernel {} ({}) in {}...",
        name,
        kernel.kernel_id,
        work_dir.path().to_string_lossy()
    );

    // Actually forks the process...
    let mut child = tokio::process::Command::new("python")
        .arg("-u")
        .arg("wrapper.py")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .env_clear()
        .current_dir(work_dir.path())
        .spawn()
        .context("Failed to start python kernel")?;
    kernel.pid.store(Arc::new(child.id()));

    // Capture the stdin and stdout of the child process...
    let mut stdin = child.stdin.take().context("Failed to get stdin")?;
    let mut stdout = BufReader::new(child.stdout.take().context("Failed to get stdout")?);

    // Read the startup sequence from the kernel until "READY!" is received...
    record_startup_sequence(kernel, &mut child, &mut stdout).await?;

    log::info!(
        "Kernel {} ({}) is fully booted and running...",
        name,
        kernel.kernel_id
    );
    kernel.state.store(Arc::new(KernelState::Idle));

    // Runs the kernel, handling incoming calls. If the kernel exists, we yield an error so that
    // a new process is started. If the endpoint is closed, we yield Ok so that the kernel actor
    // terminates.
    loop {
        tokio::select! {
           call = cmd_endpoint.recv() => match call {
                Some(mut call) => handle_kernel_call(&mut call, &mut stdin, &mut stdout, kernel)
                    .await
                    .complete(call),
                            None => {
                    kernel.state.store(Arc::new(KernelState::Terminated));
                    let _ = stdin.write_all(b"EXIT!").await;
                    log::info!("Kernel {} ({}) is terminating...", name, kernel.kernel_id);
                    return Ok(());
                }
           },
           _ = child.wait() => {
                 return Err(anyhow::anyhow!("Kernel exited"));
           }
        }
    }
}

async fn record_startup_sequence(
    kernel: &Arc<Kernel>,
    child: &mut Child,
    stdout: &mut BufReader<ChildStdout>,
) -> anyhow::Result<()> {
    let mut line = String::new();
    while child.try_wait()?.is_none() && line != "READY!\n" {
        line.clear();
        let _ = stdout
            .read_line(&mut line)
            .await
            .context("Failed to receive ready signal from kernel")?;
        if let Some(version) = line.strip_prefix("VERSION:") {
            kernel
                .version
                .store(Arc::new(Some(version.trim().to_owned())));
        } else if line != "READY!\n" {
            kernel
                .state
                .store(Arc::new(KernelState::Starting(line.trim().to_owned())));
        }
    }

    Ok(())
}

async fn handle_kernel_call(
    call: &mut Call,
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
    kernel: &Arc<Kernel>,
) -> CommandResult {
    let input = call.request.str_parameter(1)?.replace('\n', " ");

    let watch = Instant::now();
    kernel
        .state
        .store(Arc::new(KernelState::Running(watch, input.to_owned())));

    stdin
        .write_all(input.as_bytes())
        .await
        .context("Failed to send query to kernel")?;
    let _ = stdin
        .write(&[b'\n'])
        .await
        .context("Failed to submit query to kernel")?;

    let mut line = String::new();
    let _ = stdout
        .read_line(&mut line)
        .await
        .context("Failed to receive answer from kernel")?;

    println!("{}", watch.elapsed().as_micros());
    kernel.duration.add(watch.elapsed().as_micros() as i32);
    call.response.bulk(line.trim_end())?;

    kernel.state.store(Arc::new(KernelState::Idle));

    Ok(())
}
