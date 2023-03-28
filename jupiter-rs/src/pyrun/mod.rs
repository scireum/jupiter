//! Executes Python kernels in separate processes and sends JSON messaes back and forth.
//!
//! Using the `pyrun-kernel` loader, kernels can be loaded from ZIP archives. Next to all required
//! resources, the ZIP must contain a `kernel.py` file which contains the actual kernel code.
//!
//! A kernel roughly looks like this:
//! ```python
//!
//! VERSION = "v1.2.3"
//!
//! # (do heavy-lifting here...)...
//! # use print(..) to signal the progress of the startup sequence...
//!
//! def kernel(json):
//!     return { result: json["input"] * 2 }
//! ```
//! The kernel is automatically extended by the required glue code (see `prelude.py`) which takes
//! care of the inter-process communication. This is actually handled by reading and writing lines
//! to `stdin` from `stdout` of the forked process.
//!
//! The command `PY.RUN <kernel> <json>` can be used to execute a kernel. Note that the process
//! itself always keeps running, so heavy-lifiing (like laoding language models) should be done
//! outside of the kernel function.
//!
//! Using `PY.STATS` a list of all running kernels can be retrieved. This is useful for debugging
//! and monitoring purposes.
use crate::spawn;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

use crate::commands::{queue, CommandError, Queue, ResultExt};
use crate::commands::{Call, CommandDictionary, CommandResult, Endpoint};
use crate::fmt::format_short_duration;
use crate::platform::Platform;
use crate::pyrun::kernel::{Kernel, KernelState};
use crate::pyrun::pyrun_kernel_loader::KernelLoader;
use crate::repository::Repository;
use tokio::sync::mpsc::{Receiver, Sender};

mod kernel;
pub mod pyrun_kernel_loader;

/// Enumerates the commands supported by this actor.
#[derive(FromPrimitive)]
enum Commands {
    Call,
    Stats,
}

/// Describes the administrative commands supported by this actor.
pub enum PyRunCommand {
    /// Updates or installs a new kernel which resides in the given temp dir.
    /// Next to the name, the number of parallel instances is also given.
    UpdateKernel(String, u8, TempDir),

    /// Deletes / halts all kernels with the given name.
    DropKernel(String),
}

/// Describes the public API of the framework.
pub struct PyRun {
    sender: Sender<PyRunCommand>,
}

impl PyRun {
    /// Executes (enqueues) the given administrative command.
    pub async fn perform(&self, command: PyRunCommand) {
        if let Err(error) = self.sender.send(command).await {
            log::error!("Failed to enqueue administrative PyRun command: {}", error);
        }
    }
}

/// Installs the framework into the given platform.
///
/// If a commands dictionary is available, the following commands are registered: **PY.RUN** and
/// **PY.STATS**.
///
/// If a repository is available, the `pyrun-kernel` loader is registered.
pub fn install(platform: Arc<Platform>) {
    let (cmd_queue, cmd_endpoint) = queue();
    let (admin_sender, admin_receiver) = tokio::sync::mpsc::channel(16);
    let pyrun = Arc::new(PyRun {
        sender: admin_sender,
    });

    actor(cmd_endpoint, admin_receiver);

    let commands = platform.require::<CommandDictionary>();
    commands.register_command("PY.RUN", cmd_queue.clone(), Commands::Call as usize);
    commands.register_command("PY.STATS", cmd_queue, Commands::Stats as usize);

    platform.register::<PyRun>(pyrun);

    if let Some(repo) = platform.find::<Repository>() {
        repo.register_loader(
            "pyrun-kernel".to_owned(),
            Arc::new(KernelLoader::new(platform.clone())),
        );
    }
}

/// Spawns the actual actor which handles all commands or admin tasks.
fn actor(mut endpoint: Endpoint, mut admin_receiver: Receiver<PyRunCommand>) {
    spawn!(async move {
        let mut kernels = fnv::FnvHashMap::default();
        let mut kernel_states = Vec::default();

        loop {
            tokio::select! {
                   call = endpoint.recv() => match call {
                        Some(call) =>{
                            filter_terminated_kernels(&mut kernel_states);
                            handle_call(call, &kernels, &kernel_states).await;
                        }
                        None => return
                   },
                   cmd = admin_receiver.recv() => match cmd {
                        Some(cmd) => {
                            filter_terminated_kernels(&mut kernel_states);
                            handle_admin(cmd, &mut kernels, &mut kernel_states).await;
                        }
                        None => return
                   }
            }
        }
    });
}

fn filter_terminated_kernels(kernel_states: &mut Vec<Arc<Kernel>>) {
    kernel_states.retain(|kernel| kernel.state() != KernelState::Terminated);
}

async fn handle_call(
    mut call: Call,
    kernels: &fnv::FnvHashMap<String, (Vec<Queue>, AtomicU8)>,
    kernel_states: &Vec<Arc<Kernel>>,
) {
    let command = Commands::from_usize(call.token);

    match command {
        Some(Commands::Call) => run_command(call, kernels).await,
        Some(Commands::Stats) => stats_command(&mut call, kernel_states).complete(call),
        _ => call.handle_unknown_token(),
    }
}

async fn run_command(mut call: Call, kernels: &fnv::FnvHashMap<String, (Vec<Queue>, AtomicU8)>) {
    let kernel_name = if let Ok(name) = call.request.str_parameter(0) {
        name.to_owned()
    } else {
        call.complete(Err(CommandError::ClientError(anyhow::anyhow!(
            "Missing kernel name as first parameter!"
        ))));
        return;
    };

    if let Some((queues, next_index)) = kernels.get(&kernel_name) {
        // If multiple instances of the kernel are present, we round-robin between them...
        let index = next_index.load(Ordering::Relaxed);
        next_index.store(index % queues.len() as u8, Ordering::Relaxed);
        if let Err(error) = queues[index as usize].send(call).await {
            log::error!(
                "Failed to enqueue call to kernel '{}': {}",
                kernel_name,
                error
            );
        }
    } else {
        // We return a special message here, which can be handled by the client.
        // This is used to distinguish between a kernel which is not installed and a kernel which
        // crashed / is in an erroneous state.
        let result = call.response.bulk("UNKNWON_KERNEL");
        call.complete_output(result);
    }
}

fn stats_command(call: &mut Call, kernel_states: &Vec<Arc<Kernel>>) -> CommandResult {
    let mut result = String::new();

    result += format!(
        "{:<30} {:>2} {:>6} {:>15} {:>8}  {:>13} {:>5}\n",
        "Name", "ID", "PID", "Version", "Calls", "Avg. Duration", "Forks"
    )
    .as_str();
    result += crate::response::SEPARATOR;
    for kernel in kernel_states {
        result += format!(
            "{:<30} {:>2} {:>6} {:>15} {:>8} {:>13} {:>5}\n",
            kernel.name(),
            kernel.kernel_id(),
            kernel.pid(),
            kernel.version(),
            kernel.calls(),
            format_short_duration(kernel.avg_duration()),
            kernel.forks()
        )
        .as_str();
        result += format!("State     => {}\n", kernel.state()).as_str();
        result += format!("Directory => {}\n", kernel.work_dir()).as_str();
    }
    result += crate::response::SEPARATOR;

    call.response.bulk(result)?;

    Ok(())
}

/// Handles administrative commands while having exclusive access to the data structures
/// of the main actor.
async fn handle_admin(
    command: PyRunCommand,
    kernels: &mut fnv::FnvHashMap<String, (Vec<Queue>, AtomicU8)>,
    kernel_states: &mut Vec<Arc<Kernel>>,
) {
    match command {
        PyRunCommand::UpdateKernel(name, num_kernels, temp_dir) => {
            log::info!("New or updated kernel: {}", &name);
            let kernel_queues =
                kernel::start_kernels(name.clone(), num_kernels, Arc::new(temp_dir), kernel_states)
                    .await;
            let _ = kernels.insert(name.clone(), (kernel_queues, AtomicU8::new(0)));
        }
        PyRunCommand::DropKernel(name) => {
            log::info!("Dropping kernel: {}...", &name);
            let _ = kernels.remove(&name);
        }
    };
}
