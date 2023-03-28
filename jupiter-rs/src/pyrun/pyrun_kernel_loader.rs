//! Provides a loder to load ZIP files a PY.RUN kernels.
//!
//! A kernel is a zip file which contains a bunch of resources along with a `kernel.py` file which
//! is then executed via python.
use crate::platform::Platform;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use std::fmt::{Display, Formatter};

use crate::pyrun::{PyRun, PyRunCommand};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use zip::ZipArchive;
/// Represents the global loader instance to be registered.
pub struct KernelLoader {
    platform: Arc<Platform>,
}

impl KernelLoader {
    /// Creates the global loader instance to be registered via `Repository::register_loader`.
    pub fn new(platform: Arc<Platform>) -> Self {
        KernelLoader { platform }
    }
}

impl Display for KernelLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PYRUN-KERNEL")
    }
}

const PRELUDE: &str = include_str!("prelude.py");

#[async_trait::async_trait]
impl Loader for KernelLoader {
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let name = loader_info.get_config()["name"]
            .as_str()
            .context("Cannot load PyRun kernel, as no 'name' is specified in loader config.")?;
        let num_kernels = i64::min(
            u8::MAX as i64,
            loader_info.get_config()["num_kernels"]
                .as_i64()
                .unwrap_or(1),
        ) as u8;

        log::info!(
            "Loading {} as PyRun kernel '{}'...",
            loader_info.file_name(),
            name
        );

        let mut kernel_path = PathBuf::new();
        kernel_path.push(loader_info.get_data());

        // We need to wrap this as spawn_blocking, as rust_bert internally uses blocking IO...
        let directory = tokio::task::spawn_blocking(move || unzip_kernel(kernel_path))
            .await
            .context("Failed to extract ZIP archive!")??;

        let mut kernel_file = File::open(directory.path().join("kernel.py"))
            .await
            .context("Cannot find kernel.py in ZIP file!")?;
        let mut kernel_code = String::new();
        let _ = kernel_file
            .read_to_string(&mut kernel_code)
            .await
            .context("Cannot load kernel.py!")?;
        let mut file = File::create(directory.path().join("wrapper.py"))
            .await
            .context("Failed to create wrapper.py")?;

        file.write_all(kernel_code.as_bytes())
            .await
            .context("Failed to write kernel code")?;
        file.write_all(PRELUDE.as_bytes())
            .await
            .context("Failed to write prelude code")?;

        self.platform
            .require::<PyRun>()
            .perform(PyRunCommand::UpdateKernel(
                name.to_owned(),
                num_kernels,
                directory,
            ))
            .await;

        Ok(())
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }

    async fn file_deleted(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let name = loader_info.get_config()["name"]
            .as_str()
            .context("Cannot load PyRun kernel, as no 'name' is specified in loader config.")?;

        log::info!(
            "Dropping {} as PyRun kernel for {}",
            loader_info.file_name(),
            name
        );

        self.platform
            .require::<PyRun>()
            .perform(PyRunCommand::DropKernel(name.to_owned()))
            .await;

        Ok(())
    }
}

fn unzip_kernel(zip_archive: PathBuf) -> anyhow::Result<TempDir> {
    let kernel_directory = tempdir()?;

    let mut zip =
        ZipArchive::new(std::fs::File::open(&zip_archive).context("Failed to open ZIP archive")?)
            .context("Failed to parse ZIP archive")?;

    log::debug!(
        "Extracting top-tokens model {} into {}",
        zip_archive.to_string_lossy(),
        kernel_directory.path().to_string_lossy()
    );

    zip.extract(kernel_directory.path())?;

    Ok(kernel_directory)
}
