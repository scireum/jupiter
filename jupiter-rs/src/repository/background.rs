/// Contains the background worker of the repository.
///
/// As the repository has to execute some "long running" tasks like downloading data via HTTP
/// or executing loaders, we use this background worker to execute the tasks without blocking
/// the frontend worker, which is in charge of handling incoming commands.
///
/// Note that being a simple actor, each background task is executed after another. Once we believe
/// that the repository has changed (e.g. after downloading a file), we notify the frontend again.
use crate::platform::Platform;
use crate::repository::loader::LoaderInfo;
use crate::repository::{BackgroundEvent, FileEvent, Repository, RepositoryFile};
use anyhow::Context;
use chrono::DateTime;
use futures::TryStreamExt;
use hyper::{Body, Client, Request, Uri};
use hyper_tls::HttpsConnector;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::read_dir;
use tokio::fs::{DirEntry, File};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

/// Describes the commands supported by this actor.
#[derive(Debug)]
pub enum BackgroundCommand {
    /// Instructs the actor to scan the on-disk contents of the repository.
    Scan,

    /// Fetches a file via HTTP. Note that the file is only downloaded, if its `Last-Modified`
    /// header is newer, than the file on disk (or if it is missing).
    Fetch(String, String),

    /// Fetches a file via HTTP without any last modified checks.
    ForceFetch(String, String),

    /// Stores the given data into the given file.
    Store(String, Vec<u8>),

    /// Deletes the given file.
    Delete(String),

    /// Executes the given loader after the given file has changed.
    ExecuteLoaderForChange(LoaderInfo),

    /// Executes the given loader after the given file has been deleted.
    ExecuteLoaderForDelete(LoaderInfo),
}

/// Spawns an actor which listens on the returned inbound queue and will post updates into the given
/// outbound queue once the repository contents might have changed.
pub fn actor(
    platform: Arc<Platform>,
) -> (
    mpsc::Sender<BackgroundCommand>,
    mpsc::Receiver<BackgroundEvent>,
) {
    let (cmd_queue, mut cmds) = mpsc::channel(1024);
    let (mut change_notifier, changes) = mpsc::channel(1024);

    tokio::spawn(async move {
        let mut files = Vec::new();

        while platform.is_running() {
            if let Some(cmd) = cmds.recv().await {
                match cmd {
                    BackgroundCommand::Scan => {
                        files = scan_repository(files, &mut change_notifier).await
                    }
                    BackgroundCommand::Fetch(path, url) => {
                        log::info!("Fetching {} from {}...", path, url);
                        match fetch_file_command(&path, &url, false).await {
                            Ok(_) => {
                                log::info!("Download of {} completed...", path);
                                files = scan_repository(files, &mut change_notifier).await;
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to fetch data for {} from {}: {}",
                                    path,
                                    url,
                                    e
                                );
                            }
                        }
                    }
                    BackgroundCommand::ForceFetch(path, url) => {
                        log::info!("Fetching (forced) {} from {}...", path, url);
                        match fetch_file_command(&path, &url, true).await {
                            Ok(_) => {
                                log::info!("Download of {} completed...", path);
                                files = scan_repository(files, &mut change_notifier).await;
                            }
                            Err(e) => {
                                log::error!("Failed to fetch data for {} from {}: {}", path, url, e)
                            }
                        }
                    }
                    BackgroundCommand::Store(path, data) => {
                        log::info!("Updating contents of {}...", path);
                        match store_file_command(&path, data).await {
                            Ok(_) => {
                                log::info!("Contents of {} successfully updated...", path);
                                files = scan_repository(files, &mut change_notifier).await;
                            }
                            Err(e) => log::error!("Failed to store data for: {} - {}", path, e),
                        }
                    }
                    BackgroundCommand::Delete(path) => {
                        log::info!("Deleting {}...", path);
                        match delete_file_command(&path).await {
                            Ok(_) => {
                                log::info!("File {} successfully deleted...", path);
                                files = scan_repository(files, &mut change_notifier).await;
                            }
                            Err(e) => log::error!("Failed to delete {}: {}", path, e),
                        }
                    }
                    BackgroundCommand::ExecuteLoaderForChange(loader) => {
                        log::info!("Executing loader for updated file: {}", loader.file_name());
                        let start = Instant::now();

                        match loader.get_loader().file_changed(&loader).await {
                            Ok(_) => log::info!(
                                "Successfully loaded {} - Duration: {} ms",
                                loader.file_name(),
                                start.elapsed().as_millis()
                            ),
                            Err(e) => log::error!(
                                "Failed to execute loader for {}: {}",
                                loader.file_name(),
                                e
                            ),
                        }
                    }
                    BackgroundCommand::ExecuteLoaderForDelete(loader) => {
                        log::info!("Executing loader for deleted file: {}", loader.file_name());
                        let start = Instant::now();

                        match loader.get_loader().file_deleted(&loader).await {
                            Ok(_) => log::info!(
                                "Successfully unloaded {} - Duration: {} ms",
                                loader.file_name(),
                                start.elapsed().as_millis()
                            ),
                            Err(e) => log::error!(
                                "Failed to execute unload for {}: {}",
                                loader.file_name(),
                                e
                            ),
                        }
                    }
                }
            }
        }
    });

    (cmd_queue, changes)
}

async fn scan_repository(
    files: Vec<RepositoryFile>,
    change_notifier: &mut mpsc::Sender<BackgroundEvent>,
) -> Vec<RepositoryFile> {
    log::info!("Scanning repository contents...");
    match scan_repository_command(&files, change_notifier).await {
        Ok(new_files) => {
            if let Err(error) = change_notifier
                .send(BackgroundEvent::FileListUpdated(new_files.clone()))
                .await
            {
                log::error!(
                    "Failed to send new repository contents to the frontend: {}",
                    error
                );
                files
            } else {
                log::info!("Repository scan completed.");
                new_files
            }
        }
        Err(e) => {
            log::error!("Failed to scan repository: {}", e);
            files
        }
    }
}

async fn scan_repository_command(
    files: &[RepositoryFile],
    change_notifier: &mut mpsc::Sender<BackgroundEvent>,
) -> anyhow::Result<Vec<RepositoryFile>> {
    let base_path = Repository::base_dir().await;
    let updated_files = scan(base_path).await?;

    sync_lists(&updated_files, &files, change_notifier).await;

    Ok(updated_files)
}

/// Scans the given `base_path` and returns a list of detected files.
async fn scan(base_path: PathBuf) -> anyhow::Result<Vec<RepositoryFile>> {
    let mut num_files = 0;
    let mut num_directories = 0;

    log::info!("Scanning repository...");

    if base_path.is_dir() {
        let files =
            perform_repository_scan(base_path, &mut num_files, &mut num_directories).await?;

        log::info!(
            "Repository contents - files: {}, directories: {}",
            num_files,
            num_directories
        );

        Ok(files)
    } else {
        Err(anyhow::anyhow!("Repository does not exist!"))
    }
}

async fn perform_repository_scan(
    base_path: PathBuf,
    num_files: &mut i32,
    num_directories: &mut i32,
) -> anyhow::Result<Vec<RepositoryFile>> {
    let mut directories_to_scan = vec![("".to_string(), base_path)];
    let mut files = Vec::new();

    while let Some((prefix, path)) = directories_to_scan.pop() {
        *num_directories += 1;
        let mut entries = read_dir(path)
            .await
            .context("Failed to read directory contents")?;
        while let Some(file) = entries
            .next_entry()
            .await
            .context("Failed to fetch net directory entry")?
        {
            let raw_filename = file.file_name();
            let filename = raw_filename.to_string_lossy();

            // We skip over hidden files and directories like .git and we also do not want to
            // inspect files which are currently being
            if !filename.starts_with('.') && !filename.ends_with(".part") {
                if file.path().is_dir() {
                    directories_to_scan.push((format!("{}/{}", prefix, filename), file.path()));
                } else {
                    match collect_file(&file, &prefix, &mut files).await {
                        Ok(true) => *num_files += 1,
                        Err(e) => log::error!(
                            "Failed to check: {} - {:?}",
                            file.path().to_string_lossy(),
                            e
                        ),
                        _ => (),
                    }
                }
            }
        }
    }

    Ok(files)
}

async fn collect_file(
    file: &DirEntry,
    prefix: &str,
    files: &mut Vec<RepositoryFile>,
) -> anyhow::Result<bool> {
    let meta = file
        .metadata()
        .await
        .context("Failed to fetch file metadata")?;
    let last_modified = meta
        .modified()
        .context("Failed to fetch file modification timestamp")?;

    files.push(RepositoryFile {
        name: format!("{}/{}", prefix, file.file_name().to_string_lossy()),
        path: file.path(),
        size: meta.len(),
        last_modified,
    });

    Ok(true)
}

async fn sync_lists(
    current_files: &[RepositoryFile],
    previous_files: &[RepositoryFile],
    change_notifier: &mut mpsc::Sender<BackgroundEvent>,
) {
    for new_file in current_files {
        if previous_files
            .iter()
            .find(|other| &new_file == other)
            .filter(|other| new_file.size == other.size)
            .filter(|other| new_file.last_modified == other.last_modified)
            .is_none()
        {
            log::info!("New or updated file found in repository: {}", new_file.name);

            if let Err(error) = change_notifier
                .send(BackgroundEvent::FileEvent(FileEvent::FileChanged(
                    new_file.clone(),
                )))
                .await
            {
                log::error!(
                    "Failed to notify frontend about a change in {}: {}",
                    new_file.name,
                    error
                );
            }
        }
    }

    for old_file in previous_files {
        if current_files
            .iter()
            .find(|other| &old_file == other)
            .is_none()
        {
            log::info!("Deleted file found in repository: {}", old_file.name);

            if let Err(error) = change_notifier
                .send(BackgroundEvent::FileEvent(FileEvent::FileDeleted(
                    old_file.clone(),
                )))
                .await
            {
                log::error!(
                    "Failed to notify frontend about a the delete of {}: {}",
                    old_file.name,
                    error
                );
            }
        }
    }
}

async fn store_file_command(path: &str, data: Vec<u8>) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let effective_path = Repository::resolve(path)
        .await
        .context("Failed to resolve effective path.")?;

    if let Some(parent) = effective_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("Failed to create parent directories.")?;
    }

    let mut file = File::create(&effective_path)
        .await
        .context("Failed to open destination file.")?;
    file.write_all(&data)
        .await
        .context("Failed to write data to file.")?;
    file.flush().await.context("Failed flushing to disk")?;

    Ok(())
}

async fn delete_file_command(path: &str) -> anyhow::Result<()> {
    let effective_path = Repository::resolve(path)
        .await
        .context("Failed to resolve effective path.")?;

    tokio::fs::remove_file(effective_path).await?;

    Ok(())
}

async fn fetch_file_command(path: &str, url: &str, force: bool) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let effective_path = Repository::resolve(path)
        .await
        .context("Failed to resolved effective path.")?;

    if let Some(parent) = effective_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .context("Failed to create parent directories.")?;
    }

    if !force {
        if let Ok(metadata) = tokio::fs::metadata(&effective_path).await {
            if let Ok(file_modified) = metadata.modified() {
                let request = Request::builder()
                    .method("HEAD")
                    .uri(url)
                    .body(Body::empty())
                    .context("Failed to build request.")?;
                let response = if url.starts_with("https") {
                    let https = HttpsConnector::new();
                    let client = Client::builder().build::<_, Body>(https);
                    client
                        .request(request)
                        .await
                        .context("Failed fetching file headers.")?
                } else {
                    let client = Client::new();
                    client
                        .request(request)
                        .await
                        .context("Failed fetching file headers.")?
                };
                if let Some(Ok(last_modified)) = response
                    .headers()
                    .get(hyper::header::LAST_MODIFIED)
                    .map(|str| DateTime::parse_from_rfc2822(str.to_str().unwrap_or("")))
                {
                    if file_modified >= last_modified.into() {
                        log::info!(
                            "Now downloading {} as {} hasn't changed since its last download",
                            path,
                            url
                        );
                        return Ok(());
                    }
                }
            }
        }
    }

    let current_extension = if let Some(extension) = effective_path.extension() {
        extension.to_string_lossy().to_string()
    } else {
        "".to_string()
    };

    let mut tmp_path = effective_path.clone();
    tmp_path.set_extension(current_extension + ".part");
    let mut file = File::create(&tmp_path)
        .await
        .context("Failed to open destination file.")?;

    let response = if url.starts_with("https") {
        let https = HttpsConnector::new();
        let client = Client::builder().build::<_, Body>(https);
        client
            .get(Uri::from_str(url).context("Invalid uri")?)
            .await?
    } else {
        let client = Client::new();
        client
            .get(Uri::from_str(url).context("Invalid uri")?)
            .await?
    };

    let mut reader = to_tokio_async_read(
        response
            .into_body()
            .map(|result| {
                result.map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("Error: {}", error))
                })
            })
            .into_async_read(),
    );

    tokio::io::copy(&mut reader, &mut file).await?;
    file.flush().await.context("Failed to flush data to disk")?;

    if tokio::fs::metadata(&effective_path).await.is_ok() {
        tokio::fs::remove_file(&effective_path)
            .await
            .context("Failed to delete old file")?;
    }
    tokio::fs::rename(&tmp_path, &effective_path)
        .await
        .context("Failed to rename file to its effective name.")?;

    Ok(())
}

fn to_tokio_async_read(r: impl futures::io::AsyncRead) -> impl tokio::io::AsyncRead {
    tokio_util::compat::FuturesAsyncReadCompatExt::compat(r)
}
