//! The repository is responsible for storing, processing and updating data files used by other
//! parts of Jupiter.
//!
//! Locally the repository is simply a local directory. However, using the various commands the
//! contents of this directory can be inspected and updated (via HTTP downloads - e.g. from an
//! AWS S3 bucket).
//!
//! Once a file (its contents) change, one or more [Loaders](loader::Loader) are invoked to process
//! the updated data. To control which loaders process which file, metadata files are placed in the
//! directory **loaders**. Each of these files is a **YAML** file which configure the loader.
//!
//! # Example
//!
//! A loader descriptior could looke this. Note that each loader descriptor a namespace assigned so
//! that (e.g. when cloning data from git or s3) multiple Jupiters can operate on the same
//! datasource, even if only a part of the datasets is used.
//! ```yaml
//! loader: 'idb-csv'         # Use the CsvLoader
//! file: '/my-data/test.csv' # Process this file
//! namespace: 'core'         # Only perform a load if the "core" namespace is enabled in the config
//! table: 'test'             # Load the contents into the IDB test table.
//! indices: ['code']         # Create a lookup index for the column code.
//! fulltextIndices: ['name'] # Create a fulltext search index for the column name.
//! ```
//!
//! A set of base loaders is provided to load various data files into an one or more
//! [Infograph Tables](crate::idb::table::Table):
//! * [IdbYamlLoader](idb_yaml_loader::IdbYamlLoader) is used to load a given **YAML** file into
//!   an IDB table. This loader can be referenced as **idb-yaml**.
//! * [IdbJsonLoader](idb_json_loader::IdbJsonLoader) is used to load a given **JSON** file into
//!   an table. This loader can be referenced as **idb-json**.
//! * [IdbCsvLoader](idb_csv_loader::IdbCsvLoader) is used to load a given **CSV** file into
//!   an table. This loader can be referenced as **idb-csv**.
//!
//! Additionally a loader is defined to load [Infograph Sets](crate::idb::set::Set):
//! * [IdbYamlSetLoader](idb_yaml_set_loader::IdbYamlSetLoader) is used to load a given **YAML**
//!   file into the appropriate IDB sets. This loader can be referenced as **idb-yaml-sets**.
//!
//! Additional loaders can be registered via [register_loader](Repository::register_loader).
//!
//! # Architecture
//!
//! The repository is split up into three actors. The **foreground** actor (frontend) receives all
//! commands and processes them. As it is in charge of handling all incoming commands, none of them
//! must block for a long time. Also this actor owns the actual list of files in the repository.
//!
//! The **background** actor is in charge of handling all tasks which are potentially long-running.
//! This is mainly the task of downloading files and storing them onto disk. Also to manage
//! concurrency properly this is in charge of scanning the repository contants, deleting files and
//! it also handles executing the actual loaders for each file.
//!
//! The **loaders** actor is in charge of managing and detecting which loaders are active and which
//! need to reload or unload as their underlying data or metadata has changed.
//!
//! # Commands
//!
//! * **REPO.SCAN**: `REPO.SCAN` re-scans the local repository contents on the local disk. This
//!   automatically happens at startup and is only required if the contents of the repository are
//!   changed by an external process.
//! * **REPO.FETCH**: `REPO.FETCH file url` instructs the background actor to fetch a file from the
//!   given url. Note that the file will only been fetched if it has been modified on the server
//!   since it was last fetched.
//! * **REPO.STORE**: `REPO.STORE file contents` stores the given string contents in a file.
//! * **REPO.FETCH_FORCED**: `REPO.FETCH_FORCED file url` also fetches the given file, but doesn't
//!   perform any "last modified" checks as `REPO.FETCH` would.
//! * **REPO.LIST**: `REPO.LIST` lists all files in the repository. Note that this will yield a
//!   more or less human readable output where as `REPO.LIST raw` will return an array with
//!   provides a child array per file containing **filename**, **filesize**, **last modified**.
//! * **REPO.DELETE**: `REPO.DELETE file` deletes the given file from the repository.
//! * **REPO.INC_EPOCH**: `REPO.INC_EPOCH` immediately increments the epoch counter of the
//!   foreground actor and schedules a background tasks to increment the background epoch. Calling
//!   this after some repository tasks have been executed can be used to determine if all tasks have
//!   been handled.
//! * **REPO.EPOCHS**: `REPO.EPOCHS` reads the foreground and background epoch. Calling first
//!   `REPO.INC_EPOCH`and then `REPO.EPOCHS` one can determine if the background actor is currently
//!   working (downloading files or performing loader tasks) or if everything is handled. As
//!   **INC_EPOCH** is handled via the background loop, the returned epochs will differ, as long
//!   as the background actors is processing other tasks. Once the foreground epoch and the
//!   background one are the same, one can assume that all repository tasks have been handled.
//!   
//!
//! # Testing
//!
//! As the repository not only relies on the correct behaviour of each actor but also on their
//! orchestration, we provide most tests as integration tests below to ensure that the whole system
//! works as expected.
//!
//! # Examples
//!
//! In order to use a repository within Jupiter, simply create it, register the desired loaders
//! and install it:
//! ```no_run
//! # use jupiter::builder::Builder;
//! # use jupiter::repository::idb_yaml_loader::IdbYamlLoader;
//! # use std::sync::Arc;
//! # use jupiter::repository::create;
//! # use jupiter::server::Server;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a platform...
//!     let platform = Builder::new().enable_all().build().await;
//!     
//!     // Create a repository...
//!     let repository = jupiter::repository::create(&platform);
//!     
//!     // Install Jupiter or custom loaders...
//!     repository.register_loader("yaml".to_string(), Arc::new(IdbYamlLoader::new(platform.clone())));
//!     
//!     // Install the repository (this has to be done last, as this will perform the initial
//!     // repository scan, therefore the loaders have to be known...)..
//!     jupiter::repository::install(platform.clone(), repository);
//!
//!     // Start the main event loop...
//!     platform.require::<Server>().event_loop();
//! }
//! ```
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::Context;
use tokio::sync::broadcast;

use idb_yaml_loader::IdbYamlLoader;

use crate::commands::CommandDictionary;
use crate::platform::Platform;
use crate::repository::foreground::ForegroundCommands;
use crate::repository::idb_csv_loader::IdbCsvLoader;
use crate::repository::idb_json_loader::IdbJsonLoader;
use crate::repository::idb_yaml_set_loader::IdbYamlSetLoader;
use crate::repository::loader::{Loader, LoaderCommands};

mod background;
mod foreground;
pub mod idb_csv_loader;
pub mod idb_json_loader;
pub mod idb_yaml_loader;
pub mod idb_yaml_set_loader;
pub mod loader;

/// Represents a file within the repository.
#[derive(Clone, Debug)]
pub struct RepositoryFile {
    name: String,
    path: PathBuf,
    size: u64,
    last_modified: SystemTime,
}

impl Display for RepositoryFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

impl PartialEq for RepositoryFile {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

/// Events like this will be broadcast by the repository to all listeners which registered
/// themselves via [`Repository::listener`].
#[derive(Debug, Clone)]
pub enum FileEvent {
    /// Represents a change in the contents of the given file.
    FileChanged(RepositoryFile),

    /// Emitted if the given file has been deleted.
    FileDeleted(RepositoryFile),
}

/// Represents events which are sent back from the background worker to the frontend.
pub enum BackgroundEvent {
    /// Signals that a new list of repository files has been determined.
    FileListUpdated(Vec<RepositoryFile>),
    /// Signals that a file has most probably changed.
    FileEvent(FileEvent),
    /// Completes the roundtrip of a BackgroundCommand::EmitEpochCounter`
    EpochCounter(i64),
}

impl Display for FileEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileEvent::FileChanged(file) => write!(f, "Changed: {}", file),
            FileEvent::FileDeleted(file) => write!(f, "Deleted: {}", file),
        }
    }
}

/// Describes the type of receivers for events broadcast by the [Repository].
type FileEventReceiver = broadcast::Receiver<FileEvent>;

/// A repository is the central instance which connects the several actors together.
///
/// Note that a repository itself has nearly no public API. Rather, one or more custom
/// [Loaders](Loader) can be registered which are then invoked once an appropriate file
/// changes.
pub struct Repository {
    broadcast_sender: broadcast::Sender<FileEvent>,
    loaders: Mutex<HashMap<String, Arc<dyn Loader>>>,
}

impl Repository {
    fn new() -> Self {
        let (broadcast_sender, _) = broadcast::channel(128);

        Repository {
            broadcast_sender,
            loaders: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a loader.
    ///
    /// A loader can be referenced by a loader descriptor and will subsequently be invoked
    /// once the reference file or the loader descriptor itself changes.
    pub fn register_loader(&self, name: String, loader: Arc<dyn Loader>) {
        let _ = self.loaders.lock().unwrap().insert(name, loader);
    }

    /// Resolves the given name into a [Loader].
    pub fn find_loader(&self, name: &str) -> anyhow::Result<Arc<dyn Loader>> {
        self.loaders
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .context(format!("Unknown loader: {}", name))
    }

    #[cfg(not(test))]
    async fn base_dir() -> PathBuf {
        let path = Path::new("repository").to_path_buf();
        if let Err(error) = tokio::fs::create_dir_all(path.clone()).await {
            log::warn!(
                "Failed to create repository base directory {}: {}",
                path.to_string_lossy(),
                error
            )
        }

        path
    }
    #[cfg(test)]
    async fn base_dir() -> PathBuf {
        let mut path = Path::new("target").to_path_buf();
        path.push("test-repository");

        if let Err(error) = tokio::fs::create_dir_all(path.clone()).await {
            log::warn!(
                "Failed to create repository base directory {}: {}",
                path.to_string_lossy(),
                error
            )
        }

        path
    }

    /// Resolves the given relative path into an absolute one.
    pub async fn resolve(file_name: &str) -> anyhow::Result<PathBuf> {
        let mut result = Repository::base_dir().await;

        for element in file_name.split('/').filter(|path| !path.is_empty()) {
            result.push(element);
        }

        Ok(result)
    }

    fn listener(&self) -> FileEventReceiver {
        self.broadcast_sender.subscribe()
    }
}

/// Creates a new repository and installs some standard loaders.
///
/// These loaders are capable or loading YAML, JSON and CSV.
/// Note that create doesn't fully setup the repository, this is done va [install]. This is split
/// into two methods so that custom loaders can be registered (after creating it) but before
/// the initial repository scan is invoked (which happens in `install`).
pub fn create(platform: &Arc<Platform>) -> Arc<Repository> {
    let repo = Arc::new(Repository::new());
    platform.register::<Repository>(repo.clone());

    repo.register_loader(
        "idb-yaml".to_owned(),
        Arc::new(IdbYamlLoader::new(platform.clone())),
    );
    repo.register_loader(
        "idb-json".to_owned(),
        Arc::new(IdbJsonLoader::new(platform.clone())),
    );
    repo.register_loader(
        "idb-csv".to_owned(),
        Arc::new(IdbCsvLoader::new(platform.clone())),
    );

    repo.register_loader(
        "idb-yaml-sets".to_owned(),
        Arc::new(IdbYamlSetLoader::new(platform.clone())),
    );

    repo
}

/// Inserts the repository into the given platform and starts all required actors.
///
/// Note that all loaders have to be registered before installing the repository, as this will
/// perform the initial repository scan to determine what contents are already available.
pub fn install(platform: Arc<Platform>, repository: Arc<Repository>) {
    let (background_task_queue, update_notifier) = background::actor(platform.clone());
    let command_queue = foreground::actor(
        platform.clone(),
        repository.clone(),
        background_task_queue.clone(),
        update_notifier,
    );

    let loader_queue = loader::actor(platform.clone(), repository, background_task_queue);

    if let Some(commands) = platform.find::<CommandDictionary>() {
        commands.register_command(
            "REPO.SCAN",
            command_queue.clone(),
            ForegroundCommands::Scan as usize,
        );
        commands.register_command(
            "REPO.FETCH",
            command_queue.clone(),
            ForegroundCommands::Fetch as usize,
        );
        commands.register_command(
            "REPO.STORE",
            command_queue.clone(),
            ForegroundCommands::Store as usize,
        );
        commands.register_command(
            "REPO.FETCH_FORCED",
            command_queue.clone(),
            ForegroundCommands::ForceFetch as usize,
        );
        commands.register_command(
            "REPO.LIST",
            command_queue.clone(),
            ForegroundCommands::List as usize,
        );
        commands.register_command(
            "REPO.DELETE",
            command_queue.clone(),
            ForegroundCommands::Delete as usize,
        );
        commands.register_command(
            "REPO.EPOCHS",
            command_queue.clone(),
            ForegroundCommands::Epochs as usize,
        );
        commands.register_command(
            "REPO.INC_EPOCH",
            command_queue,
            ForegroundCommands::IncEpoch as usize,
        );
        commands.register_command("REPO.LOADERS", loader_queue, LoaderCommands::List as usize);
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::Builder;
    use crate::config::Config;
    use crate::platform::Platform;
    use crate::repository::{FileEvent, FileEventReceiver, Repository};
    use crate::server::Server;
    use crate::testing::{query_redis_async, test_async};
    use chrono::{TimeZone, Utc};
    use hyper::header::HeaderValue;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response};
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    async fn setup_env() -> (Arc<Platform>, Arc<Repository>) {
        // Ensure the repository will be empty...
        let base_dir = Repository::base_dir().await;
        if let Ok(metadata) = tokio::fs::metadata(base_dir.clone()).await {
            if metadata.is_dir() {
                tokio::fs::remove_dir_all(base_dir).await.unwrap();
            }
        }

        //  Setup and create a platform...
        let platform = Builder::new().enable_all().build().await;
        let repository = crate::repository::create(&platform);
        crate::repository::install(platform.clone(), repository.clone());

        // Specify a minimal config so that we run on a different port than a
        // production instance.
        platform
            .require::<Config>()
            .load_from_string(
                r#"
                     server:
                         port: 1503
                 "#,
                None,
            )
            .unwrap();

        // Fork the server in a separate thread..
        Server::fork_and_await(&platform.require::<Server>()).await;

        (platform, repository)
    }

    /// Expects to receive a change notification for the given listener within two seconds.
    ///
    /// Returns either the received event or None if no event was received during the timeout.
    async fn await_update(listener: &mut FileEventReceiver) -> Option<FileEvent> {
        tokio::select! {
            event = listener.recv() => match event {
                                          Ok(event) => Some(event),
                                          _ => None
                                       },
            _ = tokio::time::sleep(Duration::from_secs(2)) => None
        }
    }

    #[test]
    fn test_epochs() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            setup_env().await;
            query_redis_async(|con| redis::cmd("REPO.INC_EPOCH").query::<i64>(con)).await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            let (foreground_epoch, background_epoch) =
                query_redis_async(|con| redis::cmd("REPO.EPOCHS").query::<(i64, i64)>(con))
                    .await
                    .unwrap();

            assert_eq!(foreground_epoch, 2);
            assert_eq!(background_epoch, 2);
        });
    }

    #[test]
    fn ensure_repo_store_list_and_delete_works() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            let (platform, repository) = setup_env().await;
            let mut listener = repository.listener();

            // Ensure that the repo is indeed empty..
            let repo_contents = query_redis_async(|con| {
                redis::cmd("REPO.LIST")
                    .arg("raw")
                    .query::<Vec<Vec<(String, i64, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(repo_contents.len(), 0);

            // Programmatically store a file...
            query_redis_async(|con| {
                redis::cmd("REPO.STORE")
                    .arg("/test.yml")
                    .arg("test: true")
                    .query::<()>(con)
            })
            .await
            .unwrap();

            // Receive an update notification from the repository...
            if let Some(FileEvent::FileChanged(file)) = await_update(&mut listener).await {
                assert_eq!(file.name, "/test.yml")
            } else {
                panic!("Failed to receive file change event");
            }

            // And also ensure that the file has been picked up...
            let repo_contents = query_redis_async(|con| {
                redis::cmd("REPO.LIST")
                    .arg("raw")
                    .query::<Vec<Vec<(String, i64, String)>>>(con)
            })
            .await
            .unwrap();

            assert_eq!(repo_contents[0][0].0, "/test.yml");

            // Programmatically delete the file...
            query_redis_async(|con| redis::cmd("REPO.DELETE").arg("/test.yml").query::<()>(con))
                .await;

            // Receive an update notification from the repository...
            if let Some(FileEvent::FileDeleted(file)) = await_update(&mut listener).await {
                assert_eq!(file.name, "/test.yml")
            } else {
                panic!("Failed to receive file delete event");
            }

            // And also ensure that the file is gone...
            let repo_contents = query_redis_async(|con| {
                redis::cmd("REPO.LIST")
                    .arg("raw")
                    .query::<Vec<Vec<(String, i64, String)>>>(con)
            })
            .await
            .unwrap();
            assert_eq!(repo_contents.len(), 0);

            platform.terminate();
        });
    }

    #[test]
    fn ensure_loaders_work() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            let (platform, _) = setup_env().await;

            // To test our loaders, we also need InfoGraph DB to be online...
            crate::idb::install(platform.clone());

            // Programmatically store a file...
            query_redis_async(|con| {
                redis::cmd("REPO.STORE")
                    .arg("/test.yml")
                    .arg("test: true")
                    .query::<()>(con)
            })
            .await;

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // ...no loader visible yet...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents.len(), 0);

            // ...and a loader
            query_redis_async(|con| {
                redis::cmd("REPO.STORE")
                    .arg("/loaders/test.yml")
                    .arg(
                        r#"
                       file: "/test.yml"
                       namespace: "test"
                       loader: "idb-yaml"
                       table: "loader-test"
                       indices: ["code"]
                     "#,
                    )
                    .query::<()>(con)
            })
            .await;

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // The loader is present, but not active, as the namespace is missing...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents[0][0].0, "test.yml");
            assert_eq!(repo_contents[0][0].2, false);

            // Enable namespace...
            platform
                .require::<Config>()
                .load_from_string(
                    r#"
                     server:
                         port: 1503
                     repository:
                         namespaces: [ "test" ]
                 "#,
                    None,
                )
                .unwrap();

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // And also ensure that the file has been picked up...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents[0][0].0, "test.yml");
            assert_eq!(repo_contents[0][0].2, true);

            let last_load = &repo_contents[0][0].3;

            // Update the file, so that loaders are triggered again...
            query_redis_async(|con| {
                redis::cmd("REPO.STORE")
                    .arg("/test.yml")
                    .arg("test: true")
                    .query::<()>(con)
            })
            .await;

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // And also ensure that the file has been picked up...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents[0][0].0, "test.yml");
            assert_eq!(repo_contents[0][0].2, true);
            // Ensure that the "last_load" timestamp changed...
            assert_ne!(&repo_contents[0][0].3, last_load);

            // Update the loader description which should also trigger an update...
            query_redis_async(|con| {
                redis::cmd("REPO.STORE")
                    .arg("/loaders/test.yml")
                    .arg(
                        r#"
                       file: "/test.yml"
                       namespace: "test"
                       loader: "idb-yaml"
                       table: "loader-test"
                       indices: ["code"]
                     "#,
                    )
                    .query::<()>(con)
            })
            .await;

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // And also ensure that the file has been picked up...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents[0][0].0, "test.yml");
            assert_eq!(repo_contents[0][0].2, true);
            // Ensure that the "last_load" timestamp changed...
            assert_ne!(&repo_contents[0][0].3, last_load);

            // Disable namespace...
            platform
                .require::<Config>()
                .load_from_string(
                    r#"
                     server:
                         port: 1503
                     repository:
                         namespaces: [ ]
                 "#,
                    None,
                )
                .unwrap();

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Now the loader should be disabled again...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents[0][0].0, "test.yml");
            assert_eq!(repo_contents[0][0].2, false);

            // Programmatically delete the loader...
            query_redis_async(|con| {
                redis::cmd("REPO.DELETE")
                    .arg("/loaders/test.yml")
                    .query::<()>(con)
            })
            .await;

            // Ensure all internal messaging is handled...
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // ...now the loader is gone...
            let repo_contents = fetch_loaders().await;
            assert_eq!(repo_contents.len(), 0);

            platform.terminate();
        });
    }

    async fn fetch_loaders() -> Vec<Vec<(String, String, bool, String)>> {
        query_redis_async(|con| {
            redis::cmd("REPO.LOADERS")
                .arg("raw")
                .query::<Vec<Vec<(String, String, bool, String)>>>(con)
        })
        .await
        .unwrap()
    }

    async fn mini_http_server(req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let mut response = Response::new("test: true".into());
        if req.uri().path().contains("new") {
            response.headers_mut().insert(
                hyper::header::LAST_MODIFIED,
                HeaderValue::from_str(Utc::now().to_rfc2822().as_str()).unwrap(),
            );
        } else {
            response.headers_mut().insert(
                hyper::header::LAST_MODIFIED,
                HeaderValue::from_str(
                    Utc.ymd(2010, 10, 10)
                        .and_hms(10, 10, 10)
                        .to_rfc2822()
                        .as_str(),
                )
                .unwrap(),
            );
        }

        Ok(response.into())
    }

    #[test]
    fn ensure_fetch_works() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            tokio::spawn(async {
                let server_addr: SocketAddr = "127.0.0.1:7979"
                    .parse::<SocketAddr>()
                    .expect("Unable to parse socket address");
                let make_svc = make_service_fn(|_conn| async {
                    Ok::<_, Infallible>(service_fn(mini_http_server))
                });

                let server = hyper::server::Server::bind(&server_addr).serve(make_svc);
                if let Err(e) = server.await {
                    panic!("server error: {}", e);
                }
            });

            let (platform, repository) = setup_env().await;
            let mut listener = repository.listener();

            // Fetch a file from our local server...
            query_redis_async(|con| {
                redis::cmd("REPO.FETCH")
                    .arg("/test.yml")
                    .arg("http://127.0.0.1:7979/file.yml")
                    .query::<()>(con)
            })
            .await;

            // Receive an update notification from the repository...
            if let Some(FileEvent::FileChanged(file)) = await_update(&mut listener).await {
                assert_eq!(file.name, "/test.yml")
            } else {
                panic!("Failed to receive file change event");
            }

            // Fetching an unchanged file...
            query_redis_async(|con| {
                redis::cmd("REPO.FETCH")
                    .arg("/test.yml")
                    .arg("http://127.0.0.1:7979/file.yml")
                    .query::<()>(con)
            })
            .await;

            // We do not expect an update here as the file didn't change...
            if await_update(&mut listener).await.is_some() {
                panic!("Unexpected update event received...");
            }

            // Fetch the changed file will then again transfer it...
            query_redis_async(|con| {
                redis::cmd("REPO.FETCH")
                    .arg("/test.yml")
                    .arg("http://127.0.0.1:7979/new_file.yml")
                    .query::<()>(con)
            })
            .await;

            // Receive an update notification from the repository...
            if let Some(FileEvent::FileChanged(file)) = await_update(&mut listener).await {
                assert_eq!(file.name, "/test.yml")
            } else {
                panic!("Failed to receive file change event");
            }

            // Force fetching the same file will then again transfer it...
            query_redis_async(|con| {
                redis::cmd("REPO.FETCH_FORCED")
                    .arg("/test.yml")
                    .arg("http://127.0.0.1:7979/file.yml")
                    .query::<()>(con)
            })
            .await;

            // Receive an update notification from the repository...
            if let Some(FileEvent::FileChanged(file)) = await_update(&mut listener).await {
                assert_eq!(file.name, "/test.yml")
            } else {
                panic!("Failed to receive file change event");
            }

            platform.terminate();
        });
    }
}
