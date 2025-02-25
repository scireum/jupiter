//! Contains the system configuration.
//!
//! Provides access to the system configuration which is loaded from the **config/settings.yml** file.
//! Note that we observe this file for changes and reload it once a change is detected. Therefore
//! each user of the config should attach itself to the [Config::notifier](Config::notifier) and
//! re-process the config once a change message is received.
//!
//! Being an in-memory database framework we want to prevent restarts / downtimes as much as
//! possible.
//!
//! Note that the **Config** struct is kind of constant and can be obtained from the **Platform**
//! once and then kept around. However, when using **Config::current()** to obtain the current
//! config handle, this should not be stored, as it will not be updated once a new config has
//! been loaded.
//!
//! # Examples
//!
//! Obtaining and reading the config:
//! ```
//! # use jupiter::config;
//! # use jupiter::config::Config;
//! # use jupiter::platform::Platform;
//! # #[tokio::main]
//! # async fn main() {
//!
//! // Create a platform and install the config...
//! let platform = Platform::new();
//! let config = config::install(platform.clone(), false).await;
//!
//! // Fetch the current config document (might be reloaded from time to time) and read the
//! // selected server port..
//! let port = config.current().config()["server"]["port"].as_i64().unwrap_or(1234);
//! # }
//! ```
//!
//! Attaching a change listener:
//! ```no_run
//! # use jupiter::config;
//! # use jupiter::config::Config;
//! # use jupiter::platform::Platform;
//! # #[tokio::main]
//! # async fn main() {
//!
//! // Create a platform and install the config...
//! let platform = Platform::new();
//! let config = config::install(platform.clone(), false);
//!
//! // Obtain the config...
//! let config = platform.require::<Config>();
//! tokio::spawn(async move {
//!     loop {
//!         // Wait for a config change. This will most probably be combined with a read from
//!         // a command queue or another feature using tokio::select!...
//!         match config.notifier().recv().await {
//!             Ok(_) => log::info!("Config update received..."),
//!             _ => return,
//!         }       
//!     }
//! });
//! # }
//! ```
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use yaml_rust::{Yaml, YamlLoader};

use crate::platform::Platform;
use crate::spawn;
use anyhow::Context;
use std::path::Path;

/// Provides access to the system configuration.
///
/// Most probably a config instance is installed by the [Builder](crate::builder::Builder) and
/// can be obtained via `platform.require::<Config>()`. Note that it is highly recommended to
/// register a change listener by calling `Config::notifier()` as we expect all components to pick
/// up config changes without restarting the application.
pub struct Config {
    filename: String,
    tx: tokio::sync::broadcast::Sender<()>,
    config: ArcSwap<(Yaml, Option<SystemTime>)>,
}

/// Represents the change listener.
///
/// Internally this is simply the receiver of a broadcast. The actual message being broadcast
/// can an should be ignored. All that matters is, once a message has been received, the config
/// was changed and needs to be re-processed.
pub type ChangeNotifier = tokio::sync::broadcast::Receiver<()>;

/// Represents a handle to the currently loaded configuration.
///
/// Note that this handle should not be stored or kept around for long, as it will not be updated
/// if the underlying config changed.
pub struct Handle {
    config: Arc<(Yaml, Option<SystemTime>)>,
}

impl Config {
    /// Creates a new config reading the given file.
    ///
    /// Note that this will not install a change listener. This is only done by the
    /// [install](install) function.
    pub fn new(file: &str) -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(1);
        Config {
            filename: file.to_owned(),
            config: ArcSwap::new(Arc::new((Yaml::Null, None))),
            tx,
        }
    }

    /// Obtains a change notifier which receives a message once the config changed.
    pub fn notifier(&self) -> ChangeNotifier {
        self.tx.subscribe()
    }

    /// Obtains a handle to the currently loaded configuration.
    ///
    /// Note that this is a fairly efficient operation but still provides some overhead. Therefore
    /// this shouldn't be placed in an inner loop.
    pub fn current(&self) -> Handle {
        Handle {
            config: self.config.load_full(),
        }
    }

    /// Determines the last modified date of the config file on disk.
    ///
    /// As within docker, the file is presented as volume, we check that it is a file, as an
    /// unmounted docker volume is always presented as directory.
    async fn last_modified(&self) -> Option<SystemTime> {
        tokio::fs::metadata(&self.filename)
            .await
            .ok()
            .filter(|meta| meta.is_file())
            .and_then(|meta| meta.modified().ok())
    }

    /// Forces the config to read the underlying file.
    ///
    /// Note that this is normally called by the framework and should not be invoked manually.
    pub async fn load(&self) -> anyhow::Result<()> {
        log::info!("Loading config file {}...", &self.filename);

        if let Ok(metadata) = tokio::fs::metadata(&self.filename).await {
            if !metadata.is_file() {
                log::info!("Config file doesn't exist or is an unmounted docker volume - skipping config load.");
                return Ok(());
            }
        }

        let config_data = match tokio::fs::read_to_string(&self.filename).await {
            Ok(data) => data,
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "Cannot load config file {}: {}",
                    &self.filename,
                    error
                ));
            }
        };

        let last_modified = tokio::fs::metadata(&self.filename)
            .await
            .ok()
            .and_then(|metadata| metadata.modified().ok());

        self.load_from_string(config_data.as_str(), last_modified)
    }

    /// Loads a configuration from the given string instead of a file.
    ///
    /// This is intended to be used in test environments where we cannot / do not want to load
    /// a config file from disk.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::config::Config;
    /// # use std::time::Instant;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// let config = Config::new("test_config.yml");
    ///
    /// // Remove any left over file...
    /// std::fs::remove_file("test_config.yml");
    ///
    /// // Write a config file...
    /// assert_eq!(config.store("
    /// server:
    ///     port: 12345
    /// ").await.is_ok(), true);
    ///
    /// // Load it back and verify its contents (in a fully running Apollo, this would
    /// // happen automatically via the config watcher...)
    /// assert_eq!(config.load().await.is_ok(), true);
    /// assert_eq!(config.current().config()["server"]["port"].as_i64().unwrap(), 12345);
    ///
    /// // Writing an invalid config file is prevented...
    /// assert_eq!(config.store("server: \"test").await.is_err(), true);
    ///
    /// // Therefore the original config is still present...
    /// assert_eq!(config.load().await.is_ok(), true);
    /// assert_eq!(config.current().config()["server"]["port"].as_i64().unwrap(), 12345);
    /// # }
    /// ```
    pub async fn store(&self, config: &str) -> anyhow::Result<()> {
        log::info!(
            "Programmatically updating the config file {}...",
            &self.filename
        );

        if let Err(error) = YamlLoader::load_from_str(config) {
            Err(anyhow::anyhow!("Cannot parse config data: {}", error))
        } else {
            tokio::fs::write(&self.filename, config)
                .await
                .context("Failed to write to config file!")?;
            log::info!("Config has been updated successfully!");

            // We have nothing let to do, as we will automatically detect the change and reload the
            // config...
            Ok(())
        }
    }

    /// Loads a configuration from the given string instead of a file.
    ///
    /// This is intended to be used in test environments where we cannot / do not want to load
    /// a config file from disk.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::config::Config;
    /// use std::time::Instant;
    /// let config = Config::new("somefile.yml");
    /// config.load_from_string("
    /// server:
    ///     port: 12345
    /// ", None);
    ///
    /// assert_eq!(config.current().config()["server"]["port"].as_i64().unwrap(), 12345);
    /// ```
    pub fn load_from_string(
        &self,
        data: &str,
        last_modified: Option<SystemTime>,
    ) -> anyhow::Result<()> {
        let docs = match YamlLoader::load_from_str(data) {
            Ok(docs) => docs,
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "Cannot parse config file {}: {}",
                    &self.filename,
                    error
                ));
            }
        };

        // Store update config...
        self.config.store(Arc::new((
            docs.first().unwrap_or(&Yaml::Null).clone(),
            last_modified,
        )));

        // Notify all listeners - we ignore if there are none...
        let _ = self.tx.clone().send(());

        Ok(())
    }
}

impl Handle {
    /// Provides access to the currently loaded configuration.
    pub fn config(&self) -> &Yaml {
        &self.config.0
    }
}

/// Creates an installs a **Config** for the given platform.
///
/// This will read its contents from **config/settings.yml** and also install a change listener for
/// this file. Note that this listener will only watch the "last modified" date of the file and will
/// not perform a structural comparison. Therefore it is the duty of each config user to gracefully
/// handle partial config changes.
///
/// Note that this method is also called by the [Builder](crate::builder::Builder) unless the
/// **Config** part is disabled.
pub async fn install(platform: Arc<Platform>, auto_reload: bool) -> Arc<Config> {
    // Create the "config" directory in case it doesn't exist...
    let path = Path::new("config").to_path_buf();
    if let Err(error) = tokio::fs::create_dir_all(path.clone()).await {
        log::warn!(
            "Failed to create config base directory {}: {}",
            path.to_string_lossy(),
            error
        )
    }
    // Install a config instance and point it to "settings.yml"..
    let config = Arc::new(Config::new("config/settings.yml"));
    platform.register::<Config>(config.clone());

    // Actually try to read the file...
    if let Err(error) = config.load().await {
        log::error!("{}", error);
    }

    // Install a change listener which runs every 2s...
    if auto_reload {
        run_config_change_monitor(platform, config.clone());
    }

    config
}

fn run_config_change_monitor(platform: Arc<Platform>, config: Arc<Config>) {
    spawn!(async move {
        while platform.is_running() {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            // This will contain the last modified date of the file on disk or be None if the
            // file is absent...
            let last_modified = config.last_modified().await;

            // Contains the timestamp when the file was loaded the last time or be None if no
            // data has been loaded yet...
            let last_loaded = config.config.load().1;

            // If a file is present and newer that the one previously loaded (or if none has been
            // loaded so far) -> perform a reload and broadcast an update if the file has been
            // successfully loaded...
            if last_modified.is_some() && (last_loaded.is_none() || last_modified > last_loaded) {
                match config.load().await {
                    Ok(_) => {
                        log::info!("System configuration was re-loaded.");
                    }
                    Err(error) => log::error!("Failed to re-load system config: {}", error),
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use crate::platform::Platform;
    use std::time::SystemTime;

    #[test]
    fn ensure_config_update_works() {
        crate::testing::test_async(async {
            let platform = Platform::new();
            let config = crate::config::install(platform.clone(), false).await;

            // Load an initial config...
            config
                .load_from_string("test: 42", Some(SystemTime::now()))
                .unwrap();

            // Setup a task which notifies our oneshot channel once the config changes...
            let mut change_notifier = config.notifier();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = tokio::spawn(async move {
                match change_notifier.recv().await {
                    Ok(_) => tx.send(()).unwrap(),
                    _ => (),
                };
            });

            // Ensure that initial config is still present...
            assert_eq!(config.current().config()["test"].as_i64().unwrap(), 42);

            // Ensure that a malformed config is simply ignored...
            assert_eq!(
                config
                    .load_from_string("test: 'invalid", Some(SystemTime::now()))
                    .is_err(),
                true
            );

            // Ensure that initial config is still present...
            assert_eq!(config.current().config()["test"].as_i64().unwrap(), 42);

            // Change the config...
            config
                .load_from_string("test: 4242", Some(SystemTime::now()))
                .unwrap();

            // Await oneshot message
            match rx.await {
                Ok(()) => (),
                _ => panic!("Received invalid value..."),
            };

            // Ensure that new config is now present...
            assert_eq!(config.current().config()["test"].as_i64().unwrap(), 4242);
        });
    }
}
