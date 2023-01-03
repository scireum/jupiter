//! Provides a builder which can be used to setup and initialize the framework.
//!
//! This can be used to crate and setup central parts of the framework. As Jupiter provides some
//! optional modules, a builder permits to selectively enable or disable them.
//!
//! # Example
//! Setting up the framework with all features enabled:
//! ```no_run
//! # use jupiter::builder::Builder;
//! # use jupiter::server::Server;
//! #[tokio::main]
//! async fn main() {
//!     // Enable all features and build the platform...
//!     let platform = Builder::new().enable_all().build().await;
//!
//!     // Register custom functions here...
//!
//!     // Start the main event loop of the server...
//!     platform.require::<Server>().event_loop().await;
//! }
//! ```
use crate::platform::Platform;
use crate::server::Server;
use crate::{init_logging, JUPITER_REVISION, JUPITER_VERSION};
use std::sync::Arc;

/// Initializes the framework by creating and initializing all core components.
///
/// As Jupiter provides a bunch of components of which some are optional, the actual setup
/// can be configured here.
///
/// # Example
/// Setting up the framework with all features enabled:
/// ```no_run
/// # use jupiter::builder::Builder;
/// # use jupiter::server::Server;
/// #[tokio::main]
/// async fn main() {
///     // Enable all features and build the platform...
///     let platform = Builder::new().enable_all().build().await;
///
///     // Register custom functions here...
///     
///     // Start the main event loop of the server...
///     platform.require::<Server>().event_loop().await;
/// }
/// ```
#[derive(Default)]
pub struct Builder {
    setup_logging: bool,
    enable_signals: bool,
    core_commands: bool,
    setup_config: bool,
    setup_commands: bool,
    setup_server: bool,
    version_info: String,
    revision_info: String,
}

impl Builder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Builder {
            setup_logging: false,
            enable_signals: false,
            core_commands: false,
            setup_config: false,
            setup_commands: false,
            setup_server: false,
            version_info: "-".to_string(),
            revision_info: "-".to_string(),
        }
    }

    /// Enables all features.
    ///
    /// Note that using this method (and then maybe disabling selected components) is quote
    /// convenient, but be aware that new components which might be added in a library update
    /// will then also be enabled by default. This might or might not be the expected behaviour.
    pub fn enable_all(mut self) -> Self {
        self.setup_logging = true;
        self.enable_signals = true;
        self.core_commands = true;
        self.setup_config = true;
        self.setup_commands = true;
        self.setup_server = true;

        self
    }

    /// Enables the automatic setup of the logging system.
    ///
    /// Using this, we properly initialize **simplelog** to log to stdout. As we intend Jupiter
    /// to be run in docker containers, this is all that is needed for proper logging. The date
    /// format being used is digestible by established tools like **greylog**.
    pub fn enable_logging(mut self) -> Self {
        self.setup_logging = true;
        self
    }

    /// Disables the automatic setup of the logging system after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_logging(mut self) -> Self {
        self.setup_logging = false;
        self
    }

    /// Installs a signal listener which terminates the framework once **CTRL-C** or **SIGHUP**
    /// is received.
    ///
    /// For more details see: [signals](crate::signals)
    pub fn enable_signals(mut self) -> Self {
        self.enable_signals = true;
        self
    }

    /// Disables installing the signal listener after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_signals(mut self) -> Self {
        self.enable_signals = false;
        self
    }

    /// Installs a bunch of health and maintenance commands.
    ///
    /// For more details see: [core](crate::core)
    pub fn enable_core_commands(mut self) -> Self {
        self.core_commands = true;
        self
    }

    /// Disables installing the **core** commands after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_core_commands(mut self) -> Self {
        self.core_commands = false;
        self
    }

    /// Installs [config::Config](jupiter::config::Config) and loads the **settings.yml**.
    ///
    /// For more details see: [config](crate::config)
    pub fn enable_config(mut self) -> Self {
        self.setup_config = true;
        self
    }

    /// Disables setting up a **Config** instance after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_config(mut self) -> Self {
        self.setup_config = false;
        self
    }

    /// Creates and installs a [CommandDictionary](crate::commands::CommandDictionary).
    ///
    /// For more details see: [Commands](crate::commands)
    pub fn enable_commands(mut self) -> Self {
        self.setup_commands = true;
        self
    }

    /// Disables setting up a **Commands** instance after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_commands(mut self) -> Self {
        self.setup_commands = false;
        self
    }

    /// Creates and installs a [Server](crate::server::Server) instance.
    ///
    /// For more details see: [server](crate::server::Server). Note that still, the main event loop
    /// has to be invoked manually via: `platform.require::<Server>().event_loop().await`.
    pub fn enable_server(mut self) -> Self {
        self.setup_server = true;
        self
    }

    /// Disables setting up a **Server** instance after [enable_all()](Builder::enable_all)
    /// has been used.
    pub fn disable_server(mut self) -> Self {
        self.setup_server = false;
        self
    }

    /// Specifies the version of the application to show when calling `SYS.VERSION`.
    pub fn with_version(mut self, version: impl AsRef<str>, revision: impl AsRef<str>) -> Self {
        self.version_info = version.as_ref().to_string();
        self.revision_info = revision.as_ref().to_string();

        self
    }

    /// Builds the [Platform](jupiter::platform::Platform) registry with all the enabled components
    /// being registered.
    pub async fn build(self) -> Arc<Platform> {
        let platform = Platform::new();

        if self.setup_logging {
            init_logging();
        }

        log::info!(
            "||. JUPITER (v {} - rev {}) running on {} core(s) in {} CPU(s)",
            JUPITER_VERSION,
            JUPITER_REVISION,
            num_cpus::get(),
            num_cpus::get_physical()
        );

        if self.enable_signals {
            crate::signals::install(platform.clone());
        }

        if self.setup_config {
            let _ = crate::config::install(platform.clone(), true).await;
        }

        if self.setup_server {
            let _ = Server::install(&platform);
        }

        if self.setup_commands {
            let _ = crate::commands::CommandDictionary::install(&platform);

            if self.core_commands {
                crate::core::install(platform.clone(), self.version_info, self.revision_info);
            }
        }

        platform
    }
}
