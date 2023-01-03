//! Provides a tiny DI like container to expose all components of the system.
//!
//! The platform is more or less a simple map which keeps all central components as
//! **Arc<T>** around. Also this keeps the central **is_running** flag which is toggled to
//! *false* once [Platform::terminate](Platform::terminate) is invoked.
//!
//! Not that in common cases [Platform::require](Platform::require) is a good way of fetching a
//! service which is known to be there. However, be aware, that once the system shutdown is
//! initiated, the internal map is cleared and empty (so that all Dropped handlers run). Therefore
//! if the code might be executed after [Platform::terminate](Platform::terminate) was called, you
//! should use [Platform::find](Platform::find) and gracefully handle the **None** case. However,
//! in most cases the lookup of services is performed during startup and therefore **require** can
//! be used.
//!
//! # Examples
//!
//! ```
//! # use std::sync::Arc;
//! # use jupiter::platform::Platform;
//! struct Service {
//!     value : i32
//! }
//!
//! struct UnknownService;
//!
//! let platform = Platform::new();
//!     
//! // Registers a new service...
//! platform.register::<Service>(Arc::new(Service { value: 42 }));
//!     
//! // Obtains a reference to a previously registered service...
//! let service = platform.require::<Service>();
//! assert_eq!(service.value, 42);
//!
//! // Trying to obtain a service which hasn't been registered yet, returns an empty
//! // optional...
//! assert_eq!(platform.find::<UnknownService>().is_none(), true);
//!
//! // By default the platform is running...
//! assert_eq!(platform.is_running(), true);
//!     
//! // Once terminated...
//! platform.terminate();
//! // All services are immediately released so that their "Dropped" handlers run...
//! assert_eq!(platform.find::<Service>().is_none(), true);
//!
//! // and the platform is no longer considered active...
//! assert_eq!(platform.is_running(), false);
//! ```
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::sync::atomic::{AtomicBool, Ordering};

/// Provides a container to keep all central services in a single place.
///
/// # Examples
///
/// Building and accessing components:
/// ```
/// # use jupiter::platform::Platform;
/// # use std::sync::Arc;
///
/// struct Service {}
///
/// #[tokio::main]
/// async fn main() {
///     let platform = Platform::new();
///     platform.register(Arc::new(Service {}));
///     assert_eq!(platform.find::<Service>().is_some(), true);
/// }
/// ```
///
/// Checking the central "is running" active..
/// ```
/// # use jupiter::platform::Platform;
/// # use std::sync::Arc;
///
/// struct Service {}
///
/// #[tokio::main]
/// async fn main() {
///     let platform = Platform::new();
///     platform.register(Arc::new(Service {}));
///
///     // By default the platform is running...
///     assert_eq!(platform.is_running(), true);
///
///     // once terminated...
///     platform.terminate();
///
///     // all services are evicted so that their Dropped handlers are executed
///     assert_eq!(platform.find::<Service>().is_some(), false);
///
///     // the platform is considered halted...
///     assert_eq!(platform.is_running(), false);
/// }
/// ```
pub struct Platform {
    services: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    is_running: AtomicBool,
}

impl Platform {
    /// Creates a new platform instance..
    pub fn new() -> Arc<Self> {
        Arc::new(Platform {
            services: Mutex::new(HashMap::new()),
            is_running: AtomicBool::new(true),
        })
    }

    /// Registers a new component.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::platform::Platform;
    /// # use std::sync::Arc;
    ///
    /// struct Service {
    ///     value: i32
    /// }
    ///
    /// let platform = Platform::new();
    /// platform.register::<Service>(Arc::new(Service { value: 42 }));
    /// ```
    pub fn register<T>(&self, service: Arc<T>)
    where
        T: Any + Send + Sync,
    {
        let _ = self
            .services
            .lock()
            .unwrap()
            .insert(TypeId::of::<T>(), service);
    }

    /// Tries to resolve a previously registered service.
    ///
    /// Note, if one knows for certain, that a service will be present,
    /// [Platform::require](Platform::require) can be used.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::platform::Platform;
    /// # use std::sync::Arc;
    ///
    /// struct Service {
    ///     value: i32
    /// }
    ///
    /// struct UnknownService;
    ///
    /// let platform = Platform::new();
    /// platform.register::<Service>(Arc::new(Service { value: 42 }));
    ///
    /// // A lookup for a known service yields a result..
    /// assert_eq!(platform.find::<Service>().unwrap().value, 42);
    ///
    /// // A lookup for an unknown service returns None...
    /// assert_eq!(platform.find::<UnknownService>().is_none(), true);
    /// ```
    pub fn find<T>(&self) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        let services = self.services.lock().unwrap();
        services
            .get(&TypeId::of::<T>())
            .and_then(|entry| entry.clone().downcast::<T>().ok())
    }

    /// Resolve a previously registered service.
    ///
    /// Note, if the framework is already shutting down, all services are evicted. Therefor this
    /// might panic even if it worked before [Platform::terminate](Platform::terminate) was invoked.
    ///
    /// # Panics
    /// Panics if the requested service isn't available.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::platform::Platform;
    /// # use std::sync::Arc;
    ///
    /// struct Service {
    ///     value: i32
    /// }
    ///
    /// let platform = Platform::new();
    /// platform.register::<Service>(Arc::new(Service { value: 42 }));
    ///
    /// // A lookup for a known service yields a result..
    /// assert_eq!(platform.require::<Service>().value, 42);
    /// ```
    ///
    /// Requiring a service which is unknown will panic:
    /// ```should_panic
    /// # use jupiter::platform::Platform;
    /// # use std::sync::Arc;
    ///
    /// struct UnknownService;
    ///
    /// let platform = Platform::new();
    ///
    /// // This will panic...
    /// platform.require::<UnknownService>();
    /// ```
    pub fn require<T>(&self) -> Arc<T>
    where
        T: Any + Send + Sync,
    {
        if self.is_running() {
            match self.find::<T>() {
                Some(service) => service,
                None => panic!(
                    "A required component ({}) was not available in the platform registry!",
                    std::any::type_name::<T>()
                ),
            }
        } else {
            panic!(
                "A required component ({}) has been requested but the system is already shutting down!",
                std::any::type_name::<T>()
            )
        }
    }

    /// Determines if the platform is still running or if [Platform::terminate](Platform::terminate)
    /// has already been called.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    /// Terminates the platform.
    ///
    /// This will immediately release all services (so that the Dropped handlers run eventually).
    /// It will also toggle the [is_running()](Platform::is_running) flag to **false**.
    pub fn terminate(&self) {
        // Drop all services so that the Dropped handlers run (sooner or later)...
        self.services.lock().unwrap().clear();

        // Mark platform as halted...
        self.is_running.store(false, Ordering::Release);
    }
}
