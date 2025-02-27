//! Jupiter is a library for providing high throughput ultra low latency services via the RESP
//! protocol as defined by Redis.
//!
//! # Introduction
//! **Jupiter** is a framework for wrapping **compute** or **memory intense** components to
//! provide them as **high throughput** and **ultra low latency** services to
//! applications built on managed runtimes like **node.js**, **Java**, **Ruby**.
//!
//! These managed runtimes are great for building sophisticated web applications but have limited
//! capabilities when raw compute power or optimized memory utilization is required. This
//! on the other hand is an area where **Rust** shines, as it permits to write low-level
//! and highly optimized code which is still safe to run.
//!
//! Therefore all we need is a simple an efficient way of combining the best of both worlds.
//! To minimize the overhead of communication, we use the [RESP Protocol](https://redis.io/topics/protocol)
//! as defined by **Redis**. In contrast to HTTP this is way simpler to parse and handle while
//! also supporting zero-copy operations. Another benefit is, that for nearly every platform
//! there is already a Redis/RESP client available.
//!
//! # SIRIUS / Java
//! We at [scireum](https://www.scireum.de) use **Jupiter** in conjunction with our open source
//! Java framework [SIRIUS](https://github.com/scireum/sirius-kernel) to build web based
//! applications.
//!
//! We use **Jupiter** as an LRU cache for intermediate search results and search metadata. We also
//! store large parts of semi-constant masterdata (think of "all ZIP codes and street names in
//! germany) there. Both of these frameworks are contributed to the open source community and can
//! be either used in own applications or directly by running a
//! [Jupiter IO](https://hub.docker.com/repository/docker/scireum/jupiter-io) instance.
//!
//! Our most central use of this framework lies in **Quasar** which sadly has to remain closed
//! source. This application contains our complete text processing framework which runs everything
//! from syntactical pre-processing to full morphological analysis steps in order to maintain our
//! excellent search experience in our products.
//!
//! # Features
//! * **Ultra fast non allocating parser for RESP queries** (as sent by redis-cli and redis clients).
//!   The built-in server will use a single buffer per connection to read, parse and process queries.
//!   To deliver a response, a single buffer is allocated to buffer the response to minimize the
//!   number of required sys-calls to write a response on the wire.
//! * **100% Async/Await** - the whole server builds upon [tokio](https://tokio.rs/) and async/await
//!   primitives as provided by Rust. Also, all commands handlers are build as actors to simplify
//!   concurrency correctness and to also minimize any synchronization overheads.
//! * **Reload-aware config facility** which permits to update the configuration during operation.
//!   Therefore, no restart is ever required, even when changing the IP binding or port. This is
//!   kind of important for an in-memory application which might have an expensive startup time.
//! * **Build in management commands**. The *core* module provides a set of management commands to
//!   monitor and inspect the state of the system.
//! * **Simple and well documented code base**. After all, Jupiter isn't a large framework at all.
//!   This permits every user to browse and understand its source code and when to expect from the
//!   system. Also, this is due to the fact that Jupiter stands on the shoulders of giants
//!   (especially [tokio](https://tokio.rs/)).
//!
//! # Modules
//! * **LRU-Cache**: An size constraint cache with an intelligent refresh strategy which can be used
//!   to maintain low latency response times by employing a coordinated asynchronous cache update
//!   pattern (see `LRU.XGET` or the module documentation of [crate::lru::cache]).
//! * **InfoGraphDB**: Provides a fast and flexible static database for master data. Using the
//!   **Repository** this can be used to load master data from e.g. an S3 Bucket or a git repository
//!   into fast lookup tables or code sets. These permit to perform all kinds of lookups,
//!   reverse-lookups, "search as you type" searches and automatic translation management (even for
//!   tables with thousands of rows / structured documents). More infos: [crate::idb]
//! * **Repository**: The repository is used to fetch files from various sources and invoking
//!   appropriate loaders so that the data can be used (e.g. as IDB table). See [crate::repository]
//!
//! # Examples
//! A complete example of using Jupiter can be found here:
//! [Jupiter IO](https://github.com/scireum/jupiter/tree/master/jupiter-io).
//!
//! Still a short example on how to initialize the library can be found here [Builder](builder::Builder).
//! Some example commands can be found in the implementation of the [core](core) commands.
//!
//! # Using Jupiter
//! **Jupiter** is intended to be used as a framework for your custom application. However the
//! example instance [Jupiter IO](https://github.com/scireum/jupiter/tree/master/jupiter-io) which
//! features an **LRU cache** and an **InfoGraphDB** instance can also be directly used via docker:
//! [Docker image](https://hub.docker.com/repository/docker/scireum/jupiter-io).
//!
//! Further info can be found on [crates.io/crates/jupiter](https://crates.io/crates/jupiter).
//! As well as on GitHub: [github.com/scireum/jupiter](https://github.com/scireum/jupiter)
#![deny(
    warnings,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_results
)]
use simplelog::{format_description, ConfigBuilder, LevelFilter, SimpleLogger};
use std::sync::Once;

pub mod average;
pub mod builder;
pub mod commands;
pub mod config;
pub mod core;
pub mod fmt;
pub mod idb;
pub mod ig;
pub mod lru;
pub mod platform;
pub mod repository;
pub mod request;
pub mod response;
pub mod server;
pub mod signals;

/// Contains the version of the Jupiter library.
pub const JUPITER_VERSION: &str = "DEVELOPMENT-SNAPSHOT";

/// Contains the git commit hash of the Jupiter build being used.
pub const JUPITER_REVISION: &str = "NO-REVISION";

/// Initializes the logging system.
///
/// Note that most probably the simplest way is to use a [Builder](builder::Builder) to set up the
/// framework, which will also set up logging if enabled.
pub fn init_logging() {
    static INIT_LOGGING: Once = Once::new();

    // We need to do this as otherwise the integration tests might crash as the logging system
    // is initialized several times...
    INIT_LOGGING.call_once(|| {
        if let Err(error) = SimpleLogger::init(
            LevelFilter::Debug,
            ConfigBuilder::new()
                .set_time_format_custom(format_description!(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]"
                ))
                .set_thread_level(LevelFilter::Trace)
                .set_target_level(LevelFilter::Error)
                .set_location_level(LevelFilter::Trace)
                .build(),
        ) {
            panic!("Failed to initialize logging system: {}", error);
        }
    });
}

/// Provides a simple macro to execute an async lambda within `tokio::spawn`.
///
/// Note that this also applies std::mem::drop on the returned closure to make
/// clippy happy.
///
/// # Example
/// ```rust
/// # #[macro_use] extern crate jupiter;
/// # #[tokio::main]
/// # async fn main() {
/// spawn!(async move {
///     // perform some async stuff here...
/// });
/// # }
#[macro_export]
macro_rules! spawn {
    ($e:expr) => {{
        std::mem::drop(tokio::spawn($e));
    }};
}

#[cfg(test)]
mod testing {
    use redis::{Connection, RedisError};
    use std::sync::Mutex;
    use tokio::time::Duration;

    lazy_static::lazy_static! {
        /// Provides a global lock which has to be acquired if a test operates on shared
        /// resources. This would either be our test port (1503) on which we start our
        /// local server for integrations tests or the repository which operates on the
        /// file system. Using this lock, we can still execute all other tests in parallel
        /// and only block if required.
        pub static ref SHARED_TEST_RESOURCES: Mutex<()> = Mutex::new(());
    }

    /// Executes async code within a single threaded tokio runtime.
    pub fn test_async<F: std::future::Future>(future: F) {
        use tokio::runtime;

        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let _ = rt.block_on(future);
    }

    /// Executes a blocking Redis query in an async fashion.
    ///
    /// This is required as we must not block tokio in any way. Note that the redis create itself
    /// would permit async queries, however, this seems to rely on a previous version of tokio
    /// which crashes with our version (tokio 1.0.0).
    pub async fn query_redis_async<T, Q>(query: Q) -> Option<T>
    where
        Q: FnOnce(&mut Connection) -> Result<T, RedisError> + Send + Sync + 'static,
        T: Send + 'static,
    {
        let result = tokio::task::spawn_blocking(|| {
            let client = redis::Client::open("redis://127.0.0.1:1503").unwrap();
            let mut con = client
                .get_connection_with_timeout(Duration::from_secs(5))
                .unwrap();
            query(&mut con)
        })
        .await;

        match result {
            Ok(Ok(result)) => Some(result),
            _ => None,
        }
    }
}
