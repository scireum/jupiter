//! Installs a signal handler which terminates the platform in CTRL+C or SIGHUP.
//!
//! Forks an async task which waits for either **CTRL+C** or **SIGHUP** and then invokes
//! [Platform::terminate](crate::platform::Platform::terminate) on the given platform.
use std::sync::Arc;

use tokio::signal::unix::SignalKind;

use crate::platform::Platform;

/// Installs a signal handler for the given platform which awaits either a **CTRL+C** or **SIGHUP**.
///
/// Note that this is automatically called by the [Builder](crate::builder::Builder) unless it is
/// disabled.
pub fn install(platform: Arc<Platform>) {
    let _ = tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        let mut sig_hup = tokio::signal::unix::signal(SignalKind::hangup()).unwrap();

        tokio::select! {
            _ = ctrl_c => {
                log::info!("Received CTRL-C. Shutting down...");
                platform.terminate();
            },
            _ = sig_hup.recv() => {
               log::info!("Received SIGHUP. Shutting down...");
                platform.terminate();
            }
        }
    });
}
