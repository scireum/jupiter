use jupiter::builder::Builder;

use jupiter::server::Server;
use std::panic::{set_hook, take_hook};

#[tokio::main]
async fn main() {
    // Installs a panic handler which crashes the whole process instead of tying to survive with
    // a missing tokio background thread. Having a panic in a tokio thread is quite ugly, as the
    // server seems to be healthy from the outside but won't handle any incoming commands.
    //
    // Therefore we crash the whole process on purpose and hope for an external watchdog like
    // docker-compose to create a new container which is in a sane and consistent state.
    let original_panic_handler = take_hook();
    set_hook(Box::new(move |panic_info| {
        original_panic_handler(panic_info);
        eprintln!("PROGRAM ALARM: A panic occurred in a thread. Crashing the whole process to enable a clean restart...");
        std::process::exit(-1);
    }));

    // Build a platform and enable all features...
    let platform = Builder::new().enable_all().build().await;

    // Setup and enable the LRU cache...
    jupiter::lru::cache::install(platform.clone());

    // Setup and enable InfoGraphDB...
    jupiter::idb::install(platform.clone());

    // Setup and install a data repository...
    jupiter::repository::install(platform.clone(), jupiter::repository::create(&platform));

    // Setup pyrun...
    jupiter::pyrun::install(platform.clone());

    platform.require::<Server>().event_loop().await;
}
