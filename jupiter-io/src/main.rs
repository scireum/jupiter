use jupiter::builder::Builder;
use jupiter::server::Server;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    // Build a platform and enable all features...
    let platform = Builder::new().enable_all().build().await;

    // Setup and enable the LRU cache...
    jupiter::lru::cache::install(platform.clone());

    // Setup and enable InfoGraphDB...
    jupiter::idb::install(platform.clone());

    // Setup and install a data repository...
    jupiter::repository::install(platform.clone(), jupiter::repository::create(&platform));

    platform.require::<Server>().event_loop().await;
}
