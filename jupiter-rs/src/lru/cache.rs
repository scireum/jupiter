//! Provides an actor which manages a set of LRU caches for string keys and values.
//!
//! To use this facility, [install](fn.install) has to be invoked. The configuration is fetched from
//! the system config and will be automatically re-loaded once the file changes.
//!
//! # Configuration
//! In the system config, an object name **caches** has to be present which specifies the settings
//! for each cache:
//!
//! ```yaml
//! caches:
//!     my_cache:
//!         # Specifies the maximal number of entries to store
//!         size: 1024
//!         # Specifies the maximal amount of memory to use (in bytes).
//!         # Supports common suffixes like: k, m, g, t
//!         max_memory: 1g
//!         # Specifies the soft time to live. After this period, an entry is considered stale
//!         # and will not be delivered by LRU.GET. However, LRU.XGET will deliver this entry
//!         # but mark it as stale. Supports common suffixes like: s, m, h, d
//!         soft_ttl: 15m
//!         # Specifies the hard time to live. After this period, neither LRU.GET nor LRU.XGET
//!         # will deliver this entry.
//!         hard_ttl: 1d
//!         # Specifies the refresh interval for LRU.XGET. If this command delivers a stale entry
//!         # (as defined by soft_ttl), it indicates that the entry is stale an should be
//!         # refreshed. However, once this has to be signalled to a client, it will no longer
//!         # request a refresh from other clients until either the entry has been refreshed or
//!         # this refresh interval has elapsed.
//!         refresh_interval: 30s
//! ```
//!
//! # Commands
//!
//! The actor defines the following commands:
//! * **LRU.PUT**: `LRU.PUT cache key value` will store the given value for the given key in the
//!   given cache.
//! * **LRU.GET**: `LRU.GET cache key` will perform a lookup for the given key in the given cache
//!   and return the value being stored or an empty string if no value is present.
//! * **LRU.XGET**: `LRU.XGET cache key` will behave just like **LRU.GET**. However, its output is
//!   a bit more elaborate. It will always respond with three values: ACTIVE, REFRESH, VALUE. If
//!   no value was found for the given key, ACTIVE and REFRESH will be 0 and VALUE will be an empty
//!   string. If a non-stale entry way found, ACTIVE is 1, REFRESH is 0 an VALUE will be the value
//!   associated with the key. Now the interesting part: If a stale entry (older than *soft_ttl* but
//!   younger than *hard_ttl*) was found, ACTIVE will be 0. For the first client to request this
//!   entry, REFRESH will be 1 and the VALUE will be the stale value associated with the key. For
//!   all subsequent invocations of this command, REFRESH will be 0 until either the entry was
//!   updated (by calling **LRU.PUT**) or if the *refresh_interval* has elapsed since the first
//!   invocation. Using this approach one can build "lazy" caches, which refresh on demand, without
//!   slowing the requesting client down (stale content can be delivered quickly, if the application
//!   accepts doing so) and also without overloading the system, as only one client will typically
//!   try to obtain a fresh value instead of all clients at once.
//! * **LRU.REMOVE**: `LRU.REMOVE cache key` will remove the value associated with the given key.
//!   Note that the value will be immediately gone without respecting any TTL.
//! * **LRU.FLUSH**: `LRU.FLUSH cache` will wipe all contents of the given cache.
//! * **LRU.STATE**: `LRU.STATE` will provide an overview of all active cache. `LRU.STATE cache`
//!   will provide detailed metrics about the given cache.
//!
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::commands::{queue, Call, CommandResult};
use crate::commands::{CommandDictionary, ResultExt};
use crate::config::Config;
use crate::fmt::{format_duration, format_size, parse_duration, parse_size};
use crate::platform::Platform;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::ig::docs::Element;
use crate::lru::LRUCache;

/// Enumerates the commands supported by this actor.
#[derive(FromPrimitive)]
enum Commands {
    Put,
    Get,
    ExtendedGet,
    Remove,
    Flush,
    Stats,
}

/// We operate on caches which store plain Strings.
type StringCache = LRUCache<String>;

/// Installs the cache actor into the given platform.
///
/// This will automatically load the config from the **Config** in this platform (and also
/// update the caches on change). Also this will register the commands defined above in the
/// **CommandDictionary** of this platform.
pub fn install(platform: Arc<Platform>) {
    let queue = actor(platform.clone());

    let commands = platform.require::<CommandDictionary>();
    commands.register_command("LRU.PUT", queue.clone(), Commands::Put as usize);
    commands.register_command("LRU.GET", queue.clone(), Commands::Get as usize);
    commands.register_command("LRU.REMOVE", queue.clone(), Commands::Remove as usize);
    commands.register_command("LRU.XGET", queue.clone(), Commands::ExtendedGet as usize);
    commands.register_command("LRU.FLUSH", queue.clone(), Commands::Flush as usize);
    commands.register_command("LRU.STATS", queue, Commands::Stats as usize);
}

/// Spawns the actual actor which handles all commands or processes config changes.
fn actor(platform: Arc<Platform>) -> crate::commands::Queue {
    let (queue, mut endpoint) = queue();

    tokio::spawn(async move {
        let config = platform.require::<Config>();
        let mut config_changed = config.notifier();

        let mut caches: HashMap<String, StringCache> = HashMap::new();
        caches = update_config(caches, &config);

        while platform.is_running() {
            tokio::select! {
                _ = config_changed.recv() => { caches = update_config(caches, &config) }
                msg = endpoint.recv() => {
                    if let Some(mut call) = msg {
                         match Commands::from_usize(call.token) {
                            Some(Commands::Put) => put_command(&mut call, &mut caches).complete(call),
                            Some(Commands::Get) => get_command(&mut call, &mut caches).complete(call),
                            Some(Commands::ExtendedGet) => extended_get_command(&mut call, &mut caches).complete(call),
                            Some(Commands::Remove) => remove_command(&mut call, &mut caches).complete(call),
                            Some(Commands::Flush) => flush_command(&mut call, &mut caches).complete(call),
                            Some(Commands::Stats) => stats_command(&mut call, &mut caches).complete(call),
                            _ => ()
                        }
                    }
                }
            }
        }
    });

    queue
}

/// Updates the currently active caches based on the settings in the given config.
///
/// Note that this provides a safety mechanism. If no config object at all is present,
/// we leave the current caches untouched. This prevents the system from wiping all caches
/// in the case of an accidental change or an invalid config.
fn update_config(
    caches: HashMap<String, StringCache>,
    config: &Arc<Config>,
) -> HashMap<String, StringCache> {
    let handle = config.current();
    let map = handle.config().root().query("caches");
    if map.is_object() {
        parse_config(caches, map)
    } else {
        log::info!("Config does not contain a 'caches' object. Skipping config update.");
        caches
    }
}

/// Actually loads the configuration for the caches now that we've verified that a config is
/// present.
fn parse_config(
    mut caches: HashMap<String, StringCache>,
    map: Element,
) -> HashMap<String, StringCache> {
    let mut result = HashMap::new();
    for (name, config) in map.entries() {
        let current_cache = caches.remove(name);
        if let Some(cache) = create_or_update(name, current_cache, config) {
            result.insert(name.to_owned(), cache);
        }
    }

    for name in caches.keys() {
        log::info!("Dropping stale cache {}...", name);
    }

    result
}

/// Creates or updates the cache with the given name based on the given config element.
///
/// In case of an invalid config, it leaves the current cache untouched. Therefore this will not
/// create a cache with an invalid or partial config. But it will also not damage or wipe an
/// active cache due to an accident or config problem.
fn create_or_update(
    name: &str,
    current_cache: Option<StringCache>,
    config: Element,
) -> Option<StringCache> {
    let size = match config.query("size").as_int().filter(|value| *value > 0) {
        None => {
            log::error!(
                "Not going to create or update {} as no cache size was given.",
                name
            );
            return current_cache;
        }
        Some(n) => n,
    } as usize;

    let max_memory = match parse_size(config.query("max_memory").to_str()) {
        Err(error) => {
            log::error!(
                "Not going to create or update {}. Failed to parse 'max_memory': {}",
                name,
                error
            );
            return current_cache;
        }
        Ok(n) => n,
    };

    let soft_ttl = match parse_duration(config.query("soft_ttl").to_str()) {
        Ok(duration) => duration,
        Err(error) => {
            log::error!(
                "Not going to create or update {}. Failed to parse 'soft_ttl': {}",
                name,
                error
            );
            return current_cache;
        }
    };

    let hard_ttl = match parse_duration(config.query("hard_ttl").to_str()) {
        Ok(duration) => duration,
        Err(error) => {
            log::error!(
                "Not going to create or update {}. Failed to parse 'hard_ttl': {}",
                name,
                error
            );
            return current_cache;
        }
    };

    let refresh_interval = match parse_duration(config.query("refresh_interval").to_str()) {
        Ok(duration) => duration,
        Err(error) => {
            log::error!(
                "Not going to create or update {}. Failed to parse 'refresh_interval': {}",
                name,
                error
            );
            return current_cache;
        }
    };

    match current_cache {
        Some(mut cache) => {
            update_cache(
                name,
                &mut cache,
                size,
                max_memory,
                soft_ttl,
                hard_ttl,
                refresh_interval,
            );

            Some(cache)
        }
        None => {
            log::info!("Creating new cache {}...", name);
            Some(LRUCache::new(
                size,
                max_memory,
                soft_ttl,
                hard_ttl,
                refresh_interval,
            ))
        }
    }
}

/// Applies the new config values on an existing cache.
fn update_cache(
    name: &str,
    cache: &mut StringCache,
    capacity: usize,
    max_memory: usize,
    soft_ttl: Duration,
    hard_ttl: Duration,
    refresh_interval: Duration,
) {
    if cache.capacity() != capacity {
        log::info!(
            "Updating the size of {} from {} to {}.",
            name,
            cache.capacity(),
            capacity
        );
        cache.set_capacity(capacity);
    }

    if cache.max_memory() != max_memory {
        log::info!(
            "Updating max_memory of {} from {} to {}.",
            name,
            format_size(cache.max_memory()),
            format_size(max_memory)
        );
        cache.set_max_memory(max_memory);
    }

    if cache.soft_ttl() != soft_ttl {
        log::info!(
            "Updating soft_ttl of {} from {} to {}.",
            name,
            format_duration(cache.soft_ttl()),
            format_duration(soft_ttl)
        );
        cache.set_soft_ttl(soft_ttl);

        log::info!("Flushing {} due to changed TTL settings...", name);
        cache.flush();
    }

    if cache.hard_ttl() != hard_ttl {
        log::info!(
            "Updating hard_ttl of {} from {} to {}.",
            name,
            format_duration(cache.hard_ttl()),
            format_duration(hard_ttl)
        );
        cache.set_hard_ttl(hard_ttl);

        log::info!("Flushing {} due to changed TTL settings...", name);
        cache.flush();
    }

    if cache.refresh_interval() != refresh_interval {
        log::info!(
            "Updating refresh_interval of {} from {} to {}.",
            name,
            format_duration(cache.refresh_interval()),
            format_duration(refresh_interval)
        );
        cache.set_refresh_interval(refresh_interval);
    }
}

/// Obtains the cache with the given name or yields an appropriate error message.
fn get_cache<'a>(
    name: &str,
    caches: &'a mut HashMap<String, StringCache>,
) -> anyhow::Result<&'a mut StringCache> {
    match caches.get_mut(name) {
        Some(cache) => Ok(cache),
        None => Err(anyhow::anyhow!("Unknown cache: {}", name)),
    }
}

/// Implements the LRU.PUT command.
fn put_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;

    cache.put(
        call.request.str_parameter(1)?.to_owned(),
        call.request.str_parameter(2)?.to_owned(),
    )?;

    call.response.ok()?;
    Ok(())
}

/// Implements the LRU.GET command.
fn get_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;

    if let Some(value) = cache.get(call.request.str_parameter(1)?) {
        call.response.bulk(value)?;
    } else {
        call.response.empty_string()?;
    }

    Ok(())
}

/// Implements the LRU.XGET command.
fn extended_get_command(
    call: &mut Call,
    caches: &mut HashMap<String, StringCache>,
) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;

    if let Some((alive, refresh, value)) = cache.extended_get(call.request.str_parameter(1)?) {
        call.response.array(3)?;
        call.response.boolean(alive)?;
        call.response.boolean(refresh)?;
        call.response.bulk(value)?;
    } else {
        call.response.array(3)?;
        call.response.boolean(false)?;
        call.response.boolean(false)?;
        call.response.empty_string()?;
    }

    Ok(())
}

/// Implements the LRU.REMOVE command.
fn remove_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;
    cache.remove(call.request.str_parameter(1)?);
    call.response.ok()?;

    Ok(())
}

/// Implements the LRU.FLUSH command.
fn flush_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;
    cache.flush();
    call.response.ok()?;

    Ok(())
}

/// Delegates the LRU.STATS command to the proper implementation based on its arguments.
fn stats_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    if call.request.parameter_count() == 0 {
        all_stats_command(call, caches)
    } else {
        cache_stats_command(call, caches)
    }
}

/// Implements `LRU.STATS` command.
fn all_stats_command(call: &mut Call, caches: &mut HashMap<String, StringCache>) -> CommandResult {
    let mut result = String::new();

    result += "Use 'LRU.STATS <cache>' for detailed metrics.\n\n";

    result += format!(
        "{:<30} {:>12} {:>20}\n",
        "Name", "Num Entries", "Allocated Memory"
    )
    .as_str();
    result += crate::response::SEPARATOR;

    for (name, cache) in caches {
        result += format!(
            "{:<30} {:>12} {:>20}\n",
            name,
            cache.len(),
            format_size(cache.allocated_memory())
        )
        .as_str();
    }
    result += crate::response::SEPARATOR;

    call.response.bulk(result)?;

    Ok(())
}

/// Implements the `LRU.STATS cache` command.
fn cache_stats_command(
    call: &mut Call,
    caches: &mut HashMap<String, StringCache>,
) -> CommandResult {
    let cache = get_cache(call.request.str_parameter(0)?, caches)?;

    let mut result = String::new();

    result += format!("{:<30} {:>20}\n", "Num Entries", cache.len()).as_str();
    result += format!("{:<30} {:>20}\n", "Max Entries", cache.capacity()).as_str();
    result += format!("{:<30} {:>18.2} %\n", "Utilization", cache.utilization()).as_str();
    result += format!(
        "{:<30} {:>20}\n",
        "Allocated Memory",
        format_size(cache.allocated_memory())
    )
    .as_str();
    result += format!(
        "{:<30} {:>20}\n",
        "Max Memory",
        format_size(cache.max_memory())
    )
    .as_str();
    result += format!(
        "{:<30} {:>18.2} %\n",
        "Memory Utilization",
        cache.memory_utilization()
    )
    .as_str();
    result += format!(
        "{:<30} {:>20}\n",
        "Total Memory",
        format_size(cache.total_allocated_memory())
    )
    .as_str();
    result += format!("{:<30} {:>20}\n", "Reads", cache.reads()).as_str();
    result += format!("{:<30} {:>20}\n", "Writes", cache.writes()).as_str();
    result += format!("{:<30} {:>18.2} %\n", "Hit Rate", cache.hit_rate()).as_str();
    result += format!(
        "{:<30} {:>18.2} %\n",
        "Write/Read Ratio",
        cache.write_read_ratio()
    )
    .as_str();

    call.response.bulk(result)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::builder::Builder;
    use crate::commands::CommandDictionary;
    use crate::config::Config;
    use crate::request::Request;
    use mock_instant::MockClock;
    use std::time::Duration;

    /// Tests if commands yield the expected responses.
    ///
    /// Especially this ensures, that XGET behaves as expected.
    #[test]
    fn test_commands() {
        crate::testing::test_async(async {
            let platform = Builder::new()
                .enable_config()
                .enable_commands()
                .build()
                .await;

            // Define a test cache with known TTLs
            platform
                .require::<Config>()
                .load_from_string(
                    "caches:
                              test:
                                 size: 10000
                                 max_memory: 16m
                                 soft_ttl: 15m
                                 hard_ttl: 30m
                                 refresh_interval: 10s
                          ",
                    None,
                )
                .unwrap();

            // Install a cache actor...
            crate::lru::cache::install(platform.clone());

            // PUT an value into the cache...
            let mut dispatcher = platform.require::<CommandDictionary>().dispatcher();
            let result = dispatcher
                .invoke(
                    Request::example(vec!["LRU.PUT", "test", "foo", "bar"]),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");

            // ...and ensure we can read it back
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.GET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "$3\r\nbar\r\n");

            // REMOVE the value...
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.REMOVE", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");

            // ...and ensure it's gone.
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.GET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+\r\n");

            // PUT a value into the cache again...
            let result = dispatcher
                .invoke(
                    Request::example(vec!["LRU.PUT", "test", "foo", "bar"]),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");

            // Await longer than soft_ttl...
            MockClock::advance(Duration::from_secs(16 * 60));

            // ...therefore ensure that GET will no longer return the value.
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.GET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+\r\n");

            // but XGET will and also ask for a refresh...
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.XGET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(
                std::str::from_utf8(&result[..]).unwrap(),
                "*3\r\n:0\r\n:1\r\n$3\r\nbar\r\n"
            );

            // after that, XGET will still return the value but no longer ask for a refresh...
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.XGET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(
                std::str::from_utf8(&result[..]).unwrap(),
                "*3\r\n:1\r\n:0\r\n$3\r\nbar\r\n"
            );

            // one the refresh period has passed, XGET will once again ask us to refresh the stale
            // value...
            MockClock::advance(Duration::from_secs(12));
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.XGET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(
                std::str::from_utf8(&result[..]).unwrap(),
                "*3\r\n:0\r\n:1\r\n$3\r\nbar\r\n"
            );

            // After waiting for hard_ttl to be elapsed, even XGET will no longer return the value..
            MockClock::advance(Duration::from_secs(16 * 60));
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.XGET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(
                std::str::from_utf8(&result[..]).unwrap(),
                "*3\r\n:0\r\n:0\r\n+\r\n"
            );

            // PUT an value into the cache again...
            platform.require::<CommandDictionary>().dispatcher();
            dispatcher
                .invoke(
                    Request::example(vec!["LRU.PUT", "test", "foo", "bar"]),
                    None,
                )
                .await
                .unwrap();

            // FLUSH it...
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.FLUSH", "test"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");

            // ...and ensure it's gone.
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.GET", "test", "foo"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+\r\n");

            // Now let's invoke LRU.STATS - being a diagnostic command, we do not test
            // the actual result, but at least ensure a positive response...
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.STATS"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[0..1]).unwrap(), "$");
            let result = dispatcher
                .invoke(Request::example(vec!["LRU.STATS", "test"]), None)
                .await
                .unwrap();
            assert_eq!(std::str::from_utf8(&result[0..1]).unwrap(), "$");
        });
    }
}
