//! Provides a size constrained LRU Cache.
//!
//! An LRU cache drops the least recently used entry if it about to grow beyond the given limits.
//! In contrast to prominent caches like Redis itself or memcached, this implementation provides
//! a special command named `LRU.GET`. The idea is quite simple: Imagine the cache stores data which
//! is kind of hard to compute (takes some time), but we still want to refresh the contents from
//! time to time. Now if we detect a stale entry, we'd rather use this entry one more time (but
//! signal the client, that it should recompute and `PUT` an update value). Now as we most probably
//! server many clients at the same time, we also do not want each client to run the re-computation
//! in parallel, so we remember that we signalled a refresh and will then again suppress this flag
//! for a certain period, so that, while one client computes the new data, other clients can still
//! use the stale cache data without slowing down.
//!
//! Now of course, this approach can only be used for certain types of cached data, where using
//! stale (or potentially stale) data doesn't lead to disaster. However, if this trick can be pulled
//! off, this leads to super fast response times along with a nice way of keeping the cache up to
//! date by still using relatively short TTLs.
//!
//! Provides a generic cache which can store all kinds of values for which the [ByteSize](ByteSize)
//! trait is implemented. Each cache provides various settings to limit its size (in terms of
//! allocated memory) and also to specify the lifetime (TTL = time to live) for each entry.
//!
//! The [cache](cache) module provides an actor which contains a set of caches, as determined in
//! the system config. To enable this, [cache::install](cache::fn.install) has to be called.
pub mod cache;
mod lru_cache;

pub use lru_cache::ByteSize;
pub use lru_cache::LRUCache;
