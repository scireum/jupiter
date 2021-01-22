//! Provides a size constrained LRU Cache.
//!
//! An LRU cache drops the least recently used entry if it about to grow beyond the given limits.
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
