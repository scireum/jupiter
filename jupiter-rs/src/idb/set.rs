//! Represents a Set as used by InfoGraphDB.
//!
//! Such sets can determine if they contain a given string and they also provide the insertion
//! index for each key.
//!
//! # Example
//!
//! ```
//! # use jupiter::idb::set::Set;
//! let mut set = Set::default();
//! set.add("A".to_owned());
//! set.add("B".to_owned());
//! set.add("C".to_owned());
//!
//! assert!(set.contains("A"));
//! assert!(set.contains("B"));
//! assert!(set.contains("C"));
//! assert!(!set.contains("D"));
//! assert!(!set.contains("E"));
//!
//! assert_eq!(set.index_of("A"), 1);
//! assert_eq!(set.index_of("B"), 2);
//! assert_eq!(set.index_of("C"), 3);
//! assert_eq!(set.index_of("D"), 0);
//! ```
use fnv::FnvHashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Represents an ordered set of strings.
pub struct Set {
    data: FnvHashMap<String, usize>,
    key_data_size: usize,
    queries: AtomicUsize,
}

impl Default for Set {
    /// Creates a new and empty set.
    fn default() -> Self {
        Set {
            data: FnvHashMap::default(),
            key_data_size: 0,
            queries: AtomicUsize::new(0),
        }
    }
}

impl Set {
    /// Adds the given key to the set.
    ///
    /// If the set already contains the given key, nothing will change.
    pub fn add(&mut self, key: String) {
        let next_index = self.data.len() + 1;
        self.data.entry(key).or_insert(next_index);
    }

    /// Determines the number of keys in the set.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if this set is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Estimates the memory required to represent the set.
    pub fn allocated_memory(&self) -> usize {
        // This is the semi-official way of estimating the size of the underlying hash table..
        //
        // Internally a bit more than the actual capacity is allocated to guarantee a proper load
        // factor...
        let table_size = self.data.capacity() * 11 / 10
            // Per entry, the key and an index is stored...
            * (std::mem::size_of::<String>() + std::mem::size_of::<usize>());

        // Next to the table size we also need to take the heap data which contains the actual
        // string bytes into account...
        self.key_data_size + table_size
    }

    /// Reports the number of queries which have been executed against this set.
    pub fn num_queries(&self) -> usize {
        self.queries.load(Ordering::Relaxed)
    }

    /// Determines if the given key is contained in this set.
    pub fn contains(&self, key: &str) -> bool {
        self.queries
            .store(self.queries.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

        self.data.contains_key(key)
    }

    /// Returns the insertion index of the given key.
    ///
    /// The index will be one-based (the first key will have index 1, the second 2 etc.). If a
    /// key isn't present, 0 will be returned.
    pub fn index_of(&self, key: &str) -> usize {
        self.queries
            .store(self.queries.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

        *self.data.get(key).unwrap_or(&0)
    }
}
