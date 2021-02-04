#[cfg(test)]
use mock_instant::Instant;
#[cfg(not(test))]
use std::time::Instant;

use std::time::Duration;

use linked_hash_map::LinkedHashMap;

/// Returns the allocated memory in bytes.
pub trait ByteSize {
    /// Returns the amount of allocated memory in bytes.
    ///
    /// Note that most probably this is an approximation and not the exact byte value. However,
    /// it should represent the "largest" part of an instance. (E.g. for a string, this would
    /// be the bytes allocated on the heap and might discard the fields allocated on the stack
    /// used to store the length and capacity as well as the pointer itself.
    fn allocated_size(&self) -> usize;
}

impl ByteSize for String {
    fn allocated_size(&self) -> usize {
        self.capacity()
    }
}

/// Provides a size constrained LRU cache.
///
/// A cache behaves just like a **Map** as long as there is no shortage in storage. However, if
/// either the max number of entries is reached or if the allocated memory is above a certain
/// limit, old (least recently used) entries will be evicted - hence the name LRU cache.
///
/// Note, that we also permit to manage and control stale data by assigning each entry two
/// TTLs (time to live). If the soft TTL has expired, the normal **get** function will no longer
/// return the value. If the hard TTL has expired, even the **extended_get** function will ignore
/// this entry.
///
/// Note that this cache supports graceful handling of stale data to support lazy refreshing. See
/// **extended_get** for a detailed explanation.
///
/// # Examples
/// ```
/// # use jupiter::lru::LRUCache;
/// # use std::time::Duration;
///
/// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
/// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
/// // Stale entries are refreshed at most every 2s.
/// let mut lru = LRUCache::new(128,
///                             1024,
///                             Duration::from_secs(60),
///                             Duration::from_secs(60 * 60),
///                             Duration::from_secs(2),
///         );
///
/// lru.put("Foo".to_owned(), "Bar".to_owned()).unwrap();
/// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
///
/// // this will still fit..
/// lru.put("Foo1".to_owned(), "X".repeat(512)).unwrap();
/// assert_eq!(lru.get("Foo").is_some(), true);
/// assert_eq!(lru.get("Foo1").is_some(), true);
///
/// // this will hit the max memory constraint...
/// lru.put("Foo2".to_owned(), "X".repeat(512)).unwrap();
/// // ..and therefore will throw the two others out:
/// assert_eq!(lru.get("Foo").is_some(), false);
/// assert_eq!(lru.get("Foo1").is_some(), false);
/// assert_eq!(lru.get("Foo2").is_some(), true);
///```
pub struct LRUCache<V: ByteSize> {
    num_entries: usize,
    capacity: usize,
    allocated_memory: usize,
    max_memory: usize,
    soft_ttl: Duration,
    hard_ttl: Duration,
    refresh_interval: Duration,
    reads: usize,
    hits: usize,
    writes: usize,
    map: LinkedHashMap<String, Entry<V>>,
}

struct Entry<V: ByteSize> {
    mem_size: usize,
    soft_ttl: Instant,
    hard_ttl: Instant,
    next_refresh_request: Instant,
    value: V,
}

impl<V: ByteSize> LRUCache<V> {
    /// Creates a new cache which can store up to **capacity** entries or as many until
    /// they allocated **max_memory** of heap.
    ///
    /// Sett **extended_get** for an explanation of **soft_ttl**, **hard_ttl** and
    /// **refresh_interval**.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// lru.put("Foo".to_owned(), "Bar".to_owned()).unwrap();
    /// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
    ///```
    pub fn new(
        capacity: usize,
        max_memory: usize,
        soft_ttl: Duration,
        hard_ttl: Duration,
        refresh_interval: Duration,
    ) -> Self {
        LRUCache {
            num_entries: 0,
            capacity,
            allocated_memory: 0,
            max_memory,
            soft_ttl,
            hard_ttl,
            refresh_interval,
            reads: 0,
            hits: 0,
            writes: 0,
            map: LinkedHashMap::with_capacity(capacity),
        }
    }

    /// Stores the given value for the given key.
    ///
    /// # Errors
    /// Fails if the given entry is larger than **max_memory** (the max total size of the cache).
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
    ///```    
    pub fn put(&mut self, key: String, value: V) -> anyhow::Result<()> {
        let entry = Entry {
            mem_size: key.len() + value.allocated_size(),
            soft_ttl: Instant::now() + self.soft_ttl,
            hard_ttl: Instant::now() + self.hard_ttl,
            next_refresh_request: Instant::now(),
            value,
        };

        if entry.mem_size > self.max_memory {
            return Err(anyhow::anyhow!(
                "The entry to be cached is larger than the whole cache size!"
            ));
        }

        let mut delta_mem: isize = entry.mem_size as isize;
        let mut delta_count = 1;

        if let Some(stale_entry) = self.map.insert(key, entry) {
            delta_mem -= stale_entry.mem_size as isize;
            delta_count = 0
        }

        self.writes += 1;
        self.num_entries += delta_count;
        self.allocated_memory = (self.allocated_memory as isize + delta_mem) as usize;

        self.enforce_constraints();

        Ok(())
    }

    fn enforce_constraints(&mut self) {
        while self.num_entries > self.capacity || self.allocated_memory > self.max_memory {
            match self.map.pop_front() {
                Some(lru_entry) => {
                    self.num_entries -= 1;
                    self.allocated_memory -= lru_entry.1.mem_size;
                }
                None => unreachable!("Failed to enforce constraints of a LRU cache!"),
            }
        }
    }

    /// Returns the value which has previously been stored for the given key or **None** if
    /// no value is present.
    ///
    /// Note that get will no longer return a value once its **soft_ttl** has expired.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // After inserting a value...
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// // ..it can be retrieved.
    /// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
    ///```   
    pub fn get(&mut self, key: &str) -> Option<&V> {
        self.reads += 1;

        let now = Instant::now();

        match self.map.get_refresh(key) {
            Some(entry) if entry.soft_ttl > now => {
                self.hits += 1;
                Some(&entry.value)
            }
            _ => None,
        }
    }

    /// Returns the value which has previously been stored for the given key just like **get**.
    ///
    /// However, this will also return values which **soft_ttl* has already expired. Therefore
    /// this function doesn't only return the value itself, but two additional flags (in front
    /// of it): ACTIVE and REFRESH.
    ///
    /// ACTIVE, the first value of the returned triple indicates whether the value is a normal
    /// and still valid value (**true**). If this is **false**, the entry is stale (its soft_ttl
    /// has expired, but its hard_ttl hasn't).
    ///
    /// The second value in the triple, REFRESH, indicates if the cache encourages the caller to
    /// update the stale value. Therefore, this is only ever true, if ACTIVE is false, as otherwise
    /// there is no need to refresh anything. For the first time, a stale entry (its value) is
    /// returned by this function, the REFRESH flag is returned as **true**. Any subsequent call
    /// for this entry will then return **false** until either the value is refreshed (via **put**)
    /// or after **refresh_interval** has expired. During this period, the entry will be reported
    /// as ACTIVE again so that it can be distinguished from a fully stale entry.
    ///
    /// This can be used to lazy update stale content.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be immediately considered stale and be completely evicted
    /// // after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(0),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // Insert a value...
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    ///
    /// // Due to the exotic settings of the cache, the entry is immediately stale and a refresh
    /// // is requested...
    /// assert_eq!(lru.extended_get("Foo").unwrap(), (false, true, &"Bar".to_owned()));
    /// // ..but not for again for the next 2s (in the meantime it is reported as non-stale).
    /// assert_eq!(lru.extended_get("Foo").unwrap(), (true, false, &"Bar".to_owned()));
    ///
    /// // If we add a value again, it is also again immediately stale and a refresh would be
    /// // requested...
    /// lru.put("Foo".to_owned(), "Bar1".to_owned());
    /// assert_eq!(lru.extended_get("Foo").unwrap(), (false, true, &"Bar1".to_owned()));
    ///```
    pub fn extended_get(&mut self, key: &str) -> Option<(bool, bool, &V)> {
        self.reads += 1;

        let now = Instant::now();

        match self.map.get_refresh(key) {
            Some(entry) if entry.hard_ttl > now => {
                self.hits += 1;

                let mut alive = entry.soft_ttl > now;
                let mut refresh = false;

                if !alive {
                    // If the current entry is stale, we determine if we should ask for a refresh..
                    if entry.next_refresh_request <= now {
                        // Yes, we did not instruct another caller recently to perform a refresh...
                        entry.next_refresh_request = now + self.refresh_interval;
                        refresh = true;
                    } else {
                        // We already asked another caller to refresh. In the meantime we pretend
                        // that the entry is still valid so that it will be used (otherwise it
                        // looks like a stale result...)
                        alive = true;
                    }
                }

                Some((alive, refresh, &entry.value))
            }
            _ => None,
        }
    }

    /// Removes the entry for the given key if present.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // After inserting a value...
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// // ..it can be retrieved.
    /// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
    ///
    /// // However, once it is removed...
    /// lru.remove("Foo");
    /// // ..it's no longer accessible.
    /// assert_eq!(lru.get("Foo"), None);
    /// ```
    pub fn remove(&mut self, key: &str) {
        self.writes += 1;

        if let Some(entry) = self.map.remove(key) {
            self.num_entries -= 1;
            self.allocated_memory -= entry.mem_size;
        }
    }

    /// Removes all entries in this cache.
    ///
    /// Note that this will also zero all metrics (reads, writes, cache hits).
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // After inserting a value...
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// // ..it can be retrieved.
    /// assert_eq!(lru.get("Foo").unwrap(), &"Bar".to_owned());
    ///
    /// // However, once the cache is flushed...
    /// lru.flush();
    ///
    /// // ..it's no longer accessible.
    /// assert_eq!(lru.get("Foo"), None);
    /// ```
    pub fn flush(&mut self) {
        self.map.clear();
        self.allocated_memory = 0;
        self.num_entries = 0;
        self.reads = 0;
        self.writes = 0;
        self.hits = 0;
    }

    /// Returns the number of elements in the cache.
    ///
    /// Note that this might include entries which have reached their **hard_ttl** and can therefore
    /// not be used via **get** or **extended_get**. However these will of course be evicted once
    /// the cache makes room for new entries.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// assert_eq!(lru.len(), 0);
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// assert_eq!(lru.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.num_entries
    }

    /// Determines if the cache is completely empty.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// assert_eq!(lru.is_empty(), true);
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// assert_eq!(lru.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.num_entries == 0
    }

    /// Returns to overall capacity (max number of entries) of this cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Changes the maximal number of entries permitted in this cache.
    ///
    /// Note that most probably it is smarter to limit the **max_memory** than the number of
    /// entries.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 10 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(10,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // Add some entries...
    /// lru.put("Foo".to_owned(), "Bar".to_owned());
    /// lru.put("Foo1".to_owned(), "Bar".to_owned());
    /// lru.put("Foo2".to_owned(), "Bar".to_owned());
    /// lru.put("Foo3".to_owned(), "Bar".to_owned());
    /// lru.put("Foo4".to_owned(), "Bar".to_owned());
    /// lru.put("Foo5".to_owned(), "Bar".to_owned());
    /// assert_eq!(lru.len(), 6);
    ///
    /// // Now request that the cache is reduced to only 3 entries...
    /// lru.set_capacity(3);
    /// assert_eq!(lru.capacity(), 3);
    ///
    /// // ensure that all other entries are gone...
    /// assert_eq!(lru.len(), 3);
    /// ```
    pub fn set_capacity(&mut self, capacity: usize) {
        let previous_capacity = self.capacity;
        self.capacity = capacity;
        if previous_capacity > self.capacity {
            self.enforce_constraints();
        }
    }

    /// Returns the maximal amount of memory to be (roughly) occupied by this cache.
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    /// Specifies the maximal amount of memory to be (roughly) occupied by this cache.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::lru::LRUCache;
    /// # use std::time::Duration;
    ///
    /// // Specifies a cache which can store up to 128 entries which can allocated up to 1024 bytes of
    /// // memory. Each entry will be considered stale after 1m and be completely evicted after 1h.
    /// // Stale entries are refreshed at most every 2s.
    /// let mut lru = LRUCache::new(128,
    ///                             1024,
    ///                             Duration::from_secs(60),
    ///                             Duration::from_secs(60 * 60),
    ///                             Duration::from_secs(2),
    ///         );
    ///
    /// // Add some entries...
    /// lru.put("Foo0".to_owned(), "Bar".to_owned());
    /// lru.put("Foo1".to_owned(), "Bar".to_owned());
    /// lru.put("Foo2".to_owned(), "Bar".to_owned());
    /// lru.put("Foo3".to_owned(), "Bar".to_owned());
    /// lru.put("Foo4".to_owned(), "Bar".to_owned());
    /// lru.put("Foo5".to_owned(), "Bar".to_owned());
    /// assert_eq!(lru.len(), 6);
    ///
    /// // Now request that the cache is reduced to only 14 bytes...
    /// lru.set_max_memory(14);
    /// assert_eq!(lru.max_memory(), 14);
    ///
    /// // .. this will kick each but the last two entries out of the cache..
    /// assert_eq!(lru.len(), 2);
    /// ```
    pub fn set_max_memory(&mut self, max_memory: usize) {
        let previous_max_memory = self.max_memory;
        self.max_memory = max_memory;
        if previous_max_memory > self.max_memory {
            self.enforce_constraints();
        }
    }

    /// Returns the current soft time to live to apply on entries.
    ///
    /// See **extended_get** for a detailed description on TTLs.
    pub fn soft_ttl(&self) -> Duration {
        self.soft_ttl
    }

    /// Specifies the current soft time to live to apply on entries.
    ///
    /// Note that this will not affect existing entries, but only ones placed in the cache
    /// after this call.
    ///
    /// See **extended_get** for a detailed description on TTLs.
    pub fn set_soft_ttl(&mut self, soft_ttl: Duration) {
        self.soft_ttl = soft_ttl;
    }

    /// Returns the current hard time to live to apply on entries.
    ///
    /// See **extended_get** for a detailed description on TTLs.
    pub fn hard_ttl(&self) -> Duration {
        self.hard_ttl
    }

    /// Specifies the current hard time to live to apply on entries.
    ///
    /// Note that this will not affect existing entries, but only ones placed in the cache
    /// after this call.
    ///
    /// See **extended_get** for a detailed description on TTLs.
    pub fn set_hard_ttl(&mut self, hard_ttl: Duration) {
        self.hard_ttl = hard_ttl;
    }

    /// Returns the refresh suppression interval.
    ///
    /// See **extended_get** for a detailed description on this topic.
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    /// Specifies the refresh suppression interval to use.
    ///
    /// Note that this is not
    ///
    /// See **extended_get** for a detailed description on this topic.
    pub fn set_refresh_interval(&mut self, refresh_interval: Duration) {
        self.refresh_interval = refresh_interval;
    }

    /// Returns the amount of memory allocated to store the data of the keys and values of this
    /// cache.
    ///
    /// The returned value is in bytes. Note that this most probably a rough estimate but should
    /// account for the largest part of allocated memory.
    pub fn allocated_memory(&self) -> usize {
        self.allocated_memory
    }

    /// Returns the total amount of memory allocated by this cache.
    ///
    /// In contrast to **allocated_memory()** this method also tries to account for the internal
    /// hash map and other metadata. Note that these are most probably only estimates as not all
    /// involved structures are well known.
    ///
    /// The returned value is in bytes.
    pub fn total_allocated_memory(&self) -> usize {
        self.allocated_memory
            + self.map.capacity()
                * (std::mem::size_of::<String>() + std::mem::size_of::<Entry<V>>())
    }

    /// Returns the cache utilization in percent.
    pub fn utilization(&self) -> f32 {
        self.num_entries as f32 / self.capacity as f32 * 100.
    }

    /// Returns the memory utilization in percent.
    pub fn memory_utilization(&self) -> f32 {
        self.allocated_memory as f32 / self.max_memory as f32 * 100.
    }

    /// Returns the cache hit rate in percent.
    ///
    /// Note that all metrics are reset when **flush()** is called.
    pub fn hit_rate(&self) -> f32 {
        match self.reads {
            0 => 0.,
            n => self.hits as f32 / n as f32 * 100.,
        }
    }

    /// Returns the write read ration in percent.
    ///
    /// This simply computes how many of the operations were writes. A healthy cache has way more
    /// reads than writes, therefore this might be a helpful metric.
    ///
    /// Note that all metrics are reset when **flush()** is called.
    pub fn write_read_ratio(&self) -> f32 {
        match self.reads {
            0 => 100.,
            n => self.writes as f32 / (self.writes + n) as f32 * 100.,
        }
    }

    /// Returns the total number of reads performed on this cache since the last flush.
    pub fn reads(&self) -> usize {
        self.reads
    }

    /// Returns the total number of writes performed on this cache since the last flush.
    pub fn writes(&self) -> usize {
        self.writes
    }
}

#[cfg(test)]
mod tests {
    use crate::lru::LRUCache;
    use mock_instant::MockClock;
    use tokio::time::Duration;

    #[test]
    fn capacity_is_enforced() {
        // Creates a cache, which hase quite some room memory wise but only permits
        // four entries at the same time...
        let mut lru = LRUCache::new(
            4,
            8192,
            Duration::from_secs(60 * 60),
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
        );

        // We expect 4 entries to fully fit in the cache....
        lru.put("Hello".to_owned(), "World".to_owned()).unwrap();
        lru.put("Hello1".to_owned(), "World1".to_owned()).unwrap();
        lru.put("Hello2".to_owned(), "World2".to_owned()).unwrap();
        lru.put("Hello3".to_owned(), "World3".to_owned()).unwrap();
        assert_eq!(lru.len(), 4);
        assert_eq!(lru.get("Hello").unwrap(), &"World".to_owned());
        assert_eq!(lru.get("Hello1").unwrap(), &"World1".to_owned());
        assert_eq!(lru.get("Hello2").unwrap(), &"World2".to_owned());
        assert_eq!(lru.get("Hello3").unwrap(), &"World3".to_owned());

        // Now if another entry is added, the LRU (least recently used/inserted)
        // will be dropped...
        lru.put("Hello4".to_owned(), "World4".to_owned()).unwrap();
        assert_eq!(lru.get("Hello"), None);
        assert_eq!(lru.get("Hello1").unwrap(), &"World1".to_owned());
        assert_eq!(lru.get("Hello2").unwrap(), &"World2".to_owned());
        assert_eq!(lru.get("Hello3").unwrap(), &"World3".to_owned());
        assert_eq!(lru.get("Hello4").unwrap(), &"World4".to_owned());

        // Now if we "use" another entry, it gets "saved" and another one will
        // be evicted upon an insertion...
        let _ = lru.get("Hello1");
        lru.put("Hello5".to_owned(), "World5".to_owned()).unwrap();
        assert_eq!(lru.get("Hello1").unwrap(), &"World1".to_owned());
        assert_eq!(lru.get("Hello2"), None);
        assert_eq!(lru.get("Hello3").unwrap(), &"World3".to_owned());
        assert_eq!(lru.get("Hello4").unwrap(), &"World4".to_owned());
        assert_eq!(lru.get("Hello5").unwrap(), &"World5".to_owned());

        // Removing an entry will make room to permit inserting yet another
        // entry without removing any entries...
        assert_eq!(lru.len(), 4);
        lru.remove("Hello5");
        assert_eq!(lru.len(), 3);
        lru.put("Hello6".to_owned(), "World6".to_owned()).unwrap();
        assert_eq!(lru.get("Hello1").unwrap(), &"World1".to_owned());
        assert_eq!(lru.get("Hello3").unwrap(), &"World3".to_owned());
        assert_eq!(lru.get("Hello4").unwrap(), &"World4".to_owned());
        assert_eq!(lru.get("Hello6").unwrap(), &"World6".to_owned());
        assert_eq!(lru.len(), 4);
    }

    #[test]
    fn max_memory_is_enforced() {
        let mut lru = LRUCache::new(
            128,
            12 * 4,
            Duration::from_secs(60 * 60),
            Duration::from_secs(60 * 60),
            Duration::from_secs(60),
        );

        // We expect 4 entries with a size of 12 bytes each to fully fit in the cache....
        // (Note that the allocation tracking only takes the raw string sizes into account
        // and ignores additional fields like length and the size of the underlying table /
        // vectors itself.)
        lru.put("Hello0".to_owned(), "World0".to_owned()).unwrap();
        lru.put("Hello1".to_owned(), "World1".to_owned()).unwrap();
        lru.put("Hello2".to_owned(), "World2".to_owned()).unwrap();
        lru.put("Hello3".to_owned(), "World3".to_owned()).unwrap();
        assert_eq!(lru.len(), 4);
        assert_eq!(lru.allocated_memory(), 12 * 4);
        assert_eq!(lru.get("Hello0").unwrap(), &"World0".to_owned());
        assert_eq!(lru.get("Hello1").unwrap(), &"World1".to_owned());
        assert_eq!(lru.get("Hello2").unwrap(), &"World2".to_owned());
        assert_eq!(lru.get("Hello3").unwrap(), &"World3".to_owned());

        // If we remove an entry, the used memory is corrected...
        lru.remove("Hello0");
        assert_eq!(lru.len(), 3);
        assert_eq!(lru.allocated_memory(), 12 * 3);

        // If we replace an entry, the used memory is corrected...
        lru.put("Hello1".to_owned(), "".to_owned()).unwrap();
        assert_eq!(lru.allocated_memory(), 12 * 3 - 6);

        // Restore the original entry, so that the table as three entries with 12 bytes each...
        lru.put("Hello1".to_owned(), "World1".to_owned()).unwrap();
        assert_eq!(lru.allocated_memory(), 12 * 3);

        // If we now add an entry with is 13 bytes long, one entry has to be dropped and therefore
        // we end up with three remaining entries....
        lru.put("Hello0".to_owned(), "World01".to_owned()).unwrap();
        assert_eq!(lru.allocated_memory(), 12 * 3 + 1);
        assert_eq!(lru.len(), 3);
        // "Hello2" was the least recently touched/modified, so it will have been evicted to make
        // room for our new entry...
        assert_eq!(lru.get("Hello2"), None);
    }

    #[test]
    fn ttls_are_properly_enforced() {
        let mut lru = LRUCache::new(
            1024,
            1024,
            Duration::from_secs(15 * 60),
            Duration::from_secs(30 * 60),
            Duration::from_secs(60),
        );

        lru.put("Foo".to_owned(), "Bar".to_owned()).unwrap();
        assert_eq!(lru.get("Foo").unwrap(), "Bar");

        MockClock::advance(Duration::from_secs(16 * 60));
        assert_eq!(lru.get("Foo"), None);

        assert_eq!(
            lru.extended_get("Foo").unwrap(),
            (false, true, &"Bar".to_owned())
        );

        assert_eq!(
            lru.extended_get("Foo").unwrap(),
            (true, false, &"Bar".to_owned())
        );

        MockClock::advance(Duration::from_secs(2 * 60));
        assert_eq!(
            lru.extended_get("Foo").unwrap(),
            (false, true, &"Bar".to_owned())
        );

        MockClock::advance(Duration::from_secs(16 * 60));
        assert_eq!(lru.extended_get("Foo"), None);
    }

    #[test]
    fn ttls_are_discarded_on_put() {
        let mut lru = LRUCache::new(
            1024,
            1024,
            Duration::from_secs(15 * 60),
            Duration::from_secs(30 * 60),
            Duration::from_secs(60),
        );

        lru.put("Foo".to_owned(), "Bar".to_owned()).unwrap();
        assert_eq!(lru.get("Foo").unwrap(), "Bar");
        MockClock::advance(Duration::from_secs(16 * 60));
        assert_eq!(lru.get("Foo"), None);
        assert_eq!(
            lru.extended_get("Foo").unwrap(),
            (false, true, &"Bar".to_owned())
        );
        lru.put("Foo".to_owned(), "Bar1".to_owned()).unwrap();
        assert_eq!(lru.get("Foo").unwrap(), "Bar1");
        assert_eq!(
            lru.extended_get("Foo").unwrap(),
            (true, false, &"Bar1".to_owned())
        );
    }

    #[test]
    fn metrics_are_computed_correctly() {
        let mut lru = LRUCache::new(
            4,
            10,
            Duration::from_secs(15 * 60),
            Duration::from_secs(30 * 60),
            Duration::from_secs(60),
        );

        // Write 3 values into the cache...
        lru.put("A".to_owned(), "A".to_owned()).unwrap();
        lru.put("B".to_owned(), "B".to_owned()).unwrap();
        lru.put("C".to_owned(), "C".to_owned()).unwrap();

        // Perform 4 reads, of which 3 hit a cache entry...
        assert_eq!(lru.get("A").is_some(), true);
        assert_eq!(lru.get("B").is_some(), true);
        assert_eq!(lru.get("C").is_some(), true);
        assert_eq!(lru.get("D").is_none(), true);

        // ... therefore we hat 3 writes, 4 reads of which 3 hit a value which
        // yields a hit rate of 75%
        assert_eq!(lru.writes(), 3);
        assert_eq!(lru.reads(), 4);
        assert_eq!(lru.hit_rate().round() as i32, 75);

        // Perform 3 more reads..
        assert_eq!(lru.get("A").is_some(), true);
        assert_eq!(lru.get("B").is_some(), true);
        assert_eq!(lru.get("C").is_some(), true);

        // we've now had 7 reads and 3 writes. This yields a write/read ratio of
        // 30% (3 of 10 total operation were writes...)
        assert_eq!(lru.reads(), 7);
        assert_eq!(lru.write_read_ratio().round() as i32, 30);

        // We know our keys and values consume 6 bytes...
        assert_eq!(lru.allocated_memory(), 6);

        // We cannot compute the total allocated memory correctly, but we can at least perform
        // a sanity check here..
        assert_eq!(lru.total_allocated_memory() > lru.allocated_memory(), true);

        // The cache contains 3 entries and has a capacity of 4 -> 75% utilization...
        assert_eq!(lru.utilization().round() as i32, 75);

        // The cache contains 6 bytes of data and has a max memory of 10 -> 60% memory utilization..
        assert_eq!(lru.memory_utilization().round() as i32, 60);
    }
}
