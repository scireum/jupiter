//! Provides a `SymbolTable` and a `SymbolMap` for an efficient and compact representation
//! of objects / inner maps.
//!
//! As most datasets consist of many objects which all share the same keys, we keep the actual
//! strings in a symbol table and only store the symbols in the actual objects.
//!
//! This heavily reduces the memory being consumed and also permits a very cache friendly and there-
//! fore super fast lookup for values in inner objects.
use std::collections::HashMap;
use std::slice::Iter;

/// Defines the representation of a symbol.
///
/// Note that a single `Doc` (and its `SymbolTable') may only store up to 2^31 - 1 distinct symbols.
pub type Symbol = i32;

/// Used to resolve and lookup symbols.
#[derive(Clone, Default)]
pub struct SymbolTable {
    table: HashMap<String, Symbol>,
    symbols: Vec<String>,
}

impl SymbolTable {
    /// Creates a new and empty symbol table.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let table = SymbolTable::new();
    ///
    /// assert_eq!(table.len(), 0);
    /// ```
    pub fn new() -> Self {
        SymbolTable {
            table: HashMap::new(),
            symbols: Vec::new(),
        }
    }

    /// Tries to resolve the given `string` into an existing `Symbol`.
    ///
    /// If no symbol with the given name is known, `None` is returned. If a new
    /// symbol should be created instead, use `find_or_create`.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// let symbol = table.find_or_create("Test").unwrap();
    ///
    /// assert_eq!(table.resolve("Test").unwrap(), symbol);
    /// assert_eq!(table.resolve("Unknown").is_none(), true);
    /// ```
    pub fn resolve(&self, string: impl AsRef<str>) -> Option<Symbol> {
        self.table.get(string.as_ref()).copied()
    }

    /// Resolve the given `string` into a new or an existing `Symbol`.
    ///
    /// If no symbol should be created if the given name is unknown, use `resolve`.
    ///
    /// # Errors
    ///
    /// This will return an error if the internal symbol table overflows (if there are more than
    /// std::i32::MAX - 2 symbols).
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// let symbol = table.find_or_create("Test").unwrap();
    /// assert_eq!(table.resolve("Test").unwrap(), symbol);
    /// ```
    pub fn find_or_create(&mut self, string: impl AsRef<str>) -> anyhow::Result<Symbol> {
        let value = string.as_ref();
        if let Some(symbol) = self.table.get(value) {
            Ok(*symbol)
        } else if self.symbols.len() >= i32::MAX as usize {
            Err(anyhow::anyhow!("Symbol table overflow!"))
        } else {
            let new_symbol = (self.symbols.len() + 1) as i32;

            let _ = self.table.insert(value.to_owned(), new_symbol);
            self.symbols.push(value.to_owned());

            Ok(new_symbol)
        }
    }

    /// Retrieves the name of the given `Symbol`.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// let symbol = table.find_or_create("Test").unwrap();
    ///
    /// // A known symbol can be looked up...
    /// assert_eq!(table.lookup(symbol), "Test");
    ///
    /// // An unknown symbol is simply translated to ""
    /// assert_eq!(table.lookup(1024), "");
    /// ```
    pub fn lookup(&self, symbol: Symbol) -> &str {
        self.symbols
            .get((symbol - 1) as usize)
            .map(|string| string.as_str())
            .unwrap_or("")
    }

    /// Determines the number of known symbols in the table.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// // The same symbol is only added once to a table...
    /// let symbol = table.find_or_create("Test").unwrap();
    /// let symbol1 = table.find_or_create("Test").unwrap();
    /// assert_eq!(symbol, symbol1);
    ///
    /// // ..therefore the table size is 1.
    /// assert_eq!(table.len(), 1);
    ///
    /// // If we add another symbol...
    /// table.find_or_create("Test 2").unwrap();
    ///
    /// // ...the size grows to 2.
    /// assert_eq!(table.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.symbols.len()
    }

    /// Determines if the symbol table is empty.
    ///
    /// # Examples
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// assert_eq!(table.is_empty(), true);
    /// let _ = table.find_or_create("Test").unwrap();
    /// assert_eq!(table.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.symbols.is_empty()
    }

    /// Estimates the allocated memory required to represent the symbol table.
    ///
    /// Note that this is only an approximation as some inner types to not reveal their
    /// size.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolTable;
    /// let mut table = SymbolTable::new();
    ///
    /// table.find_or_create("Hello").unwrap();
    /// table.find_or_create("World").unwrap();
    ///
    /// println!("{}", table.allocated_size());
    /// ```
    pub fn allocated_size(&self) -> usize {
        // This is the semi-official way of estimating the size of the underlying hash table..
        //
        // Internally a bit more than the actual capacity is allocated to guarantee a proper load
        // factor...
        let table_size = self.table.capacity() * 11 / 10
            // Per entry, the key, its value and a hash is stored...
            * (std::mem::size_of::<usize>() + std::mem::size_of::<String>() + std::mem::size_of::<Symbol>());

        // The lookup-table is known to be simply a Vec of strings...
        let lookup_size = self.symbols.capacity() * std::mem::size_of::<String>();

        // And of course we have to add up the bytes in each string...
        let content_size: usize = self.symbols.iter().map(|string| string.len()).sum();

        table_size + lookup_size + content_size
    }
}

/// Provides a efficient lookup map using `Symbol` as key.
///
/// This map is optimized  as we know for sure, that the key is an `i32`. Also, we will use this
/// map for objects within `Node` and therefore know that most commonly there will be only a few
/// keys (most probably < 8 and almost always < 32).
///
/// Therefore we simply use a `Vec` of tuples to store the keys and their value. We also keep
/// this vector sorted by the key so that a lookup can be done via `binary_search`.
///
/// Using this approach easily beats `HashMap` in both, performance and memory consumption.
#[derive(Debug, Eq, PartialEq, Default)]
pub struct SymbolMap<V> {
    entries: Vec<(Symbol, V)>,
}

impl<V: Default> SymbolMap<V> {
    /// Creates a new and empty map.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<i32>::new();
    /// assert_eq!(map.len(), 0);
    /// ```
    pub fn new() -> Self {
        SymbolMap {
            entries: Vec::with_capacity(2),
        }
    }

    /// Retrieves the value for the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<i32>::new();
    ///
    /// map.put(42, 1);
    ///
    /// assert_eq!(map.get(42).unwrap(), &1);
    /// assert_eq!(map.get(99), None);
    /// ```
    pub fn get(&self, key: Symbol) -> Option<&V> {
        if let Ok(pos) = self.entries.binary_search_by_key(&key, |entry| entry.0) {
            Some(&self.entries[pos].1)
        } else {
            None
        }
    }

    /// Retrieves the value for the given key or creates a new one in none exists.
    ///
    /// If a new value is created, [Default::default](Default::default) is invoked and a mutable
    /// reference is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<i32>::new();
    ///
    /// map.put(42, 1);
    ///
    /// assert_eq!(map.get_or_insert(42, || 1), &1);
    /// assert_eq!(map.get_or_insert(23, || 0), &0);
    /// ```
    pub fn get_or_insert(&mut self, key: Symbol, mut factory: impl FnMut() -> V) -> &mut V {
        match self.entries.binary_search_by_key(&key, |entry| entry.0) {
            Ok(pos) => &mut self.entries[pos].1,
            Err(pos) => {
                self.entries.insert(pos, (key, factory()));
                &mut self.entries[pos].1
            }
        }
    }

    /// Retrieves the value for the given key as mutable reference.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// map.put(42, "Hello".to_owned());
    /// map.get_mut(42).unwrap().push_str(" World");
    ///
    /// assert_eq!(map.get(42).unwrap().as_str(), "Hello World");
    /// ```
    pub fn get_mut(&mut self, key: Symbol) -> Option<&mut V> {
        if let Ok(pos) = self.entries.binary_search_by_key(&key, |entry| entry.0) {
            Some(&mut self.entries[pos].1)
        } else {
            None
        }
    }

    /// Puts a value in the for the given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// map.put(42, "Hello".to_owned());
    ///
    /// assert_eq!(map.get(42).unwrap().as_str(), "Hello");
    /// ```
    pub fn put(&mut self, key: Symbol, value: V) {
        match self.entries.binary_search_by_key(&key, |entry| entry.0) {
            Ok(pos) => self.entries[pos].1 = value,
            Err(pos) => self.entries.insert(pos, (key, value)),
        }
    }

    /// Counts the number of entries in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// // If an entry is put in the map, its size is incremented...
    /// map.put(42, "Hello".to_owned());
    /// assert_eq!(map.len(), 1);
    ///    
    /// // If the entry is overwritten, the size remains the same...
    /// map.put(42, "Hello".to_owned());
    /// assert_eq!(map.len(), 1);
    ///
    /// // If another entry is added, its size grows once again...
    /// map.put(99, "World".to_owned());
    /// assert_eq!(map.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Determines if this map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// assert_eq!(map.is_empty(), true);
    /// map.put(42, "Hello".to_owned());
    /// assert_eq!(map.is_empty(), false);
    /// ```
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Reports the capacity currently reserved.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// let initial_capacity = map.capacity();
    ///
    /// for i in 1..=initial_capacity+1 {
    ///     map.put(i as i32, "Hello".to_owned());
    /// }
    ///
    /// assert_eq!(map.capacity() > initial_capacity, true);
    /// ```
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Provides an iterator over all entries in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// # use jupiter::ig::symbols::SymbolMap;
    /// # use itertools::Itertools;
    /// let mut map = SymbolMap::<String>::new();
    ///
    /// map.put(42, "Hello".to_owned());
    /// map.put(99, "World".to_owned());
    ///
    /// let string = map.entries().map(|(key, value)| format!("{}: {}", key, value)).join(", ");
    /// assert_eq!(string, "42: Hello, 99: World");
    /// ```
    pub fn entries(&self) -> Iter<(Symbol, V)> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use crate::ig::symbols::{SymbolMap, SymbolTable};

    #[test]
    pub fn resolve_and_lookup_works() {
        let mut table = SymbolTable::new();

        let symbol = table.find_or_create("Hello").unwrap();
        assert_eq!(table.find_or_create("Hello").unwrap(), symbol);
        assert_eq!(table.resolve("Hello").unwrap(), symbol);
        assert_eq!(table.lookup(symbol), "Hello");
        assert_eq!(table.lookup(-1), "");
        assert_eq!(table.resolve("World").is_none(), true);
        assert_eq!(table.len(), 1);
    }

    #[test]
    pub fn get_and_put_work_for_symbol_map() {
        let mut map = SymbolMap::new();
        for symbol in (1..128).rev() {
            map.put(symbol, symbol);
        }
        for symbol in 1..128 {
            assert_eq!(*map.get(symbol).unwrap(), symbol);
            assert_eq!(*map.get_mut(symbol).unwrap(), symbol);
        }

        assert_eq!(map.get(130).is_none(), true);
        assert_eq!(map.get_mut(130).is_none(), true);

        map.put(1, 42);
        assert_eq!(*map.get(1).unwrap(), 42);
        assert_eq!(*map.get_mut(1).unwrap(), 42);
        for symbol in 2..128 {
            assert_eq!(*map.get(symbol).unwrap(), symbol);
            assert_eq!(*map.get_mut(symbol).unwrap(), symbol);
        }

        assert_eq!(map.len(), 127);
        assert_eq!(
            map.entries().map(|(symbol, _)| symbol).sum::<i32>(),
            127 * 128 / 2
        );
    }

    #[test]
    pub fn get_or_insert_works() {
        let mut map = SymbolMap::new();

        assert_eq!(map.get_or_insert(42, || 0), &0);

        map.put(42, 1);
        assert_eq!(map.get_or_insert(42, || 1), &1);

        *map.get_or_insert(23, || 0) = 5;
        assert_eq!(map.get_or_insert(23, || 0), &5);
    }
}
