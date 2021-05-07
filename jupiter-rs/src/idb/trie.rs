//! Provides a lookup table for arbitrary data structures using a TRIE.
//!
//! A TRIE is a memory efficient was for providing string based lookup tables. Compared to hash maps
//! these should actually match their performance as computing the hash of a string is O(str len).
//! This is the same complexity of the actual lookup in the trie. However, a hash map needs to
//! allocate more memory than actually is required in order to achieve a certain level of
//! performance.
//!
//! However, the main benefit of a TRIE is that being a tree like structure, a depth first search
//! can be performed. This permits to not only query matching values but also support a efficient
//! way of performing a prefix search.
//!
//! Implementing a TRIE is quite easy as it is a tree where every branch is labeled with a character
//! (an u8 in our case as we us the UTF-8 representation of `str`). Inserting a value is performed
//! by iterating over its bytes and navigating or building the appropriate tree. The last node
//! being reached is then supplied with the value to insert. Performing a lookup works quite the
//! same way.
use std::ops::Deref;
use std::slice::Iter;

/// Represents a trie to map strings to values of type `T`.
pub struct Trie<T> {
    root: TrieNode<T>,
}

struct TrieNode<T> {
    branches: Vec<(u8, TrieNode<T>)>,
    // Note that we deliberately put the Vec in a box here, as the Option is None most of the time
    // anyway. Therefore we only want a single word pointer in favor or storing 3 words required
    // or a Vec.
    #[allow(clippy::clippy::box_vec)]
    values: Option<Box<Vec<T>>>,
}

impl<T> TrieNode<T> {
    /// Computes the additionally allocated memory outside of the node.
    fn allocated_size(&self) -> usize {
        let mut result = self.branches.capacity() * std::mem::size_of::<(u8, TrieNode<T>)>();
        for (_, branch) in self.branches.iter() {
            result += branch.allocated_size();
        }
        if let Some(values) = &self.values {
            result += std::mem::size_of::<Vec<T>>();
            result += values.capacity() * std::mem::size_of::<T>();
        }

        result
    }

    /// Counts the number of child nodes in the sub tree represented by this node.
    fn num_children(&self) -> usize {
        let mut result = self.branches.len();
        for (_, branch) in self.branches.iter() {
            result += branch.num_children();
        }

        result
    }

    /// Counts the number of values in the sub tree represented by this node.
    fn num_values(&self) -> usize {
        let mut result = if let Some(values) = &self.values {
            values.len()
        } else {
            0
        };
        for (_, branch) in self.branches.iter() {
            result += branch.num_values();
        }

        result
    }
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie::new()
    }
}

impl<T> Trie<T> {
    /// Creates a new and empty trie.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let trie : Trie<i32> = Trie::new();
    ///
    /// assert_eq!(trie.num_nodes(), 0);
    /// assert_eq!(trie.num_entries(), 0);
    /// ```
    pub fn new() -> Self {
        Trie {
            root: TrieNode {
                branches: Vec::new(),
                values: None,
            },
        }
    }

    fn insert_checked<P>(&mut self, key: &str, value: T, predicate: P)
    where
        P: Fn(&T, &TrieNode<T>) -> bool,
    {
        let mut node = &mut self.root;

        for &ch in key.as_bytes().iter() {
            if let Some(idx) = node.branches.iter().position(|branch| branch.0 == ch) {
                node = unsafe { &mut node.branches.get_unchecked_mut(idx).1 };
            } else {
                node.branches.push((
                    ch,
                    TrieNode {
                        branches: Vec::with_capacity(1),
                        values: None,
                    },
                ));

                // Walk down to our newly created node. We can unwrap here, as we know for sure
                // that the vec isn't empty.
                let last_index = node.branches.len() - 1;
                node = unsafe { &mut node.branches.get_unchecked_mut(last_index).1 };
            }
        }

        if predicate(&value, node) {
            // insert the value...
            if let Some(values) = &mut node.values {
                values.push(value);
            } else {
                node.values = Some(Box::new(vec![value]));
            }
        }
    }

    /// Inserts the given value for the given key.
    ///
    /// Note that this is optimized for performance. Therefore this doesn't check if the value is
    /// already present for the key. If the exactly same was already present it will still be added
    /// again. See [Trie::insert_unique] for a way of only inserting entries
    /// once.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// trie.insert("a", 1);
    /// assert_eq!(trie.num_nodes(), 1);
    /// assert_eq!(trie.num_entries(), 1);
    ///
    /// // Duplicate values can be inserted...
    /// trie.insert("a", 1);
    /// assert_eq!(trie.num_nodes(), 1);
    /// assert_eq!(trie.num_entries(), 2);
    ///
    /// // The tree grows as required...
    /// trie.insert("ab", 1);
    /// assert_eq!(trie.num_nodes(), 2);
    /// assert_eq!(trie.num_entries(), 3);
    /// ```
    pub fn insert(&mut self, key: &str, value: T) {
        self.insert_checked(key, value, |_, _| true)
    }

    /// Retrieves the values stored for a given string.
    ///
    /// Note that [Trie::query] provides a boilerplate API
    /// which directly returns an iterator and therefore doesn't require unwrapping the Option.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// // A simple value can be retrieved...
    /// trie.insert("a", 1);
    /// assert_eq!(trie.query_values("a").unwrap()[0], 1);
    ///
    /// // A duplicate value can be retrieved...
    /// trie.insert("a", 2);
    /// assert_eq!(trie.query_values("a").unwrap()[0], 1);
    /// assert_eq!(trie.query_values("a").unwrap()[1], 2);
    ///
    /// // A "child-entry" can be retrieved and leaves the actual entries intact...
    /// trie.insert("ab", 3);
    /// assert_eq!(trie.query_values("a").unwrap()[0], 1);
    /// assert_eq!(trie.query_values("a").unwrap()[1], 2);
    /// assert_eq!(trie.query_values("ab").unwrap()[0], 3);
    /// ```
    pub fn query_values(&self, query: impl AsRef<str>) -> Option<&Vec<T>> {
        let mut node = &self.root;

        for &ch in query.as_ref().as_bytes().iter() {
            node = match node.branches.iter().find(|branch| branch.0 == ch) {
                Some((_, node)) => node,
                None => return None,
            };
        }

        node.values.as_ref().map(|values| values.deref())
    }

    /// Performs a partial lookup in the Trie.
    ///
    /// This is kind of the inverse of [Trie::prefix_query] as re iterate through the Trie as
    /// long as we can. Once we cannot continue, we return the matched node and the length of
    /// the query prefix we matched.
    ///
    /// # Example
    /// ```rust
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    /// trie.insert("abc", 1);
    ///
    /// let (len, data) = trie.partial_query("abcd").unwrap();
    /// assert_eq!(len, 3);
    /// assert_eq!(data[0], 1);
    /// ```
    pub fn partial_query(&self, query: impl AsRef<str>) -> Option<(usize, &Vec<T>)> {
        let mut node = &self.root;
        let query = query.as_ref().as_bytes();
        let mut intermediate_result = None;

        for (index, &ch) in query.iter().enumerate() {
            if let Some(values) = &node.values {
                intermediate_result = Some((index, values.deref()));
            }
            node = match node.branches.iter().find(|branch| branch.0 == ch) {
                Some((_, node)) => node,
                None => return intermediate_result,
            };
        }

        if let Some(values) = &node.values {
            Some((query.len(), values.deref()))
        } else {
            intermediate_result
        }
    }

    /// Provides a boilerplate way of querying values.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// trie.insert("a", 1);
    /// assert_eq!(trie.query("a").next().unwrap(), &1);
    /// assert_eq!(trie.query("unknown").next().is_none(), true);
    /// ```
    pub fn query(&self, query: impl AsRef<str>) -> OptionalIter<T> {
        if let Some(values) = self.query_values(query) {
            OptionalIter::Found(values.iter())
        } else {
            OptionalIter::Empty
        }
    }

    /// Returns an iterator which performs a depth first search yielding the complete sub tree
    /// below the given prefix.
    ///
    /// # Example
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    /// trie.insert("abc", 1);
    /// trie.insert("abc", 2);
    /// trie.insert("abcd", 3);
    ///
    /// let results: Vec<&i32> = trie.prefix_query("abc").collect();
    /// assert_eq!(results, vec![&1, &2, &3]);
    /// ```
    pub fn prefix_query(&self, prefix: &str) -> PrefixIter<T> {
        let mut node = &self.root;

        // Finding the "end node" of the prefix is pretty much the same as performing a
        // simple query. The only difference is the way we abort if no matching sub tree
        // exists (we need to return an empty iterator here)...
        for &ch in prefix.as_bytes().iter() {
            node = match node.branches.iter().find(|branch| branch.0 == ch) {
                Some((_, node)) => node,
                None => {
                    return PrefixIter {
                        current_iter: None,
                        stack: vec![],
                    };
                }
            };
        }

        if let Some(values) = &node.values {
            PrefixIter {
                current_iter: Some(values.iter()),
                stack: vec![(0, node)],
            }
        } else {
            PrefixIter {
                current_iter: None,
                stack: vec![(0, node)],
            }
        }
    }

    /// Performs a depth first traversal of the TRIE while permitting to filter which branches to
    /// visit. We also permit to drag a state along which is passed into the predicate.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    /// trie.insert("1", 1);
    /// trie.insert("2", 2);
    /// trie.insert("3", 3);
    ///
    /// // This predicate accepts all branches and we therefore end up with all values.
    /// let results: Vec<&i32> = trie.filtered_depth_first(1, |s, ch| Some(1)).collect();
    /// assert_eq!(results, vec![&1, &2, &3]);
    /// ```
    pub fn filtered_depth_first<S, P>(
        &self,
        initial_state: S,
        predicate: P,
    ) -> FilteredDepthFirstIter<T, S, P>
    where
        P: Fn(S, u8) -> Option<S>,
    {
        // Here we start with the root node but depending if there are values present, we should
        // initialize the iterator properly. We could also always pass the iter in and let the
        // implementation below detect, that the values are empty and thus continue with the next
        // node. However, this case is quite common (values for "" are rare), therefore we perform
        // this check here (which is cheap and efficient).
        if let Some(values) = &self.root.values {
            FilteredDepthFirstIter {
                current_iter: Some(values.iter()),
                stack: vec![(0, initial_state, &self.root)],
                predicate,
            }
        } else {
            FilteredDepthFirstIter {
                current_iter: None,
                stack: vec![(0, initial_state, &self.root)],
                predicate,
            }
        }
    }

    /// Returns the amount of memory (in bytes) used by this trie.
    pub fn allocated_size(&self) -> usize {
        self.root.allocated_size()
    }

    /// Returns the number of entries stored in this trie.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// trie.insert("a", 1);
    /// assert_eq!(trie.num_entries(), 1);
    ///
    /// // Duplicate values can be inserted and are counted as extra entry.
    /// trie.insert("a", 1);
    /// assert_eq!(trie.num_entries(), 2);
    /// ```
    pub fn num_entries(&self) -> usize {
        self.root.num_values()
    }

    /// Returns the number of nodes in this trie.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// // A node per character is created...
    /// trie.insert("abc", 1);
    /// assert_eq!(trie.num_nodes(), 3);
    ///
    /// // Re-using an existing path won't add new nodes.
    /// trie.insert("abc", 1);
    /// assert_eq!(trie.num_nodes(), 3);
    /// ```
    pub fn num_nodes(&self) -> usize {
        self.root.num_children()
    }

    /// Performs a depth first walk and invokes the callback for each entry in the Trie.
    ///
    /// Note that this might be quite a long running task and there is no way of interrupting it
    /// (as no iterator is used). Therefore this is should only be used for data management tasks
    /// and not for lookups of any kind.
    ///
    /// Also note that this uses a stack based recursion. Therefore this must not be used if
    /// keys in this Trie are unhealthy long.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// // A node per character is created...
    /// trie.insert("abc", 1);
    /// trie.insert("abcd", 2);
    ///
    /// let mut counter = 0;
    /// trie.scan(&mut |_,_| counter += 1);
    /// assert_eq!(counter, 2);
    /// ```
    pub fn scan<C>(&self, callback: &mut C)
    where
        C: FnMut(&str, &T),
    {
        let mut prefix = Vec::new();
        Trie::scan_node(&mut prefix, &self.root, callback);
    }

    fn scan_node<C>(prefix: &mut Vec<u8>, node: &TrieNode<T>, callback: &mut C)
    where
        C: FnMut(&str, &T),
    {
        if let Some(values) = &node.values {
            if let Ok(prefix_str) = std::str::from_utf8(prefix.as_slice()) {
                for value in values.iter() {
                    callback(prefix_str, value);
                }
            }
        }

        for (ch, child) in &node.branches {
            prefix.push(*ch);

            Trie::scan_node(prefix, &child, callback);
            let _ = prefix.pop();
        }
    }
}

impl<T: PartialEq> Trie<T> {
    /// Inserts the given value for the given key.
    ///
    /// In contrast to [Trie::insert] this checks if the value is already present for
    /// the key and won't insert a duplicate.
    /// again.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::idb::trie::Trie;
    /// let mut trie = Trie::new();
    ///
    /// trie.insert_unique("a", 1);
    /// assert_eq!(trie.num_nodes(), 1);
    /// assert_eq!(trie.num_entries(), 1);
    ///
    /// // Duplicate values will not be inserted...
    /// trie.insert_unique("a", 1);
    /// assert_eq!(trie.num_nodes(), 1);
    /// assert_eq!(trie.num_entries(), 1);
    /// ```
    pub fn insert_unique(&mut self, key: &str, value: T) {
        self.insert_checked(key, value, |new_value, node| {
            if let Some(values) = &node.values {
                !values.contains(&new_value)
            } else {
                true
            }
        });
    }
}

/// A simple convenience type which gracefully handles the Option::None case for Trie::query.
pub enum OptionalIter<'a, T> {
    /// Represents a match.
    Found(Iter<'a, T>),
    /// Represents a miss.
    Empty,
}

impl<'a, T> Iterator for OptionalIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OptionalIter::Found(iter) => iter.next(),
            _ => None,
        }
    }
}

/// Represents the iterator used by [Trie::prefix_query].
pub struct PrefixIter<'a, T> {
    current_iter: Option<Iter<'a, T>>,
    stack: Vec<(usize, &'a TrieNode<T>)>,
}

impl<'a, T> Iterator for PrefixIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        // As long as we have values left from the last node which had values,
        // we emit those....
        if let Some(current) = &mut self.current_iter {
            if let Some(value) = current.next() {
                return Some(value);
            } else {
                self.current_iter = None;
            }
        }

        // ..otherwise, if the stack is empty, the traversal is complete and we can constantly
        // emit None...
        if self.stack.is_empty() {
            return None;
        }

        // ...in all other cases we perform a DFS (depth first search) for a node with values.
        while let Some((idx, node)) = self.stack.last_mut() {
            // Check the next branch in the top-most node...
            if let Some(child) = node.branches.get(*idx) {
                *idx += 1;
                // Inspect the child node next (this is a DFS after all...)
                self.stack.push((0, &child.1));
                if let Some(values) = &child.1.values {
                    // However, if the child has values, first and foremost emit those by
                    // storing them in current_iter (which will be picked up by the self.next()
                    // below.
                    self.current_iter = Some(values.iter());
                    break;
                }
            } else {
                // No branches left for this node, pop the stack and continue with the parent node..
                let _ = self.stack.pop();
            }
        }

        // We either found a node and setup current_iter properly OR the stack is completely
        // empty. Both cases are handled gracefully by the code above, therefore we simply
        // invoke next again (yes we could also whack a loop around all this, but this would
        // let the code look more complex so we rely on the compiler to figure this out
        // all by itself, tail call optimizations aren't that hard anyway...).
        self.next()
    }
}

/// Represents the iterator used by [Trie::filtered_depth_first].
pub struct FilteredDepthFirstIter<'a, T, S, P>
where
    P: Fn(S, u8) -> Option<S>,
{
    stack: Vec<(usize, S, &'a TrieNode<T>)>,
    current_iter: Option<Iter<'a, T>>,
    predicate: P,
}

impl<'a, T, S: Sized + Copy, P> Iterator for FilteredDepthFirstIter<'a, T, S, P>
where
    P: Fn(S, u8) -> Option<S>,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        // Note that this iterator performs almost just like PrefixIter::next.
        // The only difference is that the external state is dragged along and that we check which
        // branches to visit, using the given predicate.
        if let Some(current) = &mut self.current_iter {
            if let Some(value) = current.next() {
                return Some(value);
            } else {
                self.current_iter = None;
            }
        }
        if self.stack.is_empty() {
            return None;
        }

        while let Some((idx, state, node)) = self.stack.last_mut() {
            if let Some((ch, child)) = node.branches.get(*idx) {
                *idx += 1;

                // Note that we perform the predicate / state transition check here in contrast
                // to consuming all nodes
                if let Some(next_state) = (self.predicate)(*state, *ch) {
                    self.stack.push((0, next_state, child));
                    if let Some(values) = &child.values {
                        self.current_iter = Some(values.iter());
                        break;
                    }
                }
            } else {
                let _ = self.stack.pop();
            }
        }

        self.next()
    }
}

#[cfg(test)]
mod tests {
    use crate::idb::trie::Trie;

    #[test]
    fn insert_and_query_works() {
        let mut trie = Trie::new();
        trie.insert("abc", 42);
        trie.insert("abc", 0);
        trie.insert("test", 1);

        let mut iter = trie.query("abc");
        assert_eq!(iter.next().unwrap(), &42);
        assert_eq!(iter.next().unwrap(), &0);
        assert_eq!(trie.query_values("abc").unwrap()[0], 42);
        assert_eq!(trie.query_values("abc").unwrap()[1], 0);

        assert_eq!(trie.query("test").next().unwrap(), &1);
        assert_eq!(trie.query_values("test").unwrap()[0], 1);
        assert_eq!(trie.query("fail").next().is_none(), true);
        assert_eq!(trie.query_values("fail").is_none(), true);
    }

    #[test]
    fn prefix_search_works() {
        let mut trie = Trie::new();
        trie.insert("abc", 1);
        trie.insert("abc", 2);
        trie.insert("abcd", 3);
        trie.insert("abcde", 4);
        trie.insert("abcdf", 5);
        trie.insert("abd", 6);

        let results: Vec<&i32> = trie.prefix_query("").collect();
        assert_eq!(results, vec![&1, &2, &3, &4, &5, &6]);

        let results: Vec<&i32> = trie.prefix_query("a").collect();
        assert_eq!(results, vec![&1, &2, &3, &4, &5, &6]);

        let results: Vec<&i32> = trie.prefix_query("ab").collect();
        assert_eq!(results, vec![&1, &2, &3, &4, &5, &6]);

        let results: Vec<&i32> = trie.prefix_query("abc").collect();
        assert_eq!(results, vec![&1, &2, &3, &4, &5]);

        let results: Vec<&i32> = trie.prefix_query("abcd").collect();
        assert_eq!(results, vec![&3, &4, &5]);

        let results: Vec<&i32> = trie.prefix_query("abcdf").collect();
        assert_eq!(results, vec![&5]);

        let results: Vec<&i32> = trie.prefix_query("unknown").collect();
        assert_eq!(results.is_empty(), true);
    }

    #[test]
    fn depth_first_search_works() {
        let mut trie = Trie::new();
        trie.insert("1", 1);
        trie.insert("2", 2);
        trie.insert("3", 3);
        trie.insert("11", 4);
        trie.insert("12", 5);
        trie.insert("13", 6);
        trie.insert("121", 7);
        trie.insert("122", 8);
        trie.insert("123", 9);

        // Note this is a poor man's way of checking if the current state matches
        // the key of the branch to check.
        //
        // Therefore we only follow branches with the current state and then set the
        // next state and this next expected state to "+1"...
        let predicate = |s, ch| if ch == s + b'0' { Some(s + 1) } else { None };

        let results: Vec<&i32> = trie.filtered_depth_first(1, predicate).collect();

        // ...therefore we expect to visit "1" (with value 1), "12" (with value 5) and
        // "123" (with value 9):
        assert_eq!(results, vec![&1, &5, &9]);
    }

    #[test]
    fn scan_works() {
        let mut trie = Trie::new();
        trie.insert("A", 1);
        trie.insert("B", 2);
        trie.insert("AB", 4);
        trie.insert("ABC", 9);

        let mut buffer = String::new();
        trie.scan(&mut |prefix, item| buffer.push_str(format!("{}{}", prefix, item).as_str()));

        assert_eq!(buffer, "A1AB4ABC9B2");
    }
}
