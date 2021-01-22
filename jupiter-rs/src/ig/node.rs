//! Provides the internal enum which is used to store the actual node data.
//!
//! As a node can either be a string, int, bool, list or inner object, we use an enum
//! to represent each type. As we also use this enum to distinguish inlined strings from
//! boxed ones, we hide all this behind [Element](crate::ig::docs::Element).
use crate::ig::strings::{BoxedString, InlineString};
use crate::ig::symbols::{Symbol, SymbolMap};

/// Contains the size occupied by a symbol map, as this is the largest payload.
///
/// This is then used to determine if the optimal size of `InlineString`, which is used to store
/// small strings in place.
pub const NODE_MAP_SIZE: usize = std::mem::size_of::<SymbolMap<Node>>();

/// Provides the internal representation of an element within the information graph.
///
/// Note that a `Doc` internally consists of `Nodes` but only hands them out wrapped as
/// `Element`. This is done for two reasons:
///
/// 1) We always have to pass a reference to the symbol table along in case we need to resolve
///    or lookup a key.
/// 2) We do not want to reveal any of the inner workings of the graph to minimize the public
///    API.
#[derive(Debug, PartialEq)]
pub enum Node {
    Empty,
    Integer(i64),
    BoxedString(BoxedString),
    InlineString(InlineString),
    Boolean(bool),
    List(Vec<Node>),
    Object(SymbolMap<Node>),
    Symbol(Symbol),
}

impl Node {
    /// Computes the **additional** size allocated for this `Node`.
    ///
    /// Therefore a node which stores all data in place (like `Node::Integer` or
    /// `Node::InlineString`) will report 0. Others, as `Node::BoxedString` will return the
    /// number of additionally allocated bytes.
    pub fn allocated_size(&self) -> usize {
        match self {
            Node::BoxedString(str) => std::mem::size_of::<usize>() + str.len(),
            Node::List(list) => {
                let vector_size = list.capacity() * std::mem::size_of::<Node>();
                let content_size: usize = list.iter().map(|node| node.allocated_size()).sum();

                vector_size + content_size
            }
            Node::Object(map) => {
                let vector_size =
                    map.capacity() * (std::mem::size_of::<Symbol>() + std::mem::size_of::<Node>());
                let content_size: usize = map.entries().map(|node| node.1.allocated_size()).sum();

                vector_size + content_size
            }
            _ => 0,
        }
    }
}

impl Default for Node {
    /// Constructs a default element for `Node` which is simply empty.
    fn default() -> Self {
        Node::Empty
    }
}

impl From<&str> for Node {
    /// Constructs a `Node` wrapping the given string.
    ///
    /// If the string is short enough, it will be put in place. Otherwise a new string is
    /// allocated on the heap.
    fn from(value: &str) -> Self {
        if value.len() <= InlineString::MAX {
            Node::InlineString(InlineString::from(value))
        } else {
            Node::BoxedString(BoxedString::from(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ig::node::Node;
    use crate::ig::strings::{BoxedString, InlineString};

    #[test]
    fn nodes_are_empty_by_default() {
        assert_eq!(Node::default(), Node::Empty);
    }

    #[test]
    fn from_string_works() {
        // Short strings are represented as InlineString...
        assert_eq!(
            Node::from("Hello"),
            Node::InlineString(InlineString::from("Hello"))
        );

        // Longed ones are allocated on the heap using BoxedString...
        assert_eq!(
            Node::from("X".repeat(64).as_str()),
            Node::BoxedString(BoxedString::from("X".repeat(64).as_str()))
        );
    }

    #[test]
    fn allocated_size_works() {
        assert_eq!(Node::Integer(1).allocated_size(), 0);
        assert_eq!(Node::Boolean(true).allocated_size(), 0);
        assert_eq!(
            Node::InlineString(InlineString::from("Hello")).allocated_size(),
            0
        );
        assert_eq!(
            Node::BoxedString(BoxedString::from("Hello")).allocated_size(),
            "Hello".len() + std::mem::size_of::<usize>()
        );
    }
}
