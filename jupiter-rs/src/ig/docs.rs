//! Contains the core elements to read / query an information graph.
//!
//! Each information graph is represented by a `Doc` which provides access to its nodes and
//! leaves via `Element`. A `Doc` is built using [DocBuilder](crate::ig::builder::DocBuilder)
//! and can then be further optimized for read access using [Table](crate::idb::table::Table).
use crate::ig::node::Node;
use crate::ig::symbols::{Symbol, SymbolTable};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::slice::Iter;

/// Provides the root node of an information graph.
///
/// This essentially bundles the [SymbolTable](crate::ig::symbols::SymbolTable) and the
/// root node of the graph.
///
/// Note that a `Doc` cannot be modified once it has been [built](crate::ig::builder) which
/// permits to use a very efficient memory layout and also permits to cache queries, lookup indices
/// and query results as provided by [Table](crate::idb::table::Table).
pub struct Doc {
    root: Node,
    symbols: SymbolTable,
}

/// Provides a node or leaf within the information graph.
///
/// Note that this is mainly a pointer into a `Doc` therefore it can be
/// copied quite cheaply.
#[derive(Copy, Clone)]
pub struct Element<'a> {
    doc: &'a Doc,
    node: &'a Node,
}

/// Represents an `Iterator` over all child elements of a list.
pub struct ElementIter<'a> {
    doc: &'a Doc,
    iter: Option<Iter<'a, Node>>,
}

/// Represents an `Iterator` over all key/value pairs of an inner object.
pub struct EntryIter<'a> {
    doc: &'a Doc,
    iter: Option<Iter<'a, (Symbol, Node)>>,
}

/// Represents a pre-compiled query.
///
/// A query can be obtained using [Doc::compile](Doc::compile). And then repeatedly being
/// executed using [Query::execute]{Query::execute}.
///
/// Compiling a query essentially splits the given query string at "." and then resolves each
/// part of the query into a `Symbol`. Therefore a query compiled for one `Doc` cannot be used
/// for another as its result would be entirely undefined. Also note that compiling queries is
/// rather fast, therefore ad-hoc queries can be executed using [Element::query](Element::query).
#[derive(Clone)]
pub struct Query {
    path: Vec<Symbol>,
}

impl Query {
    /// Determines if this is a root query (".").
    pub fn is_root_node_query(&self) -> bool {
        self.path.is_empty()
    }
}

impl Doc {
    /// Creates an entirely empty doc which can be used as a fallback or placeholder.
    pub fn empty() -> Self {
        Doc {
            symbols: SymbolTable::new(),
            root: Node::Empty,
        }
    }

    /// Creates a new document by combining the given `SymbolTable` and `Node`.
    ///
    /// Note that this is mainly an internal API as [DocBuilder](crate::ig::builder::DocBuilder)
    /// should be used to generate new docs.
    pub fn new(symbols: SymbolTable, root: Node) -> Self {
        Doc { root, symbols }
    }

    /// Returns the root node of this doc which is either a list or a map.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(1);
    /// let doc = builder.build_list(list_builder);
    ///
    /// let root = doc.root();
    ///
    /// assert_eq!(root.len(), 1)
    /// ```
    pub fn root(&self) -> Element {
        Element {
            doc: self,
            node: &self.root,
        }
    }

    /// Compiles the given query.
    ///
    /// A query is composed of a chain of keys separated by ".". For nested objects,
    /// we apply one key after another to obtain the resulting target element.
    ///
    /// Note that "." represents an identity query which returns the element itself.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut object_builder = builder.obj();
    /// let mut inner_builder = builder.obj();
    /// inner_builder.put_int("Bar", 42).unwrap();
    /// object_builder.put_object("Foo", inner_builder).unwrap();
    /// let doc = builder.build_object(object_builder);
    ///
    /// let query = doc.compile("Foo.Bar");
    /// assert_eq!(query.execute(doc.root()).as_int().unwrap(), 42);
    ///
    /// let query = doc.compile(".");
    /// assert!(query.execute(doc.root()).is_object());
    /// ```
    ///
    /// # Performance
    /// Compiling queries is rather fast, therefore ad-hoc queries can be executed using
    /// [Element::query](Element::query). Pre-compiled queries are only feasible if a query is
    /// known to be executed several times (e.g. when iterating over a list of objects and executing
    /// the same query each time).
    ///
    /// Also note that if we encounter an unknown symbol here, we know that the query cannot be
    /// fullfilled and therefore return an empty query here which always yields and empty
    /// result without any actual work being done.
    pub fn compile(&self, query: impl AsRef<str>) -> Query {
        let query = query.as_ref();
        if "." == query {
            return Query { path: Vec::new() };
        }

        let mut path = Vec::new();
        for part in query.split('.') {
            if let Some(symbol) = self.symbols.resolve(part) {
                path.push(symbol);
            } else {
                // Creates a "poisoned" query which will always yield an empty result...
                return Query { path: vec![-1] };
            }
        }

        Query { path }
    }

    /// Provides mutable access on the underlying symbol table.
    pub fn symbols_mut(&mut self) -> &mut SymbolTable {
        &mut self.symbols
    }

    /// Provides readonly access on the symbol table.
    pub fn symbols(&self) -> &SymbolTable {
        &self.symbols
    }

    /// Estimates the size (in bytes) occupied by this doc.
    ///
    /// This accounts for both, the `SymbolTable` and the actual information graph. Note that this
    /// is an approximation as not all data structures reveal their true size.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut object_builder = builder.obj();
    /// let mut inner_builder = builder.obj();
    /// inner_builder.put_int("Bar", 42).unwrap();
    /// object_builder.put_object("Foo", inner_builder).unwrap();
    /// let doc = builder.build_object(object_builder);
    ///
    /// println!("{}", doc.allocated_size());
    /// ```
    pub fn allocated_size(&self) -> usize {
        self.root.allocated_size() + self.symbols.allocated_size()
    }
}

/// Represents a node or leaf of the information graph.
///
/// This could be either an inner object, a list, an int, a bool or a string. Note that
/// the element itself is just a pointer into the graph and can therefore be copied. However,
/// its lifetime depends of the `Doc` it references.
impl<'a> Element<'a> {
    /// Executes an ad-hoc query on this element.
    ///
    /// See [Doc::compile](Doc::compile) for a description of the query syntax.
    ///
    /// Note that a query can be executed on any element but will only ever yield a non
    /// empty result if it is executed on an object.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut object_builder = builder.obj();
    /// let mut inner_builder = builder.obj();
    /// inner_builder.put_int("Bar", 42).unwrap();
    /// object_builder.put_object("Foo", inner_builder).unwrap();
    /// let doc = builder.build_object(object_builder);
    ///
    /// let result = doc.root().query("Foo.Bar");
    ///
    /// assert_eq!(result.as_int().unwrap(), 42)
    /// ```
    pub fn query(self, query: impl AsRef<str>) -> Element<'a> {
        self.doc.compile(query).execute(self)
    }

    /// Determines if this element is empty.
    ///
    /// Such elements are e.g. returned if a query doesn't match or if an out of range
    /// index is used when accessing a list.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::docs::Doc;
    /// let doc = Doc::empty();
    ///
    /// assert_eq!(doc.root().is_empty(), true);
    /// assert_eq!(doc.root().query("unknown").is_empty(), true);
    /// assert_eq!(doc.root().at(100).is_empty(), true);
    /// ```
    pub fn is_empty(&self) -> bool {
        matches!(self.node, Node::Empty)
    }

    /// Returns the string represented by this element.
    ///
    /// Note that this will only return a string if the underlying data is one.
    /// No other element will be converted into a string, as this is handled by
    /// `to_string'.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_string("Foo");
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().at(0).as_str().unwrap(), "Foo");
    /// ```
    pub fn as_str(&self) -> Option<&'a str> {
        match self.node {
            Node::InlineString(str) => Some(str.as_ref()),
            Node::BoxedString(str) => Some(str.as_ref()),
            _ => None,
        }
    }

    /// Returns a string representation of all scalar values.
    ///
    /// Returns a string for string, int or bool values. Everything else is represented
    /// as "". Note that `Element` implements `Debug` which will also render a representation
    /// for lists and objects, but its representation is only for debugging purposes and should
    /// not being relied up on.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_string("Foo");
    /// list_builder.append_bool(true);
    /// list_builder.append_int(42);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().at(0).to_str().as_ref(), "Foo");
    /// assert_eq!(doc.root().at(1).to_str().as_ref(), "true");
    /// assert_eq!(doc.root().at(2).to_str().as_ref(), "42");
    /// ```
    pub fn to_str(&self) -> Cow<str> {
        match self.node {
            Node::InlineString(str) => Cow::Borrowed(str.as_ref()),
            Node::BoxedString(str) => Cow::Borrowed(str.as_ref()),
            Node::Integer(value) => Cow::Owned(format!("{}", value)),
            Node::Boolean(true) => Cow::Borrowed("true"),
            Node::Boolean(false) => Cow::Borrowed("false"),
            Node::Symbol(symbol) => Cow::Borrowed(self.doc.symbols.lookup(*symbol)),
            _ => Cow::Borrowed(""),
        }
    }

    /// Returns the int value represented by this element.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(42);
    /// let doc = builder.build_list(list_builder);
    ///
    /// let value = doc.root().at(0).as_int().unwrap();
    ///
    /// assert_eq!(value, 42);
    /// ```
    pub fn as_int(&self) -> Option<i64> {
        if let Node::Integer(value) = self.node {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns the bool value represented by this element.
    ///
    /// Note that this actually represents a tri-state logic by returning an `Option<bool>`.
    /// This helps to distinguish missing values from `false`. If this isn't necessarry,
    /// [as_bool()](Element::as_bool) can be used which treats both cases as `false`.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_bool(true);
    /// list_builder.append_bool(false);
    /// list_builder.append_string("true");
    /// list_builder.append_int(1);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().at(0).try_as_bool().unwrap(), true);
    /// assert_eq!(doc.root().at(1).try_as_bool().unwrap(), false);
    /// assert_eq!(doc.root().at(2).try_as_bool().is_none(), true);
    /// assert_eq!(doc.root().at(3).try_as_bool().is_none(), true);
    /// assert_eq!(doc.root().at(4).try_as_bool().is_none(), true);
    /// ```
    pub fn try_as_bool(&self) -> Option<bool> {
        if let Node::Boolean(value) = self.node {
            Some(*value)
        } else {
            None
        }
    }

    /// Returns `true` if this element wraps an actual `bool` `true` or `false` in all other cases.
    ///
    /// Note that [try_as_bool()](Element::try_as_bool) can be used if `false` needs to be
    /// distinguished from missing or non-boolean elements.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_bool(true);
    /// list_builder.append_bool(false);
    /// list_builder.append_string("true");
    /// list_builder.append_int(1);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().at(0).as_bool(), true);
    /// assert_eq!(doc.root().at(1).as_bool(), false);
    /// assert_eq!(doc.root().at(2).as_bool(), false);
    /// assert_eq!(doc.root().at(3).as_bool(), false);
    /// assert_eq!(doc.root().at(4).as_bool(), false);
    /// ```
    pub fn as_bool(&self) -> bool {
        if let Node::Boolean(value) = self.node {
            *value
        } else {
            false
        }
    }

    /// Determines if this element is a list.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(1);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().is_list(), true);
    /// assert_eq!(doc.root().at(1).is_list(), false);
    /// ```
    pub fn is_list(&self) -> bool {
        matches!(self.node, Node::List(_))
    }

    /// Determines if this element is an object.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    /// obj_builder.put_int("test", 1);
    /// let doc = builder.build_object(obj_builder);
    ///
    /// assert_eq!(doc.root().is_object(), true);
    /// assert_eq!(doc.root().query("test").is_object(), false);
    /// ```
    pub fn is_object(&self) -> bool {
        matches!(self.node, Node::Object(_))
    }

    /// Returns the number of elements in the underlying list or number of entries in the
    /// underlying map.
    ///
    /// If this element is neither a list nor a map, 0 is returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// let mut obj_builder = builder.obj();
    /// obj_builder.put_int("Foo", 42);
    /// list_builder.append_object(obj_builder);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().len(), 1);
    /// assert_eq!(doc.root().at(0).len(), 1);
    ///
    /// assert_eq!(doc.root().at(0).query("Foo").len(), 0);
    /// assert_eq!(doc.root().at(1).len(), 0);
    /// ```
    pub fn len(&self) -> usize {
        match self.node {
            Node::List(list) => list.len(),
            Node::Object(map) => map.len(),
            _ => 0,
        }
    }

    /// Returns the n-th element of the underlying list.
    ///
    /// If the underlying element isn't a list or if the given index is outside of the range
    /// of the list, an `empty` element is returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_string("Foo");
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().at(0).as_str().unwrap(), "Foo");
    /// assert_eq!(doc.root().at(1).is_empty(), true);
    /// assert_eq!(doc.root().at(0).at(1).is_empty(), true);
    /// ```
    pub fn at(&self, index: usize) -> Element<'a> {
        if let Node::List(list) = self.node {
            if let Some(node) = list.get(index) {
                return Element {
                    doc: self.doc,
                    node,
                };
            }
        }

        Element {
            doc: self.doc,
            node: &Node::Empty,
        }
    }

    /// Returns an iterator for all elements of the underlying list.
    ///
    /// If the list is empty or if the underlying element isn't a list, an
    /// empty iterator will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// # use itertools::Itertools;
    /// # use jupiter::ig::docs::Doc;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(1);
    /// list_builder.append_int(2);
    /// list_builder.append_int(3);
    /// let doc = builder.build_list(list_builder);
    ///
    /// assert_eq!(doc.root().iter().map(|e| format!("{}", e.to_str())).join(", "), "1, 2, 3");
    ///
    /// assert_eq!(Doc::empty().root().iter().next().is_none(), true);
    /// ```
    pub fn iter(&'a self) -> ElementIter<'a> {
        if let Node::List(list) = self.node {
            ElementIter {
                doc: self.doc,
                iter: Some(list.iter()),
            }
        } else {
            ElementIter {
                doc: self.doc,
                iter: None,
            }
        }
    }

    /// Returns an iterator over all entries of the underlying map.
    ///
    /// If the underlying element isn't a map, an empty iterator is returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// # use itertools::Itertools;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    /// obj_builder.put_int("Foo", 42);
    /// obj_builder.put_int("Bar", 24);
    /// let doc = builder.build_object(obj_builder);
    ///
    /// let entry_string = doc
    ///                     .root()
    ///                     .entries()
    ///                     .map(|(k, v)| format!("{}: {}", k, v.to_str()))
    ///                     .join(", ");
    /// assert_eq!(entry_string, "Foo: 42, Bar: 24");
    /// ```
    pub fn entries(self) -> EntryIter<'a> {
        if let Node::Object(map) = self.node {
            EntryIter {
                doc: self.doc,
                iter: Some(map.entries()),
            }
        } else {
            EntryIter {
                doc: self.doc,
                iter: None,
            }
        }
    }
}

impl Query {
    /// Executes the query against the given element.
    ///
    /// Note that the element must be part of the same doc for which the query has been compiled
    /// otherwise the return value is undefined.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut object_builder = builder.obj();
    /// let mut inner_builder = builder.obj();
    /// inner_builder.put_int("Bar", 42).unwrap();
    /// object_builder.put_object("Foo", inner_builder).unwrap();
    ///
    /// let doc = builder.build_object(object_builder);
    ///
    /// let query = doc.compile("Foo.Bar");
    /// assert_eq!(query.execute(doc.root()).as_int().unwrap(), 42);
    ///
    /// let query = doc.compile("XXX.UNKNOWN");
    /// assert!(query.execute(doc.root()).is_empty());
    ///
    /// let query = doc.compile(".");
    /// assert!(query.execute(doc.root()).is_object());
    /// ```
    pub fn execute<'a>(&self, element: Element<'a>) -> Element<'a> {
        let mut current_node = element.node;

        for key in self.path.iter() {
            if let Node::Object(map) = current_node {
                current_node = map.get(*key).unwrap_or(&Node::Empty);
            } else {
                current_node = &Node::Empty;
            }
        }

        Element {
            doc: element.doc,
            node: current_node,
        }
    }

    /// Executes this query against the given element and returns all matches.
    ///
    /// # Example
    /// There might be several matches if there is an inner list in the doc:
    /// ```
    ///
    /// use yaml_rust::YamlLoader;
    /// use jupiter::ig::yaml::list_to_doc;
    /// let input = r#"
    /// test:
    ///     - foo:
    ///         label: bar
    ///     - foo:
    ///         label: baz
    ///         "#;
    ///
    /// let rows = YamlLoader::load_from_str(input).unwrap();
    /// let doc = list_to_doc(rows.as_slice(), |_| true).unwrap();
    ///
    ///
    /// let query = doc.compile("test.foo.label");
    /// let matches = query.execute_all(doc.root());
    /// assert_eq!(matches.len(), 2);
    /// assert_eq!(matches[0].as_str().unwrap(), "bar");
    /// assert_eq!(matches[1].as_str().unwrap(), "baz");
    /// ```
    pub fn execute_all<'a>(&self, element: Element<'a>) -> Vec<Element<'a>> {
        let mut result = Vec::new();
        self.execute_into(0, element.doc, element.node, &mut result);

        result
    }

    fn execute_into<'a>(
        &self,
        index: usize,
        doc: &'a Doc,
        node: &'a Node,
        results: &mut Vec<Element<'a>>,
    ) {
        if let Some(key) = self.path.get(index) {
            if let Node::Object(map) = node {
                self.execute_into(
                    index + 1,
                    doc,
                    map.get(*key).unwrap_or(&Node::Empty),
                    results,
                );
            } else if let Node::List(list) = node {
                for child in list {
                    self.execute_into(index, doc, child, results);
                }
            }
        } else {
            results.push(Element { doc, node });
        }
    }
}

impl Debug for Element<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.node {
            Node::Empty => write!(f, "(Empty)"),
            Node::Integer(value) => write!(f, "{}", value),
            Node::Boolean(value) => write!(f, "{}", value),
            Node::InlineString(value) => write!(f, "{}", value.as_ref()),
            Node::BoxedString(value) => write!(f, "{}", value.as_ref()),
            Node::List(_) => f.debug_list().entries(self.iter()).finish(),
            Node::Object(_) => {
                let mut helper = f.debug_map();
                for (name, value) in self.entries() {
                    let _ = helper.key(&name).value(&value);
                }
                helper.finish()
            }
            Node::Symbol(value) => write!(f, "{} ({})", value, self.doc.symbols.lookup(*value)),
        }
    }
}

impl<'a> Iterator for ElementIter<'a> {
    type Item = Element<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.iter {
            if let Some(node) = iter.next() {
                Some(Element {
                    doc: self.doc,
                    node,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a> Iterator for EntryIter<'a> {
    type Item = (&'a str, Element<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(ref mut iter) = self.iter {
            if let Some((symbol, node)) = iter.next() {
                Some((
                    self.doc.symbols.lookup(*symbol),
                    Element {
                        doc: self.doc,
                        node,
                    },
                ))
            } else {
                None
            }
        } else {
            None
        }
    }
}
