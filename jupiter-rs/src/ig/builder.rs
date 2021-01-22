//! Provides `DocBuilder` which used to create a `Doc` programmatically.
//!
//! Many of the performance optimizations of `Infograph` are based on the fact
//! that a `Doc` is created once and then never modified. (Of course a new Doc can be created
//! to replace a current version, but the data itself is immutable).
//!
//! Therefore a `DocBuilder` along with the helpers provided here is used to setup the data and
//! then converted into a `Doc` as a final step.
//!
//! Note that there are also helpers like [yaml::hash_to_doc](crate::ig::yaml::hash_to_doc)
//! or [yaml::list_to_doc](crate::ig::yaml::list_to_doc) which directly transform a given
//! `Yaml` hash or list into a `Doc`.
use crate::ig::docs::Doc;
use crate::ig::node::Node;
use crate::ig::symbols::{Symbol, SymbolMap, SymbolTable};
use std::sync::Mutex;

/// Provides a builder to generate a `Doc`.
///
/// A doc an internally have either a list or a map as its root node. Therefore either
/// `build_object()`or `build_list()` has to be called to create the resulting `Doc`.
///
/// # Examples
///
/// Creating a list based `Doc`:
/// ```
/// # use jupiter::ig::builder::{DocBuilder, ListBuilder};
/// let builder = DocBuilder::new();
/// let mut list_builder = builder.list();
/// list_builder.append_int(1);
/// list_builder.append_int(2);
/// list_builder.append_int(3);
///
/// let doc = builder.build_list(list_builder);
/// assert_eq!(doc.root().at(1).as_int().unwrap(), 2);
/// ```
///
/// Creating a map based `Doc`:
/// ```
/// # use jupiter::ig::builder::DocBuilder;
/// let builder = DocBuilder::new();
/// let mut obj_builder = builder.obj();
/// obj_builder.put_int("Test", 1);
/// obj_builder.put_int("Foo", 2);
///
/// let doc = builder.build_object(obj_builder);
/// assert_eq!(doc.root().query("Test").as_int().unwrap(), 1);
/// assert_eq!(doc.root().query("Foo").as_int().unwrap(), 2);
/// ```
///
#[derive(Default)]
pub struct DocBuilder {
    symbols: Mutex<SymbolTable>,
}

impl DocBuilder {
    /// Creates a new builder instance.
    pub fn new() -> Self {
        DocBuilder {
            symbols: Mutex::new(SymbolTable::new()),
        }
    }

    /// Resolves the given name into a `Symbol` for repeated insertions.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    pub fn resolve(&self, symbol: impl AsRef<str>) -> anyhow::Result<Symbol> {
        let mut guard = self.symbols.lock().unwrap();
        guard.find_or_create(symbol)
    }

    /// Creates a new object to be used as either the root object or a child object within
    /// the Doc being built.
    ///
    /// We need a factory function here, so that we can access the shared symbol table and therefore
    /// provide convenience methods like **put_string**.
    pub fn obj(&self) -> ObjectBuilder {
        ObjectBuilder {
            doc_builder: &self,
            map: SymbolMap::new(),
        }
    }

    /// Creates a new list to be used as either the root list or a child list within the Doc being
    /// built.
    ///
    /// Note that technically there is currently no reason to use a factory function here. However,
    /// to be future proof and to also be symmetrical to **obj()**, we still provide this as the
    /// default way to obtain a **ListBuilder**.
    pub fn list(&self) -> ListBuilder {
        ListBuilder { list: Vec::new() }
    }

    /// Turns the builder into a `Doc` which root element is an object.
    pub fn build_object(&self, obj: ObjectBuilder) -> Doc {
        Doc::new(self.symbols.lock().unwrap().clone(), Node::Object(obj.map))
    }

    /// Turns the builder into a `Doc` which root element is a list.
    pub fn build_list(&self, list: ListBuilder) -> Doc {
        Doc::new(self.symbols.lock().unwrap().clone(), Node::List(list.list))
    }
}

/// Builds an inner object or map within a `Doc` or another element.
///
/// A builder can either be obtained via [DocBuilder::build_object](DocBuilder::build_object).
pub struct ObjectBuilder<'a> {
    doc_builder: &'a DocBuilder,
    map: SymbolMap<Node>,
}

impl<'a> ObjectBuilder<'a> {
    /// Places an integer value within the object being built.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let mut builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.put_int("Test", 42).unwrap();
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_int().unwrap(), 42);
    /// ```
    pub fn put_int(&mut self, key: impl AsRef<str>, value: i64) -> anyhow::Result<()> {
        let symbol = self.doc_builder.resolve(key)?;
        self.insert_int(symbol, value);
        Ok(())
    }

    /// Places an integer value within the object being built using a `Symbol`which has been looked
    /// up previously.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let mut builder = DocBuilder::new();
    /// let key = builder.resolve("Test").unwrap();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.insert_int(key, 42);
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_int().unwrap(), 42);
    /// ```
    pub fn insert_int(&mut self, key: Symbol, value: i64) {
        self.map.put(key, Node::Integer(value));
    }

    /// Places a string value within the object being built.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.put_string("Test", "Foo").unwrap();
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_str().unwrap(), "Foo");
    /// ```
    pub fn put_string(
        &mut self,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> anyhow::Result<()> {
        let symbol = self.doc_builder.resolve(key)?;
        self.insert_string(symbol, value);

        Ok(())
    }

    /// Places a string value within the object being built using a `Symbol`which has been looked
    /// up previously.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let key = builder.resolve("Test").unwrap();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.insert_string(key, "Foo");
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_str().unwrap(), "Foo");
    /// ```
    pub fn insert_string(&mut self, key: Symbol, value: impl AsRef<str>) {
        self.map.put(key, Node::from(value.as_ref()));
    }

    /// Places a bool value within the object being built.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.put_bool("Test", true).unwrap();
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_bool(), true);
    /// ```
    pub fn put_bool(&mut self, key: impl AsRef<str>, value: bool) -> anyhow::Result<()> {
        let symbol = self.doc_builder.resolve(key)?;
        self.insert_bool(symbol, value);

        Ok(())
    }

    /// Places bool value within the object being built using a `Symbol`which has been looked
    /// up previously.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let key = builder.resolve("Test").unwrap();
    /// let mut obj_builder = builder.obj();
    ///
    /// obj_builder.insert_bool(key, true);
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").as_bool(), true);
    /// ```
    pub fn insert_bool(&mut self, key: Symbol, value: bool) {
        self.map.put(key, Node::Boolean(value));
    }

    /// Places a list value within the object being built. Returns the builder used to populate
    /// the list.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    ///
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(1);
    /// obj_builder.put_list("Test", list_builder).unwrap();
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").at(0).as_int().unwrap(), 1);
    /// ```
    pub fn put_list(&mut self, key: impl AsRef<str>, list: ListBuilder) -> anyhow::Result<()> {
        let symbol = self.doc_builder.resolve(key)?;
        self.insert_list(symbol, list);
        Ok(())
    }

    /// Places a list value within the object being built using a `Symbol`which has been looked
    /// up previously. Returns the builder used to populate the list.   
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let  builder = DocBuilder::new();
    /// let key = builder.resolve("Test").unwrap();
    /// let mut obj_builder = builder.obj();
    ///
    /// let mut list_builder = builder.list();
    /// list_builder.append_int(1);
    /// obj_builder.insert_list(key, list_builder);
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test").at(0).as_int().unwrap(), 1);
    /// ```
    pub fn insert_list(&mut self, key: Symbol, list: ListBuilder) {
        self.map.put(key, Node::List(list.list));
    }

    /// Places an inner object within the object being built. Returns the builder used to populate
    /// the list.
    ///
    /// # Errors
    /// If the internal symbol table overflows, an error is returned.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut obj_builder = builder.obj();
    ///
    /// let mut inner_obj_builder = builder.obj();
    /// inner_obj_builder.put_string("Foo", "Bar").unwrap();
    /// obj_builder.put_object("Test", inner_obj_builder).unwrap();
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test.Foo").as_str().unwrap(), "Bar");
    /// ```
    pub fn put_object(&mut self, key: impl AsRef<str>, obj: ObjectBuilder) -> anyhow::Result<()> {
        let symbol = self.doc_builder.resolve(key)?;
        self.insert_object(symbol, obj);
        Ok(())
    }

    /// Places an inner object within the object being built using a `Symbol`which has been looked
    /// up previously. Returns the builder used to populate the list.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let key = builder.resolve("Test").unwrap();
    /// let mut obj_builder = builder.obj();
    ///
    /// let mut inner_obj_builder = builder.obj();
    /// inner_obj_builder.put_string("Foo", "Bar").unwrap();
    /// obj_builder.insert_object(key, inner_obj_builder);
    ///
    /// let doc = builder.build_object(obj_builder);
    /// assert_eq!(doc.root().query("Test.Foo").as_str().unwrap(), "Bar");
    /// ```
    pub fn insert_object(&mut self, key: Symbol, obj: ObjectBuilder) {
        self.map.put(key, Node::Object(obj.map));
    }
}

/// Builds an inner list within a `Doc` or another element.
///
/// A builder can be obtained via [DocBuilder::root_list_builder](DocBuilder::build_list).
pub struct ListBuilder {
    list: Vec<Node>,
}

impl ListBuilder {
    /// Appends an integer value to the list being built.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    ///
    /// list_builder.append_int(42);
    ///
    /// let doc = builder.build_list(list_builder);
    /// assert_eq!(doc.root().at(0).as_int().unwrap(), 42);
    /// ```
    pub fn append_int(&mut self, value: i64) {
        self.list.push(Node::Integer(value));
    }

    /// Appends a string to the list being built.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    ///
    /// list_builder.append_string("Test");
    ///
    /// let doc = builder.build_list(list_builder);
    /// assert_eq!(doc.root().at(0).as_str().unwrap(), "Test");
    /// ```
    pub fn append_string(&mut self, value: impl AsRef<str>) {
        self.list.push(Node::from(value.as_ref()));
    }

    /// Appends a bool value to the list being built.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    ///
    /// list_builder.append_bool(true);
    ///
    /// let doc = builder.build_list(list_builder);
    /// assert_eq!(doc.root().at(0).as_bool(), true);    
    /// ```
    pub fn append_bool(&mut self, value: bool) {
        self.list.push(Node::Boolean(value));
    }

    /// Appends a child-list to the list being built. Returns the builder used to populate the list.
    ///
    /// Note that this will not join two lists but rather construct a list as child element and
    /// append further items to this child list.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    ///
    /// let mut child_list_builder = builder.list();
    /// child_list_builder.append_int(42);
    /// list_builder.append_list(child_list_builder);
    ///
    /// let doc = builder.build_list(list_builder);
    /// assert_eq!(doc.root().len(), 1);    
    /// assert_eq!(doc.root().at(0).len(), 1);    
    /// assert_eq!(doc.root().at(0).at(0).as_int().unwrap(), 42);
    /// ```
    pub fn append_list(&mut self, list: ListBuilder) {
        self.list.push(Node::List(list.list));
    }

    /// Appends a child object to the list being built. Returns the builder used to populate the
    /// object.
    ///
    /// # Example
    /// ```
    /// # use jupiter::ig::builder::DocBuilder;
    /// let builder = DocBuilder::new();
    /// let mut list_builder = builder.list();
    ///
    /// let mut obj_builder = builder.obj();
    /// obj_builder.put_string("Foo", "Bar").unwrap();
    /// list_builder.append_object(obj_builder);
    ///
    /// let doc = builder.build_list(list_builder);
    /// assert_eq!(doc.root().len(), 1);    
    /// assert_eq!(doc.root().at(0).query("Foo").as_str().unwrap(), "Bar");
    /// ```
    pub fn append_object(&mut self, obj: ObjectBuilder) {
        self.list.push(Node::Object(obj.map));
    }
}
