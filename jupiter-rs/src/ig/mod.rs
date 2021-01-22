//! Provides a compact and highly efficient representation of in-memory databases.
//!
//! `infograph` permits to generate or load structured data, which can then be queried in various
//! ways. This could be anything from simple code lists (name value pair) to complex graphs of
//! master data like nested lookup tables translated into several languages.
//!
//! The work horse is the [Doc](docs::Doc) struct which contains the root **Node** and
//! the [SymbolTable](symbols::SymbolTable). Nodes as the internal representation of the supported
//! data types (strings, ints, bools, lists and maps). The external interface used to access them
//! is [Element](docs::Element) which carries the symbol table along.
//!
//! Symbols are simply numbers (`i32`) which are managed by a [SymbolTable](symbols::SymbolTable).
//! The reason is that many documents contain lots of inner objects (maps) which all share the same
//! keys. To optimize memory consumption and also to improve access times, these keys are only
//! put in them symbol table once and then referenced as [Symbol](symbols::Symbol).
//!
//! If a `Symbol` or a [Query](docs::Query) (which is a path of symbols to access an inner object)
//! is used several times, it can be compiled and then re-used, which further improves performance.
//!
//! For lists of objects a [Table](crate::idb::table::Table) can be used to further speed up querying objects
//! and data from it. A table permits to add indices for common queries and also keeps a cache
//! for both, pre-compiled queries and frequent lookups around.
//!
//! To create a **Doc** programmatically, a [DocBuilder](builder::DocBuilder) can be used.
//!
//! Also there are helpers to read common data formats:
//! * **YAML**: [hash_to_doc](yaml::hash_to_doc) or [list_to_doc](yaml::list_to_doc)
//! * **JSON**: [object_to_doc](json::object_to_doc) or [list_to_doc](json::list_to_doc)
//! * **XML**: [PullReader](xml::PullReader)
//!
//! # Performance
//! As stated above, `infograph` is built to serve queries against large in-memory data structures
//! which are read frequently and changed seldomly.
//!
//! To guarantee optimal performance and to also optimize memory consumption, three main techniques
//! are applied:
//!
//! 1) **Small string optimizations**: Strings which are rather short, are stored in place instead
//! of using a classic `String`. Note that if the value doesn't fit in place, we still only store
//! a pointer to a `[u8]` instead of a whole `Vec<u8>` which saves 8 bytes for the skipped
//! capacity field (which isn't required as the strings are immutable anyway).
//!
//! 2) **Compression via SymbolTable**: As the keys in objects are most probably repeated very often
//! we store the strings in a symbol table and only carry an `i32` along.Think if a list of 1000
//! object which all look like `{ Foo: 42, Bar: 24 }`. Instead of storing the strings repeatedly,
//! we have a symbol table: `{'Foo': 1, 'Bar': 2}` accompanied with a lookup table
//! `[ 'Foo', 'Bar' ]`. The object itself is stored as `{1: 42, 2: 24}`.
//!
//! 3) **Optimized object representation**: Objects are maps which map Symbols (`i32`) to
//! **nodes**. As many objects only contain a few entries, using a `HashMap` would be
//! quite an overkill (and slow). Therefore we use a `Vec<(Symbol, Node)>` with a small initial
//! capacity. This provides quite a compact memory layout which is very fast due to its efficient
//! usage of L1 cache. We also sort entries by their `Symbol` so that we can use a binary search
//! when looking up an entry.

mod node;
mod strings;

pub mod builder;
pub mod csv;
pub mod docs;
pub mod json;
pub mod symbols;
pub mod xml;
pub mod yaml;
