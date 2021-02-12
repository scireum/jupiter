//! A table wraps a [crate::ig::docs::Doc] together with an [Trie] as index to support InfoGraphDB queries.
//!
//! Note that a table consumes a given doc to ensure it is immutable, as the index is built
//! up on creation of the table and then kept as long as the table lives.
use crate::idb::trie::{PrefixIter, Trie};
use crate::ig::docs::{Doc, Element, Query};
use crate::ig::symbols::Symbol;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Represents an entry in the lookup index.
///
/// Basically an entry is either exact (matching the whole field contents) or loose (matching
/// a word / token within a field while comparing case-insensitive). Internally each entry
/// contains the field symbol it points to along with the position (row number) for which is
/// was created.
pub struct IndexEntry {
    field: Symbol,
    row: usize,
    exact: bool,
}

/// Specifies the minimal token length for seach tokens to be indexed.
const MIN_TOKEN_LENGTH: usize = 3;

/// Describes the type of index being built for a field.
pub enum IndexType {
    /// A lookup index permits to quickly find all rows which contain the exact field value
    /// as given in a query.
    LookupIndex(String),

    /// A fulltext index permits to search case-insensitive and also contains words or other
    /// sub tokens within the text.
    FulltextIndex(String),
}

impl IndexType {
    /// Creates a new `LookupIndex` for the given field or path.
    pub fn lookup(path: &str) -> Self {
        IndexType::LookupIndex(path.to_string())
    }

    /// Creates a new `FulltextIndex` for the given field or path.
    pub fn fulltext(path: &str) -> Self {
        IndexType::FulltextIndex(path.to_string())
    }

    /// Returns the field name or path on which this index operates.
    pub fn field_name(&self) -> &str {
        match self {
            IndexType::LookupIndex(name) => name.as_ref(),
            IndexType::FulltextIndex(name) => name.as_ref(),
        }
    }
}

/// Note that when comparing index entries, we only check if their field and row matches no matter
/// if these are exact or loose.
///
/// The idea is that when building the index, we first record all exact matches and then compute
/// the loose ones. Still we want to de-duplicate the index entries, so that for a single match
/// (as in path within the TRIE) a document only occurs once. We do not need two entries for the
/// same word and field (one being loose and one being exact) as all exact matches are also
/// considered valid when performing a loose (`IDB.SEARCH`) search.
impl PartialEq for IndexEntry {
    fn eq(&self, other: &Self) -> bool {
        self.field == other.field && self.row == other.row
    }
}

/// Wraps a [crate::ig::docs::Doc] and permits to execute fast lookups backed by a [Trie].
///
/// Note that this is the work horse of the InfoGraphDB.
pub struct Table {
    doc: Doc,
    index: Trie<IndexEntry>,
    default_lang_query: Query,
    known_indices: fnv::FnvHashSet<Symbol>,
    queries: AtomicUsize,
    scan_queries: AtomicUsize,
    scans: AtomicUsize,
}

/// Represents the result when parsing a list of fields to search in.
enum SearchFields {
    // Search in all fields. Note that this will only search in indexed fields.
    All,

    // Search in the given set of fields, which are all backed by an index.
    IndexLookup(fnv::FnvHashSet<Symbol>),

    // Perform a table scan for the given set of fields.
    Scan(Vec<Query>),
}

/// Contains the (fake) language code used to retrieve a fallback or placeholder value if no
/// proper translation is present.
const DEFAULT_LANG: &str = "xx";

impl Table {
    /// Creates a new table from the given doc while building a lookup index for the given paths.
    ///
    /// Note that each value in `indices` can be a path like `field.inner.other`.
    pub fn new(mut doc: Doc, indices: Vec<IndexType>) -> anyhow::Result<Self> {
        let mut trie = Trie::new();
        let known_indices = Table::build_indices(&mut doc, indices, &mut trie)?;
        let default_lang_query = doc.compile(DEFAULT_LANG).clone();

        Ok(Table {
            doc,
            index: trie,
            default_lang_query,
            known_indices,
            queries: AtomicUsize::new(0),
            scan_queries: AtomicUsize::new(0),
            scans: AtomicUsize::new(0),
        })
    }

    /// Compiles the list of indices into a set of symbols and also fills the appropriate data
    /// into the index TRIE.
    fn build_indices(
        doc: &mut Doc,
        indices: Vec<IndexType>,
        trie: &mut Trie<IndexEntry>,
    ) -> anyhow::Result<fnv::FnvHashSet<Symbol>> {
        let mut known_indices = fnv::FnvHashSet::default();

        for index_type in indices {
            let query = doc.compile(index_type.field_name());
            let field_symbol = doc.symbols_mut().find_or_create(index_type.field_name())?;
            let _ = known_indices.insert(field_symbol);

            for (row, element) in doc.root().iter().enumerate() {
                // Fetch the raw value..
                let child = query.execute(element);

                let mut dedup_set = fnv::FnvHashSet::default();

                // Extract all exact matches and insert them into the index...
                Table::search_values(child, |value| {
                    if !dedup_set.contains(value) {
                        trie.insert_unique(
                            value,
                            IndexEntry {
                                field: field_symbol,
                                row,
                                exact: true,
                            },
                        );
                        let _ = dedup_set.insert(value.to_string());
                    }
                });

                if let IndexType::FulltextIndex(_) = index_type {
                    // Extract and tokenize all loose matches and insert them into the index.
                    // Note that this has to be performed in a 2nd step, as a value can be a list or
                    // even a nested object, where we always need to first store all exact values
                    // and then collect the loose matches (which are not already present as exact
                    // match).
                    // An example would be a list which contains the entries "hello world" and "hello".
                    // As the tokenized version of "hello world" would also emit "hello" (as loose
                    // term) we first want to collect it as exact term.
                    Table::search_values(child, |value| {
                        Table::tokenize(value, |token| {
                            if !dedup_set.contains(token) {
                                trie.insert_unique(
                                    token,
                                    IndexEntry {
                                        field: field_symbol,
                                        row,
                                        exact: false,
                                    },
                                );
                                let _ = dedup_set.insert(token.to_string());
                            }
                        });
                    });
                }
            }
        }

        Ok(known_indices)
    }

    /// Extracts all fields values to be inserted into the index.
    ///
    /// Next to the trivial case of a simple string or int, this also iterates over inner lists
    /// or an object which contains values or an inner list. Note that we only support this single
    /// level (to catch translation maps) but do not index complete "subtrees" as this seems quite
    /// an overkill.
    fn search_values<C>(element: Element, mut callback: C)
    where
        C: FnMut(&str),
    {
        if element.is_list() {
            for child in element.iter() {
                callback(child.to_str().as_ref())
            }
        } else if element.is_object() {
            for (_, child) in element.entries() {
                if child.is_list() {
                    for child in element.iter() {
                        callback(child.to_str().as_ref())
                    }
                } else {
                    callback(child.to_str().as_ref())
                }
            }
        } else {
            callback(element.to_str().as_ref())
        }
    }

    /// Tokenizes a string value by splitting at all whitespaces or occurrences of
    /// `split_pattern`. Each inner token is trimmed (removing both, whitespaces as well
    /// as non-alphanumeric characters) and turned to lower case before being submitted to the
    /// callback.
    fn tokenize<C>(value: &str, mut callback: C)
    where
        C: FnMut(&str),
    {
        let effective_value = value.to_lowercase();
        callback(effective_value.as_str());

        for token in effective_value.split(Table::split_pattern) {
            let effective_token = token.trim_matches(|ch: char| !ch.is_alphanumeric());
            if effective_token.len() >= MIN_TOKEN_LENGTH {
                callback(effective_token);
                for inner_token in effective_token.split(|ch: char| !ch.is_alphanumeric()) {
                    if inner_token.len() >= MIN_TOKEN_LENGTH {
                        callback(inner_token);
                    }
                }
            }
        }
    }

    /// Provides a pattern matcher which identifies characters to split tokens at.
    ///
    /// We split at whitespace and all "special characters" except:
    /// `'-' | '_' | '.' | ':' | '/' | '\\' | '@'`.
    fn split_pattern(ch: char) -> bool {
        if ch.is_alphanumeric() {
            false
        } else {
            !matches!(ch, '-' | '_' | '.' | ':' | '/' | '\\' | '@')
        }
    }

    /// Parses a field name or comma separated list of field names or paths and compiles
    /// them into a `SearchFields` specifier.
    fn determine_search_fields(&self, fields_string: &str) -> SearchFields {
        if fields_string == "*" {
            SearchFields::All
        } else {
            let mut fields = fnv::FnvHashSet::default();
            for field in fields_string.split(',') {
                if let Some(field_symbol) = self.doc.symbols().resolve(field) {
                    if self.known_indices.contains(&field_symbol) {
                        let _ = fields.insert(field_symbol);
                    } else {
                        // This field or path is not indexed -> abort to a list of scan queries...
                        return self.compile_scan_fields(fields_string);
                    }
                } else {
                    // We can`t even resolve the field or path into a symbol. Therefore this field
                    // cannot be indexed and thus we need to resort to a scan query...
                    return self.compile_scan_fields(fields_string);
                }
            }

            SearchFields::IndexLookup(fields)
        }
    }

    /// Parses the list of field or paths into a list of queries to execute during the table scan.
    fn compile_scan_fields(&self, fields_string: &str) -> SearchFields {
        let mut fields = Vec::new();
        for field in fields_string.split(',') {
            fields.push(self.doc.compile(field));
        }

        SearchFields::Scan(fields)
    }

    /// Compiles the given field or path name into a fast lookup query which extracts the matching
    /// value from a given doc.
    pub fn compile(&self, query: impl AsRef<str>) -> Query {
        self.doc.compile(query.as_ref())
    }

    /// Executes a query by searching for the given value in the given list of fields
    /// (see `determine_search_fields`).
    ///
    /// If `exact` is true, the given value must match the field contents, otherwise it can also
    /// be a token or even the prefix of a token within the exact or loose index values of the
    /// selected fields.
    ///
    /// Returns an iterator which yields all matching docs or an error, if a search query is
    /// executed against non-indexed fields.
    pub fn query<'a>(
        &'a self,
        fields: &'a str,
        value: &'a str,
        exact: bool,
    ) -> anyhow::Result<TableIter<'a>> {
        self.queries
            .store(self.queries.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

        match self.determine_search_fields(fields) {
            SearchFields::All => {
                if exact {
                    Ok(TableIter::ExactIndexQuery(
                        &self.doc,
                        self.index.query_values(value),
                        None,
                        0,
                    ))
                } else {
                    Ok(TableIter::PrefixIndexQuery(
                        &self.doc,
                        self.index.prefix_query(value.to_lowercase().as_str()),
                        fnv::FnvHashSet::default(),
                        None,
                    ))
                }
            }
            SearchFields::IndexLookup(fields) => {
                if exact {
                    Ok(TableIter::ExactIndexQuery(
                        &self.doc,
                        self.index.query_values(value),
                        Some(fields),
                        0,
                    ))
                } else {
                    Ok(TableIter::PrefixIndexQuery(
                        &self.doc,
                        self.index.prefix_query(value.to_lowercase().as_str()),
                        fnv::FnvHashSet::default(),
                        Some(fields),
                    ))
                }
            }

            SearchFields::Scan(fields) => {
                if !exact {
                    Err(anyhow::anyhow!(
                        "Cannot use a search query on non-indexed fields!"
                    ))
                } else {
                    self.scan_queries.store(
                        self.scan_queries.load(Ordering::Relaxed) + 1,
                        Ordering::Relaxed,
                    );

                    Ok(TableIter::ScanQuery(&self.doc, fields, value, 0))
                }
            }
        }
    }

    /// Performs a table scan which simply iterates over all rows of a table.
    pub fn table_scan(&self) -> TableIter {
        self.scans
            .store(self.scans.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

        TableIter::TableScan(&self.doc, 0)
    }

    /// Returns the number of rows in the table.
    pub fn len(&self) -> usize {
        self.doc.root().len()
    }

    /// Determines if this table is empty.
    pub fn is_empty(&self) -> bool {
        self.doc.root().is_empty()
    }

    /// Returns the number of queries which have been executed against this table.
    pub fn num_queries(&self) -> usize {
        self.queries.load(Ordering::Relaxed)
    }

    /// Returns the number of queries which have resorted to a table scan in order to execute
    /// properly.
    pub fn num_scan_queries(&self) -> usize {
        self.scan_queries.load(Ordering::Relaxed)
    }

    /// Returns the number of deliberate table scans.
    pub fn num_scans(&self) -> usize {
        self.scans.load(Ordering::Relaxed)
    }

    /// Estimates the allocated memory for both, the table data and its index.
    ///
    /// The returned value is in bytes.
    pub fn allocated_memory(&self) -> usize {
        self.doc.allocated_size() + self.index.allocated_size()
    }

    /// Returns the selector or query for the fallback/default language ("xx").
    pub fn default_lang_query(&self) -> &Query {
        &self.default_lang_query
    }
}

/// Represents the various strategies used to iterate over contents or query results within a table.
pub enum TableIter<'a> {
    /// Represents an iterator which is based on a direct index hit.
    ExactIndexQuery(
        &'a Doc,
        Option<&'a Vec<IndexEntry>>,
        Option<fnv::FnvHashSet<Symbol>>,
        usize,
    ),
    /// Represents an iterator which is based on a prefix hit within the index.
    PrefixIndexQuery(
        &'a Doc,
        PrefixIter<'a, IndexEntry>,
        fnv::FnvHashSet<usize>,
        Option<fnv::FnvHashSet<Symbol>>,
    ),
    /// Represents an iterator which scans over all index items to check for matches.
    ScanQuery(&'a Doc, Vec<Query>, &'a str, usize),
    /// Represents an iterator which represents all entries in the index.
    TableScan(&'a Doc, usize),
}

impl<'a> Iterator for TableIter<'a> {
    type Item = Element<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            // For an exact query, we operate on the underlying index vector (which is per matching
            // token. We therefore have to check and skip over tokens of non-matching fields...
            TableIter::ExactIndexQuery(doc, hits, fields, next_hit) => {
                if let Some(hits) = hits {
                    while *next_hit < hits.len() {
                        let hit = &hits[*next_hit];
                        *next_hit += 1;
                        if hit.exact
                            && (fields.is_none() || fields.as_ref().unwrap().contains(&hit.field))
                        {
                            return Some(doc.root().at(hit.row));
                        }
                    }
                }

                None
            }
            // For prefix matches, we can simply operate on the underlying PrefixIter of the Trie
            // and also filter out matches, which do not belong to our selected fields...
            TableIter::PrefixIndexQuery(doc, iter, de_dup, fields) => {
                for hit in iter {
                    if (fields.is_none() || fields.as_ref().unwrap().contains(&hit.field))
                        && !de_dup.contains(&hit.row)
                    {
                        let _ = de_dup.insert(hit.row);
                        return Some(doc.root().at(hit.row));
                    }
                }

                None
            }
            // A table scan is a bit more involved. Naturally the documents to check are easily
            // identified (we simply iterate through all). However, we now have to perform the whole
            // value extraction process which normally happens during indexing...
            TableIter::ScanQuery(doc, fields, value, next_hit) => {
                while *next_hit < doc.root().len() {
                    let row = doc.root().at(*next_hit);
                    *next_hit += 1;

                    if TableIter::has_match(row, fields, value) {
                        return Some(row);
                    }
                }

                None
            }
            // A raw table scan is quite simple - iterate over each row, each row is a match...
            TableIter::TableScan(doc, next_hit) => {
                if *next_hit < doc.root().len() {
                    let row = doc.root().at(*next_hit);
                    *next_hit += 1;
                    Some(row)
                } else {
                    None
                }
            }
        }
    }
}

impl<'a> TableIter<'a> {
    /// Checks if for the given row any of the given fields or paths contains the given value.
    fn has_match(row: Element, fields: &[Query], value: &str) -> bool {
        let mut matching = false;
        for field in fields.iter() {
            let field_value = field.execute(row);
            Table::search_values(field_value, |search_val| {
                if search_val == value {
                    matching = true
                }
            });
            if matching {
                break;
            }
        }
        matching
    }
}

#[cfg(test)]
mod tests {
    use crate::idb::table::{IndexType, Table};
    use crate::ig::docs::Doc;
    use crate::ig::yaml::list_to_doc;
    use yaml_rust::YamlLoader;

    #[test]
    fn table_test() {
        // Load an extremely elaborate example dataset...
        let dataset = create_example_dataset();
        let table = Table::new(
            dataset,
            vec![
                IndexType::lookup("code"),
                IndexType::lookup("iso.two"),
                IndexType::fulltext("name"),
            ],
        )
        .unwrap();

        assert_eq!(table.len(), 2);
        assert_eq!(table.is_empty(), false);

        // Check that exact query work...
        let row = table.query("code", "D", true).unwrap().next().unwrap();
        assert_eq!(row.query("name.de").as_str().unwrap(), "Deutschland");

        // Check that inexact queries work...
        let row = table.query("name", "de", false).unwrap().next().unwrap();
        assert_eq!(row.query("name.de").as_str().unwrap(), "Deutschland");

        assert_eq!(
            table.query("name", "xxx", true).unwrap().next().is_none(),
            true
        );

        // Cannot perform inexact search in non-indexed field...
        assert_eq!(table.query("iso.three", "xxx", false).is_err(), true);

        // but can perform an exact search in non-indexed field...
        let row = table
            .query("iso.three", "aut", true)
            .unwrap()
            .next()
            .unwrap();
        assert_eq!(row.query("name.en").as_str().unwrap(), "Austria");

        // Enforce a simple table scan...
        assert_eq!(table.table_scan().count(), 2);

        // Ensure that the metrics are correctly updated...
        assert_eq!(table.num_queries(), 5);
        assert_eq!(table.num_scan_queries(), 1);
        assert_eq!(table.num_scans(), 1);
    }

    fn create_example_dataset() -> Doc {
        let input = r#"
code: "D"
iso:
  two: "de"
  three: "deu"
name:
  de: "Deutschland"
  en: "Germany"
---
code: "A"
iso:
  two: "at"
  three: "aut"
name:
  de: "Österreich"
  en: "Austria"
        "#;

        let rows = YamlLoader::load_from_str(input).unwrap();
        list_to_doc(rows.as_slice()).unwrap()
    }
}
