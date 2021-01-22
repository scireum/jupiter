//! Permits to load YAML data into a [Doc](crate::ig::docs::Doc).
//!
//! Wraps the given YAML data and returns it as **Doc**.
//!
//! # Examples
//!
//! CSV into a doc:
//! ```
//! # use csv::ReaderBuilder;
//! # use jupiter::ig::csv::csv_to_doc;
//! let input = "F1;F2;F3\r\nA;X;1\r\nB;Y;2";
//! let reader = ReaderBuilder::new()
//!             .delimiter(b';')
//!             .has_headers(false)
//!             .from_reader(input.as_bytes());
//! let doc = csv_to_doc(reader).unwrap();
//!
//! assert_eq!(doc.root().at(0).query("F1").as_str().unwrap(), "A");
//! assert_eq!(doc.root().at(0).query("F2").as_str().unwrap(), "X");
//! assert_eq!(doc.root().at(0).query("F3").as_str().unwrap(), "1");
//! assert_eq!(doc.root().at(1).query("F1").as_str().unwrap(), "B");
//! assert_eq!(doc.root().at(1).query("F2").as_str().unwrap(), "Y");
//! assert_eq!(doc.root().at(1).query("F3").as_str().unwrap(), "2");
//! ```
use crate::ig::builder::DocBuilder;
use crate::ig::docs::Doc;
use crate::ig::symbols::Symbol;
use csv::{Reader, StringRecordsIter};

/// Transforms the given CSV data into a doc which is a list of objects.
pub fn csv_to_doc<R>(mut reader: Reader<R>) -> anyhow::Result<Doc>
where
    R: std::io::Read,
{
    let doc_builder = DocBuilder::new();
    let mut list_builder = doc_builder.list();

    let mut record_iter = reader.records();
    let headers = read_headers(&mut record_iter, &doc_builder)?;
    let mut line = 2;
    for record in reader.records() {
        match record {
            Ok(record) => {
                let mut obj_builder = doc_builder.obj();
                for (header, value) in headers.iter().zip(record.iter()) {
                    obj_builder.insert_string(*header, value);
                }

                list_builder.append_object(obj_builder);
            }
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "Failed to parse CSV in line {}: {}",
                    line,
                    error
                ));
            }
        }
        line += 1;
    }

    Ok(doc_builder.build_list(list_builder))
}

fn read_headers<R>(
    record_iter: &mut StringRecordsIter<R>,
    doc_builder: &DocBuilder,
) -> anyhow::Result<Vec<Symbol>>
where
    R: std::io::Read,
{
    if let Some(Ok(headers)) = record_iter.next() {
        let mut resolved_headers = Vec::new();

        for header in headers.iter() {
            resolved_headers.push(doc_builder.resolve(header)?);
        }

        Ok(resolved_headers)
    } else {
        Err(anyhow::anyhow!("Failed to read headers from CSV!"))
    }
}
