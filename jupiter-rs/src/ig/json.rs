//! Permits to load JSON data into a [Doc](crate::ig::docs::Doc).
//!
//! Wraps the given JSON data and returns it as **Doc**.
//!
//! # Examples
//!
//! Loading an object into a doc:
//! ```
//! # use jupiter::ig::json::object_to_doc;
//! # use serde_json::Value;
//! let input = r#"{
//!     "a_string": "Test",
//!     "a_map": { "inner_key": 42 },
//!     "a_list": [ 1, 2, { "test": "Plain String" }],
//!     "a_bool": true    
//! }    
//! "#;
//!
//! let value: Value = serde_json::from_str(input).unwrap();
//! let doc = object_to_doc(value.as_object().unwrap()).unwrap();
//!
//! assert_eq!(
//!     value["a_string"].as_str(),
//!     doc.root().query("a_string").as_str()
//! );
//! assert_eq!(
//!     value["a_list"][0].as_i64(),
//!     doc.root().query("a_list").at(0).as_int()
//! );
//! assert_eq!(
//!     value["a_list"][1].as_i64(),
//!     doc.root().query("a_list").at(1).as_int()
//! );
//! assert_eq!(
//!     value["a_bool"].as_bool().unwrap(),
//!     doc.root().query("a_bool").as_bool()
//! );
//! assert_eq!(
//!     value["a_map"]["inner_key"].as_str(),
//!     doc.root().query("a_map.inner_key").as_str()
//! );
//! ```
//!
//! Loading a list into a doc:
//! ```
//! # use jupiter::ig::json::list_to_doc;
//! # use serde_json::Value;
//! let input = r#"[ { "a_string": "Test" }, { "a_string": "Test1" }, { "a_string": "Test2" }, 42 ]"#;
//!
//! let value: Value = serde_json::from_str(input).unwrap();
//! let doc = list_to_doc(value.as_array().unwrap()).unwrap();
//!
//! assert_eq!(doc.root().len(), 4);
//! assert_eq!(value[0]["a_string"].as_str(), doc.root().at(0).query("a_string").as_str());
//! assert_eq!(value[1]["a_string"].as_str(), doc.root().at(1).query("a_string").as_str());
//! assert_eq!(value[2]["a_string"].as_str(), doc.root().at(2).query("a_string").as_str());
//! assert_eq!(value[3].as_i64(), doc.root().at(3).as_int());
//! ```
//!

use crate::ig::builder::{DocBuilder, ListBuilder, ObjectBuilder};
use crate::ig::docs::Doc;
use anyhow::Context;
use serde_json::{Map, Value};

/// Transforms a JSON object into a [Doc](crate::ig::docs::Doc).
///
/// The generated **Doc** will have an object as its root node.
///
/// # Errors
/// This will return an error if we're running out of symbols (if there are more than 2^31-1
/// distinct keys in the hash or one of its children).
///
/// # Example
///
/// ```
/// # use jupiter::ig::json::object_to_doc;
/// # use serde_json::Value;
/// let input = r#"{
///     "a_string": "Test",
///     "a_map": { "inner_key": 42 },
///     "a_list": [ 1, 2, { "test": "Plain String" }],
///     "a_bool": true    
/// }    
/// "#;
///
/// let value: Value = serde_json::from_str(input).unwrap();
/// let doc = object_to_doc(value.as_object().unwrap()).unwrap();
///
/// assert_eq!(doc.root().query("a_string").as_str().unwrap(), "Test");
/// ```
pub fn object_to_doc(object: &Map<String, Value>) -> anyhow::Result<Doc> {
    let doc_builder = DocBuilder::new();
    let mut obj_builder = doc_builder.obj();
    transform_object(&object, &doc_builder, &mut obj_builder)?;

    Ok(doc_builder.build_object(obj_builder))
}

/// Transforms the given list of JSON objects into a [Doc](crate::ig::docs::Doc).
///
/// The generated **Doc** will have an object as its root node.
///
/// # Errors
/// This will return an error if we're running out of symbols (if there are more than 2^31-1
/// distinct keys in the hash or one of its children).
///
/// # Example
///
/// ```
/// # use jupiter::ig::json::list_to_doc;
/// # use serde_json::Value;
/// let input = r#"[ { "a_string": "Test" }, { "a_string": "Test1" }, { "a_string": "Test2" }, 42 ]"#;
///
/// let value: Value = serde_json::from_str(input).unwrap();
/// let doc = list_to_doc(value.as_array().unwrap()).unwrap();
///
/// assert_eq!(doc.root().len(), 4);
/// assert_eq!(doc.root().at(1).query("a_string").as_str().unwrap(), "Test1");
/// ```
pub fn list_to_doc(list: &[Value]) -> anyhow::Result<Doc> {
    let doc_builder = DocBuilder::new();
    let mut list_builder = doc_builder.list();
    transform_list(list, &doc_builder, &mut list_builder)?;

    Ok(doc_builder.build_list(list_builder))
}

fn transform_list(
    list: &[Value],
    doc_builder: &DocBuilder,
    builder: &mut ListBuilder,
) -> anyhow::Result<()> {
    for value in list.iter() {
        match value {
            Value::Bool(value) => builder.append_bool(*value),
            Value::Number(value) => {
                builder.append_int(value.as_i64().context("Failed to convert number to i64")?)
            }
            Value::String(value) => builder.append_string(value),
            Value::Array(inner_list) => {
                let mut list_builder = doc_builder.list();
                transform_list(inner_list, doc_builder, &mut list_builder)?;
                builder.append_list(list_builder);
            }
            Value::Object(value) => {
                let mut obj_builder = doc_builder.obj();
                transform_object(value, doc_builder, &mut obj_builder)?;
                builder.append_object(obj_builder);
            }
            Value::Null => {}
        }
    }

    Ok(())
}

fn transform_object(
    hash: &Map<String, Value>,
    doc_builder: &DocBuilder,
    builder: &mut ObjectBuilder,
) -> anyhow::Result<()> {
    for (key, value) in hash {
        match value {
            Value::Bool(value) => builder.put_bool(key, *value)?,
            Value::Number(value) => builder.put_int(
                key,
                value.as_i64().context("Failed to convert number to i64")?,
            )?,
            Value::String(value) => builder.put_string(key, value)?,
            Value::Array(inner_list) => {
                let mut list_builder = doc_builder.list();
                transform_list(inner_list, doc_builder, &mut list_builder)?;
                builder.put_list(key, list_builder)?;
            }
            Value::Object(value) => {
                let mut obj_builder = doc_builder.obj();
                transform_object(value, doc_builder, &mut obj_builder)?;
                builder.put_object(key, obj_builder)?;
            }
            Value::Null => {}
        }
    }

    Ok(())
}
