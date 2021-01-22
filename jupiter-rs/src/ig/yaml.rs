//! Permits to load YAML data into a [Doc](crate::ig::docs::Doc).
//!
//! Wraps the given YAML data and returns it as **Doc**.
//!
//! # Examples
//!
//! Loading an object into a doc:
//! ```
//! # use yaml_rust::YamlLoader;
//! # use jupiter::ig::yaml::hash_to_doc;
//! let input = "
//! a_string: 'Test'
//! a_map:
//!     inner_key: 42
//! a_list:
//!     - 1
//!     - 2
//!     - test: Plain String
//! a_bool: true        
//! ";
//!
//! let yaml = &YamlLoader::load_from_str(input).unwrap()[0];
//! let doc = hash_to_doc(yaml.as_hash().unwrap()).unwrap();
//!
//! assert_eq!(
//!     yaml["a_string"].as_str(),
//!     doc.root().query("a_string").as_str()
//! );
//! assert_eq!(
//!     yaml["a_list"][0].as_i64(),
//!     doc.root().query("a_list").at(0).as_int()
//! );
//! assert_eq!(
//!     yaml["a_list"][1].as_i64(),
//!     doc.root().query("a_list").at(1).as_int()
//! );
//! assert_eq!(
//!     yaml["a_bool"].as_bool().unwrap(),
//!     doc.root().query("a_bool").as_bool()
//! );
//! assert_eq!(
//!     yaml["a_map"]["inner_key"].as_str(),
//!     doc.root().query("a_map.inner_key").as_str()
//! );
//! ```
//!
//! Loading a list into a doc:
//! ```
//! # use yaml_rust::YamlLoader;
//! # use jupiter::ig::yaml::{hash_to_doc, list_to_doc};
//! let input = "
//! a_string: 'Test'
//! ---
//! a_string: 'Test1'
//! ---
//! a_string: 'Test2'
//! ---
//! 42
//! ";
//!
//! let yaml = &YamlLoader::load_from_str(input).unwrap();
//! let doc = list_to_doc(yaml).unwrap();
//!
//! assert_eq!(doc.root().len(), 4);
//! assert_eq!(yaml[0]["a_string"].as_str(), doc.root().at(0).query("a_string").as_str());
//! assert_eq!(yaml[1]["a_string"].as_str(), doc.root().at(1).query("a_string").as_str());
//! assert_eq!(yaml[2]["a_string"].as_str(), doc.root().at(2).query("a_string").as_str());
//! assert_eq!(yaml[3].as_i64(), doc.root().at(3).as_int());
//! ```
//!
use linked_hash_map::LinkedHashMap;
use yaml_rust::Yaml;

use crate::ig::builder::{DocBuilder, ListBuilder, ObjectBuilder};
use crate::ig::docs::Doc;

/// Transforms a YAML hash (object) into a [Doc](crate::ig::docs::Doc).
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
/// # use yaml_rust::YamlLoader;
/// # use jupiter::ig::yaml::hash_to_doc;
/// let input = "
/// a_string: 'Test'
/// a_map:
///     inner_key: 42
/// a_list:
///     - 1
///     - 2
///     - test: Plain String
/// a_bool: true        
/// ";
///
/// let yaml = &YamlLoader::load_from_str(input).unwrap()[0];
/// let doc = hash_to_doc(yaml.as_hash().unwrap()).unwrap();
///
/// assert_eq!(doc.root().query("a_string").as_str().unwrap(), "Test");
/// ```
pub fn hash_to_doc(hash: &LinkedHashMap<Yaml, Yaml>) -> anyhow::Result<Doc> {
    let doc_builder = DocBuilder::new();
    let mut obj_builder = doc_builder.obj();
    transform_hash(hash, &doc_builder, &mut obj_builder)?;

    Ok(doc_builder.build_object(obj_builder))
}

/// Transforms the given list of YAML objects into a [Doc](crate::ig::docs::Doc).
///
/// The generated **Doc** will have a list as its root node.
///
/// # Errors
/// This will return an error if we're running out of symbols (if there are more than 2^31-1
/// distinct keys in the hash or one of its children).
///
/// # Example
///
/// ```
/// # use yaml_rust::YamlLoader;
/// # use jupiter::ig::yaml::{hash_to_doc, list_to_doc};
/// let input = "
/// a_string: 'Test'
/// ---
/// a_string: 'Test1'
/// ---
/// a_string: 'Test2'
/// ---
/// 42
/// ---
/// 'text'
/// ---
/// true
/// ";
///
/// let yaml = &YamlLoader::load_from_str(input).unwrap();
/// let doc = list_to_doc(yaml).unwrap();
///
/// assert_eq!(doc.root().len(), 6);
/// assert_eq!(doc.root().at(1).query("a_string").as_str().unwrap(), "Test1");
/// assert_eq!(doc.root().at(3).as_int().unwrap(), 42);
/// assert_eq!(doc.root().at(4).as_str().unwrap(), "text");
/// assert_eq!(doc.root().at(5).as_bool(), true);
/// ```
pub fn list_to_doc(list: &[Yaml]) -> anyhow::Result<Doc> {
    let doc_builder = DocBuilder::new();
    let mut list_builder = doc_builder.list();
    transform_list(list, &doc_builder, &mut list_builder)?;

    Ok(doc_builder.build_list(list_builder))
}

fn transform_list(
    list: &[Yaml],
    doc_builder: &DocBuilder,
    builder: &mut ListBuilder,
) -> anyhow::Result<()> {
    for yaml in list.iter() {
        match yaml {
            Yaml::Hash(ref map) => {
                let mut obj_builder = doc_builder.obj();
                transform_hash(map, doc_builder, &mut obj_builder)?;
                builder.append_object(obj_builder);
            }
            Yaml::Boolean(value) => builder.append_bool(*value),
            Yaml::Integer(value) => builder.append_int(*value),
            Yaml::String(value) => builder.append_string(value),
            Yaml::Array(ref inner_list) => {
                let mut list_builder = doc_builder.list();
                transform_list(inner_list, doc_builder, &mut list_builder)?;
                builder.append_list(list_builder);
            }
            Yaml::Real(value) => builder.append_string(value),
            _ => (),
        }
    }

    Ok(())
}

fn transform_hash(
    hash: &LinkedHashMap<Yaml, Yaml>,
    doc_builder: &DocBuilder,
    builder: &mut ObjectBuilder,
) -> anyhow::Result<()> {
    for (key, value) in hash {
        if let Some(key) = key.as_str() {
            match value {
                Yaml::Hash(map) => {
                    let mut obj_builder = doc_builder.obj();
                    transform_hash(map, doc_builder, &mut obj_builder)?;
                    builder.put_object(key, obj_builder)?
                }
                Yaml::Boolean(value) => builder.put_bool(key, *value)?,
                Yaml::Integer(value) => builder.put_int(key, *value)?,
                Yaml::String(value) => builder.put_string(key, value)?,
                Yaml::Array(inner_list) => {
                    let mut list_builder = doc_builder.list();
                    transform_list(inner_list, doc_builder, &mut list_builder)?;
                    builder.put_list(key, list_builder)?;
                }
                Yaml::Real(value) => builder.put_string(key, value)?,
                _ => (),
            }
        }
    }

    Ok(())
}
