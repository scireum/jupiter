//! Imports YAML files into InfoGraph DB.
//!
//! ## Example data:
//! ```yaml
//! name: test
//! unit: PCE
//! ---
//! name: foo
//! unit: MTR
//! ```
//!
//! ## Example loader:
//! ```yaml
//! file: 'path/to/file.yml'
//! loader: 'idb-yaml'
//! namespace: 'target namespace to import into'
//! table: 'target-table-name'
//! indices: ['fields', 'to', 'index']
//! fulltextIndices: ['fields', 'to', 'search', 'in']
//! skipUnderscores: false # Determines if keys starting with _ should be ignored.
//! rowNumber: 'priority' # Specifies an auto-generated field which contains the row-number of each record
//! ```
use crate::ig::yaml::list_to_doc;
use crate::platform::Platform;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use yaml_rust::Yaml;

/// Represents the global loader instance.
pub struct IdbYamlLoader {
    platform: Arc<Platform>,
}

impl IdbYamlLoader {
    /// Creates a new loader to be passed into [Repository::register_loader].
    pub fn new(platform: Arc<Platform>) -> Self {
        IdbYamlLoader { platform }
    }
}

impl Display for IdbYamlLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDB-YAML")
    }
}

#[async_trait::async_trait]
impl Loader for IdbYamlLoader {
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let data = tokio::fs::read_to_string(loader_info.get_data())
            .await
            .context("Unable to read data file")?;
        let mut rows = yaml_rust::YamlLoader::load_from_str(data.as_str())
            .context("Cannot parse the given YAML data.")?;

        // If only one yaml object is present, and it's an array -> unwrap it. This was most probably
        // a JSON file like [{obj1}, {obj2}...]...
        if rows.len() == 1 && rows.first().unwrap().is_array() {
            rows = rows.remove(0).into_vec().unwrap();
        }

        let row_number_field = loader_info.get_config()["rowNumber"].as_str().unwrap_or("");
        if !row_number_field.is_empty() {
            let mut row_number = 1;
            let field_name = Yaml::String(row_number_field.to_string());
            for row in rows.iter_mut() {
                if let Yaml::Hash(record) = row {
                    let _ = record.insert(field_name.clone(), Yaml::Integer(row_number));
                }
                row_number += 1;
            }
        }
        let skip_underscores = loader_info.get_config()["skipUnderscores"]
            .as_bool()
            .unwrap_or(false);
        let doc = list_to_doc(rows.as_slice(), |key| {
            !skip_underscores | !key.starts_with('_')
        })
        .context("Cannot transform YAML to doc")?;

        self.update_table(doc, loader_info).await
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }
}
