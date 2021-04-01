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
//! ## Example loader:
//! ```yaml
//! file: 'path/to/file.yml'
//! loader: 'idb-yaml'
//! namespace: 'target namespace to import into'
//! table: 'target-table-name'
//! indices: ['fields', 'to', 'index']
//! fulltextIndices: ['fields', 'to', 'search', 'in']
//! ```
use crate::ig::yaml::list_to_doc;
use crate::platform::Platform;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

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
        let rows = yaml_rust::YamlLoader::load_from_str(data.as_str())
            .context("Cannot parse the given YAML data.")?;
        let doc = list_to_doc(rows.as_slice()).context("Cannot transform YAML to doc")?;

        self.update_table(doc, loader_info).await
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }
}
