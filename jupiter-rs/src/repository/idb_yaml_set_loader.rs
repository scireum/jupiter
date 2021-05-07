//! Imports YAML files as sets into InfoGraphDB.
//!
//! The file is expected to contain a map (hash) which contains lists defining the sets.
//!
//! ## Example data:
//! ```yaml
//! set1: [A, B, C]
//! set2: [X, Y]
//! ```
//! ## Example loader:
//! ```yaml
//! file: 'path/to/file.json'
//! loader: 'idb-yaml-sets'
//! namespace: 'target namespace to import into'
//! ```
use crate::idb::set::Set;
use crate::idb::{Database, DatabaseCommand};
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use apollo_framework::platform::Platform;
use linked_hash_map::OccupiedEntry;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use yaml_rust::Yaml;

/// Represents the global loader instance.
pub struct IdbYamlSetLoader {
    platform: Arc<Platform>,
}

impl IdbYamlSetLoader {
    /// Creates a new loader to be passed into [Repository::register_loader].
    pub fn new(platform: Arc<Platform>) -> Self {
        IdbYamlSetLoader { platform }
    }
}

impl Display for IdbYamlSetLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDB-YAML-SET")
    }
}

#[async_trait::async_trait]
impl Loader for IdbYamlSetLoader {
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let data = tokio::fs::read_to_string(loader_info.get_data())
            .await
            .context("Unable to read data file")?;
        let rows = yaml_rust::YamlLoader::load_from_str(data.as_str())
            .context("Cannot parse the given YAML data.")?;
        let source = loader_info.file_name().to_string();
        let sets = self.load_sets(rows);
        for (name, set) in sets {
            self.register_set(source.clone(), name, set).await?;
        }

        Ok(())
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }

    async fn file_deleted(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let source = loader_info.file_name().to_string();

        self.platform()
            .require::<Database>()
            .perform(DatabaseCommand::DropSets(source))
            .await
            .context("Failed to drop set.")?;

        Ok(())
    }
}

impl IdbYamlSetLoader {
    fn load_sets(&self, rows: Vec<Yaml>) -> Vec<(String, Set)> {
        let mut result = Vec::new();
        for row in rows {
            if let Yaml::Hash(mut map) = row {
                for entry in map.entries() {
                    if let Some((name, set)) = self.transform(entry) {
                        result.push((name, set));
                    }
                }
            }
        }

        result
    }

    fn transform(&self, entry: OccupiedEntry<Yaml, Yaml>) -> Option<(String, Set)> {
        if let Yaml::String(key) = entry.key() {
            if let Yaml::Array(list) = entry.get() {
                let mut set = Set::default();
                for item in list {
                    if let Some(str) = item.as_str() {
                        set.add(str.to_string());
                    }
                }

                Some((key.to_owned(), set))
            } else {
                None
            }
        } else {
            None
        }
    }

    async fn register_set(&self, source: String, key: String, set: Set) -> anyhow::Result<()> {
        self.platform()
            .require::<Database>()
            .perform(DatabaseCommand::CreateSet(source, key, set))
            .await
            .context("Failed to create set.")
    }
}
