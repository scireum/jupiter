//! Imports JSON files into InfoGraph DB.
//!
//! ## Example data:
//! ```json
//! [
//!     {"name": "test", "unit": "PCE"},
//!     {"name": "foo", "unit": "MTR"},
//! ]
//! ```
//! ## Example loader:
//! ```yaml
//! file: 'path/to/file.json'
//! loader: 'idb-json'
//! namespace: 'target namespace to import into'
//! table: 'target-table-name'
//! indices: ['fields', 'to', 'index']
//! fulltextIndices: ['fields', 'to', 'search', 'in']
//! ```
use crate::ig::json::list_to_doc;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use apollo_framework::platform::Platform;
use serde_json::Value;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Represents the global loader instance.
pub struct IdbJsonLoader {
    platform: Arc<Platform>,
}

impl IdbJsonLoader {
    /// Creates a new loader to be passed into [Repository::register_loader].
    pub fn new(platform: Arc<Platform>) -> Self {
        IdbJsonLoader { platform }
    }
}

impl Display for IdbJsonLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDB-JSON")
    }
}

#[async_trait::async_trait]
impl Loader for IdbJsonLoader {
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let data = loader_info.get_data_str().await?;
        let rows: Value =
            serde_json::from_str(data.as_str()).context("Cannot parse the given JSON data.")?;
        let doc = list_to_doc(rows.as_array().context("The given JSON wasn't an array.")?)
            .context("Cannot transform JSON to doc")?;

        self.update_table(doc, loader_info).await
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }
}
