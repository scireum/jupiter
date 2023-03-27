//! Imports CSV files into InfoGraph DB.
//!
//! This loader is capable of reading UTF-8 encoded CSV files using ";" as separator.
//!
//! The first line contains the headers or field names to use. All subsequent lines contain
//! the data to import.
//!
//! ## Example data:
//! ```csv
//! name;value;unit
//! test;10;PCE
//! foo;5;MTR
//! ```
//! ## Example loader:
//! ```yaml
//! file: 'path/to/file.csv'
//! loader: 'idb-csv'
//! namespace: 'target namespace to import into'
//! table: 'target-table-name'
//! indices: ['fields', 'to', 'index']
//! fulltextIndices: ['fields', 'to', 'search', 'in']
//! ```
use crate::ig::csv::csv_to_doc;
use crate::platform::Platform;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use csv::ReaderBuilder;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Represents the global loader instance.
pub struct IdbCsvLoader {
    platform: Arc<Platform>,
}

impl IdbCsvLoader {
    /// Creates a new loader to be passed into [Repository::register_loader].
    pub fn new(platform: Arc<Platform>) -> Self {
        IdbCsvLoader { platform }
    }
}

impl Display for IdbCsvLoader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IDB-CSV")
    }
}

#[async_trait::async_trait]
impl Loader for IdbCsvLoader {
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let data = tokio::fs::read_to_string(loader_info.get_data())
            .await
            .context("Unable to read data file")?;
        let reader = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(data.as_bytes());
        let doc = csv_to_doc(reader).context("Cannot parse the given CSV into a doc")?;

        self.update_table(doc, loader_info).await
    }

    fn platform(&self) -> &Arc<Platform> {
        &self.platform
    }
}
