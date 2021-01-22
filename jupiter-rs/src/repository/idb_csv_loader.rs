use crate::ig::csv::csv_to_doc;
use crate::platform::Platform;
use crate::repository::loader::{Loader, LoaderInfo};
use anyhow::Context;
use csv::ReaderBuilder;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub struct IdbCsvLoader {
    platform: Arc<Platform>,
}

impl IdbCsvLoader {
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
