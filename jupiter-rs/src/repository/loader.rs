//! Defines loaders which are in charge of processing repository files.
//!
//! Within the repository a set of **YAML** files can be defined in the sub directory "/loaders".
//! These files provide the metadata required for a [Loader] to process a file once it has changed.
//!
//! Note that common loaders which process **YAML**, **JSON** and **CSV** and put the loaded data
//! into a **InfoGraphDB** table are provided by default.
use crate::commands::{queue, Call, CommandResult, Queue};
use crate::idb::table::{IndexType, Table};
use crate::idb::{Database, DatabaseCommand};
use crate::ig::docs::Doc;
use crate::repository::background::BackgroundCommand;
use crate::repository::{FileEvent, Repository, RepositoryFile};
use anyhow::Context;
use apollo_framework::config::Config;
use apollo_framework::platform::Platform;
use chrono::{DateTime, Local};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use yaml_rust::{Yaml, YamlLoader};

/// Describes a API of a loader.
///
/// A loader is informed once a file is either changed or deleted. Based on the given metadata
/// (which is essentially the content of the **YAML** file in the **loaders** directory it can
/// perform the required actions to either process the file or remove the data once the underlying
/// file has been removed.
#[async_trait::async_trait]
pub trait Loader: Display + Send + Sync {
    /// Invoked if either the loader metadata or the file itself has changed.
    async fn file_changed(&self, loader_info: &LoaderInfo) -> anyhow::Result<()>;

    /// Can be invoked by loaders which store data in **InfoGraphDB** to update a table
    /// with the indices as defined by the metadata **YAML**.
    async fn update_table(&self, doc: Doc, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        let table = Table::new(doc, self.indices(loader_info)).context("Failed to build table.")?;
        self.platform()
            .require::<Database>()
            .perform(DatabaseCommand::CreateTable(
                self.table_name(loader_info)?.to_owned(),
                table,
            ))
            .await
            .context("Failed to create table.")?;

        Ok(())
    }

    /// Provides the platform for which this loader was registered.
    fn platform(&self) -> &Arc<Platform>;

    /// Extracts the table name from the given metadata.
    ///
    /// This will parse the property `table` and report its contents or raise an error
    /// if the property is missing.
    fn table_name<'a>(&self, loader_info: &'a LoaderInfo) -> anyhow::Result<&'a str> {
        loader_info.config["table"]
            .as_str()
            .context("Missing config 'table'!")
    }

    /// Extracts the desired indices from the given metadata.
    ///
    /// This will parse `fulltextIndices` and `indices` and will transform these lists into
    /// a single list of indices to create for the table being registered in IDB.
    fn indices(&self, loader_info: &LoaderInfo) -> Vec<IndexType> {
        let mut result = Vec::new();
        if let Some(array) = loader_info.config["fulltextIndices"].as_vec() {
            for index in array
                .iter()
                .map(|value| value.as_str().unwrap_or(""))
                .filter(|value| !value.is_empty())
            {
                result.push(IndexType::fulltext(index))
            }
        }
        if let Some(array) = loader_info.config["indices"].as_vec() {
            for index in array
                .iter()
                .map(|value| value.as_str().unwrap_or(""))
                .filter(|value| !value.is_empty())
            {
                // If a fulltext index is already present, we do not need an additional lookup
                // index..
                if !result.iter().any(|other| other.field_name() == index) {
                    result.push(IndexType::lookup(index))
                }
            }
        }

        result
    }

    /// Invoked if the underlying file or the loader itself was deleted.
    ///
    /// By default the associated table from IDB will be removed.
    async fn file_deleted(&self, loader_info: &LoaderInfo) -> anyhow::Result<()> {
        if let Ok(name) = self.table_name(loader_info) {
            self.platform()
                .require::<Database>()
                .perform(DatabaseCommand::DropTable(name.to_string()))
                .await
                .context("Failed to drop table.")?;
        }

        Ok(())
    }
}

/// Wraps the metadata as well as the files being involved into a single struct which is provided
/// to all loaders which loading or unloading data.
#[derive(Clone)]
pub struct LoaderInfo {
    loader: Arc<dyn Loader>,
    config: Yaml,
    loader_file: PathBuf,
    data_file: PathBuf,
    namespace: String,
    enabled: bool,
    last_load: Option<SystemTime>,
}

impl LoaderInfo {
    /// Determines if the loader should be invoked as either the underlying data or the loader
    /// metadata has changed.
    async fn needs_reload(&self) -> anyhow::Result<bool> {
        if self.last_load.is_none() {
            Ok(true)
        } else {
            let loader_last_modified = tokio::fs::metadata(&self.loader_file)
                .await
                .ok()
                .context(format!(
                    "Failed to fetch metadata of {}",
                    self.loader_file.to_string_lossy()
                ))?
                .modified()
                .context(format!(
                    "Failed to determine last modified date of {}",
                    self.loader_file.to_string_lossy()
                ))?;
            let data_last_modified = tokio::fs::metadata(&self.data_file)
                .await
                .ok()
                .context(format!(
                    "Failed to fetch metadata of {}",
                    self.data_file.to_string_lossy()
                ))?
                .modified()
                .context(format!(
                    "Failed to determine last modified date of {}",
                    self.data_file.to_string_lossy()
                ))?;

            Ok(loader_last_modified > self.last_load.unwrap()
                || data_last_modified > self.last_load.unwrap())
        }
    }

    /// Extracts the name of the file being loaded.
    pub fn file_name(&self) -> Cow<str> {
        if let Some(name) = self.data_file.file_name() {
            name.to_string_lossy()
        } else {
            Cow::Borrowed("")
        }
    }

    /// Provides access to the actual loader in charge.
    pub fn get_loader(&self) -> &Arc<dyn Loader> {
        &self.loader
    }

    /// Provides access to the metadata of the loader.
    pub fn get_config(&self) -> &Yaml {
        &self.config
    }

    /// Returns the data file to load or unload.
    pub fn get_data(&self) -> &Path {
        self.data_file.as_path()
    }

    /// Fetches the contents of the data file as string.
    pub async fn get_data_str(&self) -> anyhow::Result<String> {
        tokio::fs::read_to_string(self.get_data())
            .await
            .context("Unable to read data file")
    }
}

impl Display for LoaderInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} -> {}",
            self.loader_file.to_string_lossy(),
            self.data_file.to_string_lossy()
        )
    }
}

impl Debug for LoaderInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

/// Lists the external commands provided by this actor.
#[derive(FromPrimitive)]
pub enum LoaderCommands {
    /// Represents the `LIST` command.
    List,
}

/// Creates the actor which manages the list of loaders based on the changes which are provided
/// into the queue being returned. If a loader needs to load or unload data, an appropriate task
/// is put into the given background sender.
pub fn actor(
    platform: Arc<Platform>,
    repository: Arc<Repository>,
    mut background_task_sender: mpsc::Sender<BackgroundCommand>,
) -> Queue {
    let (command_queue, mut commands_endpoint) = queue();
    let _ = tokio::spawn(async move {
        use crate::commands::ResultExt;

        let mut loaders = HashMap::new();
        let mut listener = repository.listener();
        let config = platform.require::<Config>();
        let mut config_changed_flag = config.notifier();
        let mut namespaces = load_namespaces(&config);

        while platform.is_running() {
            tokio::select! {
                event = listener.recv() => {
                    match event {
                        Ok(FileEvent::FileChanged(file)) => {
                            if file.name.starts_with("/loaders/") {
                                if let Err(e) = loader_changed(
                                    &mut loaders,
                                    &namespaces,
                                    &file,
                                    &mut background_task_sender,
                                    &repository,
                                ).await {
                                    log::error!("Failed to update loader {} - {:?}", file.name, e);
                                }
                            } else if let Err(e) = file_changed(&mut loaders, &file, &mut background_task_sender).await {
                                log::error!("Failed to update data file {} - {:?}", file.name, e);
                            }
                        }
                        Ok(FileEvent::FileDeleted(file)) => {
                            if file.name.starts_with("/loaders/") {
                                if let Err(e) = loader_removed(&mut loaders, &file, &mut background_task_sender).await {
                                    log::error!("Failed to remove loader {} - {:?}", file.name, e);
                                }
                            } else if let Err(e) = file_removed(&mut loaders, &file, &mut background_task_sender).await {
                                log::error!("Failed to remove loader for deleted file {} - {:?}", file.name, e);
                            }
                        }
                        _ => {}
                    }
                }
                cmd = commands_endpoint.recv() => if let Some(mut call) = cmd {
                    if let
                        Some(LoaderCommands::List) = LoaderCommands::from_usize(call.token) { list_command(&mut call, &loaders).complete(call) }
                    },

                _ = config_changed_flag.recv() => {
                    namespaces = load_namespaces(&config);
                    if let Err(e) = enforce_namespaces(&mut loaders, &namespaces, &mut background_task_sender).await {
                        log::error!("Failed to enforce namespaces: {:?}", e);
                    }
               }
            }
        }
    });

    command_queue
}

/// Fetches the list of enabled namespaces from the system config.
fn load_namespaces(config: &Arc<Config>) -> Vec<String> {
    if let Yaml::Array(ref namespaces) = config.current().config()["repository"]["namespaces"] {
        namespaces
            .iter()
            .map(|element| element.as_str().unwrap_or("").to_owned())
            .filter(|ns| !ns.is_empty())
            .collect()
    } else {
        vec![]
    }
}

/// Reacts on a change of a loader metadata file.
async fn loader_changed(
    loaders: &mut HashMap<String, LoaderInfo>,
    namespaces: &[String],
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
    repository: &Arc<Repository>,
) -> anyhow::Result<()> {
    let config_data = tokio::fs::read_to_string(file.path.as_path()).await?;
    let mut configs = YamlLoader::load_from_str(config_data.as_str())?;
    let config = configs.pop().unwrap_or(Yaml::Null);

    let data_file = config["file"].as_str().context("")?;
    let loader = config["loader"].as_str().context("")?;
    let namespace = config["namespace"].as_str().context("")?.to_owned();

    if let Some(info) = loaders
        .values_mut()
        .find(|info| file.path == info.loader_file)
    {
        info.data_file = Repository::resolve(&data_file).await?;
        info.config = config.clone();
        info.namespace = namespace.clone();

        if info.needs_reload().await? {
            info.enabled = namespaces.contains(&namespace);
            if info.enabled {
                info.last_load = Some(SystemTime::now());
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForChange(info.clone()))
                    .await?;
            } else {
                info.last_load = None;
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForDelete(info.clone()))
                    .await?;
            }
        }

        return Ok(());
    }

    let enabled = namespaces.contains(&namespace);

    let new_loader = LoaderInfo {
        loader: repository.find_loader(loader)?,
        config: config.clone(),
        loader_file: file.path.clone(),
        data_file: Repository::resolve(&data_file).await?,
        enabled,
        namespace,
        last_load: if enabled {
            Some(SystemTime::now())
        } else {
            None
        },
    };

    let _ = loaders.insert(data_file.to_owned(), new_loader.clone());

    if enabled {
        background_task_sender
            .send(BackgroundCommand::ExecuteLoaderForChange(
                new_loader.clone(),
            ))
            .await?;
    }

    Ok(())
}

/// Processes a content update of a data file.
async fn file_changed(
    loaders: &mut HashMap<String, LoaderInfo>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    if let Some(info) = loaders.get_mut(&file.name) {
        if info.enabled && info.needs_reload().await? {
            info.last_load = Some(SystemTime::now());
            background_task_sender
                .send(BackgroundCommand::ExecuteLoaderForChange(info.clone()))
                .await?;
        }
    }

    Ok(())
}

/// Invoked if a loader varnishes.
async fn loader_removed(
    loaders: &mut HashMap<String, LoaderInfo>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    if let Some(key) = loaders.iter().find_map(|(key, info)| {
        if file.path == info.loader_file {
            Some(key.clone())
        } else {
            None
        }
    }) {
        if let Some(info) = loaders.remove(&key) {
            if info.enabled {
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForDelete(info))
                    .await?;
            }
        }
    }

    Ok(())
}

/// Handles the removal of a data file.
async fn file_removed(
    loaders: &mut HashMap<String, LoaderInfo>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    if let Some(info) = loaders.get_mut(&file.name) {
        if info.enabled {
            info.last_load = None;
            background_task_sender
                .send(BackgroundCommand::ExecuteLoaderForDelete(info.clone()))
                .await?;
        }
    }

    Ok(())
}

/// Enables or disables loaders based on changes in the list of enabled namespaces.
async fn enforce_namespaces(
    loaders: &mut HashMap<String, LoaderInfo>,
    namespaces: &[String],
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    for (_, loader) in loaders.iter_mut() {
        let is_enabled = namespaces.contains(&loader.namespace);
        if is_enabled != loader.enabled {
            loader.enabled = is_enabled;
            if is_enabled {
                log::info!(
                    "{} has been enabled via namespace {}. Loading...",
                    loader.file_name(),
                    &loader.namespace
                );
                loader.last_load = Some(SystemTime::now());
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForChange(loader.clone()))
                    .await?;
            } else {
                log::info!(
                    "{} has been disabled via namespace {}. Unloading...",
                    loader.file_name(),
                    &loader.namespace
                );
                loader.last_load = None;
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForDelete(loader.clone()))
                    .await?;
            }
        }
    }

    Ok(())
}

/// Reports the current state of all known loaders.
fn list_command(call: &mut Call, loaders: &HashMap<String, LoaderInfo>) -> CommandResult {
    if call.request.parameter_count() > 0 {
        call.response.array(loaders.len() as i32)?;
        for loader in loaders.values() {
            call.response.array(4)?;
            call.response.simple(&loader.file_name())?;
            call.response.simple(&loader.namespace)?;
            call.response.boolean(loader.enabled)?;
            if let Some(last_load) = loader.last_load {
                call.response
                    .simple(DateTime::<Local>::from(last_load).to_rfc3339())?;
            } else {
                call.response.simple("")?;
            }
        }
    } else {
        let mut result = String::new();

        result += format!(
            "{:<32} {:<16} {:<8} {:>20}\n",
            "File", "Namespace", "Enabled", "Last Load"
        )
        .as_str();
        result += crate::response::SEPARATOR;

        for loader in loaders.values() {
            result += format!(
                "{:<32} {:<16} {:<8} {:>20}\n",
                loader.file_name(),
                loader.namespace,
                loader.enabled,
                if let Some(last_load) = loader.last_load {
                    DateTime::<Local>::from(last_load)
                        .format("%Y-%m-%dT%H:%M:%S")
                        .to_string()
                } else {
                    "".to_owned()
                }
            )
            .as_str();
        }
        result += crate::response::SEPARATOR;

        call.response.bulk(result)?;
    }

    Ok(())
}
