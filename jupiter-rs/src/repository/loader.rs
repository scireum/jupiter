//! Defines loaders which are in charge of processing repository files.
//!
//! Within the repository a set of **YAML** files can be defined in the sub directory "/loaders".
//! These files provide the metadata required for a [Loader] to process a file once it has changed.
//!
//! Note that common loaders which process **YAML**, **JSON** and **CSV** and put the loaded data
//! into a **InfoGraphDB** table are provided by default.
use crate::commands::{queue, Call, CommandResult, Queue};
use crate::config::Config;
use crate::idb::table::{IndexType, Table};
use crate::idb::{Database, DatabaseCommand};
use crate::ig::docs::Doc;
use crate::platform::Platform;
use crate::repository::background::BackgroundCommand;
use crate::repository::{FileEvent, Repository, RepositoryFile};
use anyhow::Context;
use chrono::{DateTime, Local};
use itertools::Itertools;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
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
    last_error: Arc<Mutex<String>>,
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

    /// Extracts the name of the file used to configure the loader.
    pub fn loader_file_name(&self) -> Cow<str> {
        if let Some(name) = self.loader_file.file_name() {
            name.to_string_lossy()
        } else {
            Cow::Borrowed("")
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

    /// Retrieves the last known error this loader reported.
    pub fn last_error(&self) -> String {
        self.last_error.lock().unwrap().clone()
    }

    /// Stores an error for this loader.
    ///
    /// Note that this shouldn't be called manually, as the background actor of the repository
    /// (which invokes the loaders) already takes care of reporting errors. The loader itself
    /// is of course in charge to create and report proper error when called.
    pub fn store_error(&self, error: String) {
        *self.last_error.lock().unwrap() = error;
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
    loaders: &mut HashMap<String, Vec<LoaderInfo>>,
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

    for infos in loaders.values_mut() {
        for info in infos.iter_mut() {
            if file.path == info.loader_file {
                info.data_file = Repository::resolve(data_file).await?;
                info.config = config.clone();
                info.namespace = namespace.clone();

                if info.needs_reload().await? {
                    let was_enabled = info.enabled;
                    info.enabled = namespaces.contains(&namespace);
                    *info.last_error.lock().unwrap() = "".to_string();

                    if info.enabled {
                        info.last_load = Some(SystemTime::now());
                        background_task_sender
                            .send(BackgroundCommand::ExecuteLoaderForChange(info.clone()))
                            .await?;
                    } else if was_enabled {
                        info.last_load = None;
                        background_task_sender
                            .send(BackgroundCommand::ExecuteLoaderForDelete(info.clone()))
                            .await?;
                    }
                }

                return Ok(());
            }
        }
    }

    let enabled = namespaces.contains(&namespace);

    let new_loader = LoaderInfo {
        loader: repository.find_loader(loader)?,
        config: config.clone(),
        loader_file: file.path.clone(),
        data_file: Repository::resolve(data_file).await?,
        enabled,
        namespace,
        last_load: if enabled {
            Some(SystemTime::now())
        } else {
            None
        },
        last_error: Arc::new(Mutex::new("".to_string())),
    };

    if let Some(infos) = loaders.get_mut(data_file) {
        infos.push(new_loader.clone());
    } else {
        let _ = loaders.insert(data_file.to_owned(), vec![new_loader.clone()]);
    }

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
    loaders: &mut HashMap<String, Vec<LoaderInfo>>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    if let Some(infos) = loaders.get_mut(&file.name) {
        for info in infos {
            if info.enabled && info.needs_reload().await? {
                info.last_load = Some(SystemTime::now());
                *info.last_error.lock().unwrap() = "".to_string();
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForChange(info.clone()))
                    .await?;
            }
        }
    }

    Ok(())
}

/// Invoked if a loader varnishes.
async fn loader_removed(
    loaders: &mut HashMap<String, Vec<LoaderInfo>>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    for infos in loaders.values_mut() {
        if let Some((index, _)) = infos
            .iter()
            .find_position(|info| info.loader_file == file.path)
        {
            let info = infos.remove(index);
            if info.enabled {
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForDelete(info.clone()))
                    .await?;
            }
        }
    }

    Ok(())
}

/// Handles the removal of a data file.
async fn file_removed(
    loaders: &mut HashMap<String, Vec<LoaderInfo>>,
    file: &RepositoryFile,
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    if let Some(infos) = loaders.get_mut(&file.name) {
        for info in infos {
            if info.enabled {
                info.last_load = None;
                *info.last_error.lock().unwrap() = "".to_string();
                background_task_sender
                    .send(BackgroundCommand::ExecuteLoaderForDelete(info.clone()))
                    .await?;
            }
        }
    }

    Ok(())
}

/// Enables or disables loaders based on changes in the list of enabled namespaces.
async fn enforce_namespaces(
    loaders: &mut HashMap<String, Vec<LoaderInfo>>,
    namespaces: &[String],
    background_task_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> anyhow::Result<()> {
    for loaders in loaders.values_mut() {
        for loader in loaders {
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
    }

    Ok(())
}

/// Reports the current state of all known loaders.
fn list_command(call: &mut Call, loaders: &HashMap<String, Vec<LoaderInfo>>) -> CommandResult {
    let mut all_loaders = Vec::new();
    for infos in loaders.values() {
        for info in infos {
            all_loaders.push(info);
        }
    }

    if call.request.parameter_count() > 0 {
        call.response.array(all_loaders.len() as i32)?;
        for loader in all_loaders {
            call.response.array(6)?;
            call.response.simple(&loader.loader_file_name())?;
            call.response.simple(&loader.file_name())?;
            call.response.simple(&loader.namespace)?;
            call.response.boolean(loader.enabled)?;
            if let Some(last_load) = loader.last_load {
                call.response
                    .simple(DateTime::<Local>::from(last_load).to_rfc3339())?;
            } else {
                call.response.simple("")?;
            }
            call.response.bulk(loader.last_error())?;
        }
    } else {
        let mut result = String::new();

        for loader in all_loaders {
            result += format!("Loader File: {}\n", loader.loader_file_name()).as_str();
            result += format!("Data File:   {}\n", loader.file_name()).as_str();
            result += format!("Namespace:   {}\n", loader.namespace).as_str();
            result += format!("Enabled:     {}\n", loader.enabled).as_str();
            result += format!(
                "Last Load:   {}\n",
                if let Some(last_load) = loader.last_load {
                    DateTime::<Local>::from(last_load)
                        .format("%Y-%m-%dT%H:%M:%S")
                        .to_string()
                } else {
                    "".to_owned()
                }
            )
            .as_str();
            result += format!("Last Error:  {}\n", loader.last_error()).as_str();

            result += crate::response::SEPARATOR;
        }

        call.response.bulk(result)?;
    }

    Ok(())
}
