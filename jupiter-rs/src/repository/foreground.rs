/// Contains the foreground actor of the repository.
///
/// The foreground actor is in charge accepting all commands and delegating them to the
/// `background actor`. If the background actor signals, that the repository contents have changed,
/// the `loader actor`is notified.
use crate::commands::{queue, Call, CommandResult, Queue};
use crate::repository::background::BackgroundCommand;
use crate::repository::{BackgroundEvent, Repository, RepositoryFile};
use anyhow::Context;
use apollo_framework::fmt::format_size;
use apollo_framework::platform::Platform;
use chrono::{DateTime, Local};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Lists the commands supported by this actor.
#[derive(FromPrimitive)]
pub enum ForegroundCommands {
    /// Lists the repository contents.
    List,

    /// Forces a scan of the repository contents. This is mainly useful for either debugging
    /// purposes or if the repository contents are managed by an external program like git.
    Scan,

    /// Fetches the requested url.
    Fetch,

    /// Fetches the requested url without any `Last-Modified` checks.
    ForceFetch,

    /// Deletes the given file.
    Delete,

    /// Updates the given file with the given contents. This is also mainly useful for debugging
    /// or to update tiny files.
    Store,

    /// Emits an BackgroundCommand::EmitEpochCounter and also immediately increments the foreground
    /// epoch counter. This can be used to determine if the background actor is idle (as it will
    /// immediately report back a BackgroundEvent::EpochCounter in this case so that the foreground
    /// and background epoch are equal. As long as they differ, the background actor is busy.
    IncEpoch,

    /// Reports the currently known epoch for the foreground and background.
    Epochs,
}

/// Installs the actor based on the given uplink to and from the background worker and returns an
/// input queue to receive commands.
pub fn actor(
    platform: Arc<Platform>,
    repository: Arc<Repository>,
    mut background_task_sender: mpsc::Sender<BackgroundCommand>,
    mut update_notifier: mpsc::Receiver<BackgroundEvent>,
) -> Queue {
    let (command_queue, mut commands_endpoint) = queue();

    let _ = tokio::spawn(async move {
        use crate::commands::ResultExt;

        let mut files = Vec::new();
        let mut foreground_epoch = 1;
        let mut background_epoch = 1;

        if let Err(error) = background_task_sender.send(BackgroundCommand::Scan).await {
            log::error!("Failed to start initial repository scan: {}", error);
        }

        while platform.is_running() {
            tokio::select! {
                cmd = commands_endpoint.recv() => if let Some(mut call) = cmd {
                    match ForegroundCommands::from_usize(call.token) {
                        Some(ForegroundCommands::Scan) => {
                            scan_command(&mut call, &mut background_task_sender).await.complete(call)
                        }
                        Some(ForegroundCommands::List) => list_command(&mut call, &files).complete(call),
                        Some(ForegroundCommands::Fetch) => {
                            fetch_command(&mut call, &mut background_task_sender, false).await.complete(call);
                        },
                        Some(ForegroundCommands::ForceFetch) => {
                            fetch_command(&mut call, &mut background_task_sender, true).await.complete(call);
                        }
                        Some(ForegroundCommands::Delete) => {
                            delete_command(&mut call, &mut background_task_sender).await.complete(call);
                        }
                        Some(ForegroundCommands::Store) => {
                            store_command(&mut call, &mut background_task_sender).await.complete(call);
                        }
                        Some(ForegroundCommands::IncEpoch) => {
                            inc_epoch_command(&mut call, &mut foreground_epoch, &mut background_task_sender).await.complete(call);
                        }
                        Some(ForegroundCommands::Epochs) => {
                            epochs_command(&mut call, foreground_epoch, background_epoch).await.complete(call);
                        }
                        None => ()
                    }
                },
                update = update_notifier.recv() => {
                    match update {
                        Some(BackgroundEvent::FileEvent(file_event)) => {
                            let _ = repository.broadcast_sender.send(file_event);
                        }
                        Some(BackgroundEvent::FileListUpdated(new_files)) => files = new_files,
                        Some(BackgroundEvent::EpochCounter(epoch)) => background_epoch = epoch,
                        _ => {}
                    }
                }
            }
        }
    });

    command_queue
}

async fn scan_command(
    call: &mut Call,
    background_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> CommandResult {
    background_sender
        .send(BackgroundCommand::Scan)
        .await
        .context("Failed to instruct background worker to perform a repository scan.")?;
    call.response.ok()?;

    Ok(())
}

fn list_command(call: &mut Call, files: &[RepositoryFile]) -> CommandResult {
    if call.request.parameter_count() > 0 {
        call.response.array(files.len() as i32)?;
        for file in files {
            call.response.array(3)?;
            call.response.simple(&file.name)?;
            call.response.number(file.size as i64)?;
            call.response
                .simple(DateTime::<Local>::from(file.last_modified).to_rfc3339())?;
        }
    } else {
        let mut result = String::new();

        result += "Use 'REPO.LIST raw' for to obtain the raw values.\n\n";

        result += format!("{:<50} {:>12} {:>25}\n", "Name", "Size", "Last Modified").as_str();
        result += crate::response::SEPARATOR;

        for file in files {
            result += format!(
                "{:<50} {:>12} {:>25}\n",
                &file.name,
                format_size(file.size as usize),
                DateTime::<Local>::from(file.last_modified)
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string()
            )
            .as_str();
        }
        result += crate::response::SEPARATOR;

        call.response.bulk(result)?;
    }

    Ok(())
}

async fn fetch_command(
    call: &mut Call,
    background_sender: &mut mpsc::Sender<BackgroundCommand>,
    force: bool,
) -> CommandResult {
    let path = call.request.str_parameter(0)?.to_string();

    if force {
        background_sender
            .send(BackgroundCommand::ForceFetch(
                path,
                call.request.str_parameter(1)?.to_string(),
            ))
            .await
            .context("Failed to enqueue FETCH into background queue.")?;
    } else {
        background_sender
            .send(BackgroundCommand::Fetch(
                path,
                call.request.str_parameter(1)?.to_string(),
            ))
            .await
            .context("Failed to enqueue FETCH into background queue.")?;
    }

    call.response.ok()?;
    Ok(())
}

async fn delete_command(
    call: &mut Call,
    background_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> CommandResult {
    let path = call.request.str_parameter(0)?.to_string();

    background_sender
        .send(BackgroundCommand::Delete(path))
        .await
        .context("Failed to enqueue DELETE into background queue.")?;

    call.response.ok()?;
    Ok(())
}

async fn store_command(
    call: &mut Call,
    background_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> CommandResult {
    let path = call.request.str_parameter(0)?.to_string();
    let data = call.request.parameter(1)?.to_vec();

    background_sender
        .send(BackgroundCommand::Store(path, data))
        .await
        .context("Failed to enqueue STORE into background queue.")?;

    call.response.ok()?;
    Ok(())
}

async fn inc_epoch_command(
    call: &mut Call,
    foreground_epoch: &mut i64,
    background_sender: &mut mpsc::Sender<BackgroundCommand>,
) -> CommandResult {
    *foreground_epoch += 1;

    background_sender
        .send(BackgroundCommand::EmitEpochCounter(*foreground_epoch))
        .await
        .context("Failed to enqueue EMIT EPOCH into background queue.")?;

    call.response.ok()?;
    Ok(())
}

async fn epochs_command(
    call: &mut Call,
    foreground_epoch: i64,
    background_epoch: i64,
) -> CommandResult {
    call.response.array(2)?;
    call.response.number(foreground_epoch)?;
    call.response.number(background_epoch)?;

    Ok(())
}
