//! Provides a bunch of diagnostic commands.
//!
//! Provides the following commands:
//! * **SYS.COMMANDS**: Lists all known commands, their number of calls and their average duration.
//! * **SYS.CONNECTIONS**: Lists all currently connected clients.
//! * **SYS.KILL**: Terminates the connection to the given client (selected by its peer address).
//! * **SYS.MEM**: Reports the amount of currently allocated memory.
//!
//! Note that for development purposes in debug builds a command named **SYS.PANIC** is added as
//! well. All this does is raising a panic which will eventually most probably crash the tokio
//! thread or the whole process itself.
//!
//! [install](install) is invoked by the [Builder](crate::builder::Builder) unless disabled.
use crate::commands::{queue, Call, CommandDictionary, CommandError, CommandResult};
use apollo_framework::fmt::{format_short_duration, format_size};
use apollo_framework::platform::Platform;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::server::RespPayload;
use anyhow::Context;
use apollo_framework::config::Config;
use apollo_framework::server::Server;
use std::borrow::Cow;
use std::sync::Arc;

/// Enumerates the commands supported by this facility.
#[derive(FromPrimitive)]
enum Commands {
    Commands,
    Connections,
    Kill,
    Mem,
    SetConfig,
    #[cfg(debug_assertions)]
    Panic,
}

/// Installs the diagnostic commands into the given platform.
///
/// This is invoked by the [Builder](crate::builder::Builder) unless disabled.
pub fn install(platform: Arc<Platform>) {
    if let Some(commands) = platform.find::<CommandDictionary>() {
        let queue = actor(platform.clone());
        commands.register_command("SYS.COMMANDS", queue.clone(), Commands::Commands as usize);
        commands.register_command(
            "SYS.CONNECTIONS",
            queue.clone(),
            Commands::Connections as usize,
        );
        commands.register_command("SYS.KILL", queue.clone(), Commands::Kill as usize);
        commands.register_command("SYS.MEM", queue.clone(), Commands::Mem as usize);
        commands.register_command(
            "SYS.SET_CONFIG",
            queue.clone(),
            Commands::SetConfig as usize,
        );
        #[cfg(debug_assertions)]
        commands.register_command("SYS.PANIC", queue, Commands::Panic as usize);
    }
}

/// Receives incoming calls for the commands defined above.
fn actor(platform: Arc<Platform>) -> crate::commands::Queue {
    use crate::commands::ResultExt;

    let (queue, mut endpoint) = queue();

    let _ = tokio::spawn(async move {
        let server = platform.require::<Server<RespPayload>>();
        let config = platform.find::<Config>();
        let commands = platform.require::<CommandDictionary>();

        loop {
            match endpoint.recv().await {
                Some(mut call) => match Commands::from_usize(call.token) {
                    Some(Commands::Commands) => {
                        commands_command(&mut call, &commands).complete(call)
                    }
                    Some(Commands::Connections) => {
                        connections_command(&mut call, &server).complete(call)
                    }
                    Some(Commands::Kill) => kill_command(&mut call, &server).complete(call),
                    Some(Commands::Mem) => mem_command(&mut call).complete(call),
                    Some(Commands::SetConfig) => {
                        set_config_command(&mut call, &config).await.complete(call)
                    }
                    #[cfg(debug_assertions)]
                    Some(Commands::Panic) => panic_command(&mut call).complete(call),
                    _ => call.handle_unknown_token(),
                },
                _ => return,
            }
        }
    });

    queue
}

fn connections_command(call: &mut Call, server: &Arc<Server<RespPayload>>) -> CommandResult {
    let connections = server.connections();
    let mut result = String::new();

    result += format!("Open connections: {}\n\n", connections.len()).as_str();
    result += format!(
        "{:<20} {:<30} {:>10} {:>15}\n",
        "Remote Address", "Client Name", "Calls", "Avg. Duration"
    )
    .as_str();
    result += crate::response::SEPARATOR;

    for connection in connections {
        result += format!(
            "{:<20} {:<30} {:>10} {:>15}\n",
            &connection.peer_address,
            connection
                .payload
                .get_name()
                .as_ref()
                .as_ref()
                .map(|name| Cow::Owned(name.clone()))
                .unwrap_or(Cow::Borrowed("")),
            connection.payload.commands().count(),
            format_short_duration(connection.payload.commands().avg())
        )
        .as_str();
    }
    result += crate::response::SEPARATOR;

    call.response.bulk(result)?;

    Ok(())
}

fn kill_command(call: &mut Call, server: &Arc<Server<RespPayload>>) -> CommandResult {
    if server.kill(call.request.str_parameter(0)?) {
        call.response.ok()?;
        Ok(())
    } else {
        Err(CommandError::ServerError(anyhow::anyhow!("Unknown peer!")))
    }
}

async fn set_config_command(call: &mut Call, config: &Option<Arc<Config>>) -> CommandResult {
    if let Some(config) = config {
        let new_config = call
            .request
            .str_parameter(0)
            .context("Expected a valid YAML config as parameter.")?;
        config.store(new_config).await?;
        call.response.ok()?;
        Ok(())
    } else {
        Err(CommandError::ServerError(anyhow::anyhow!(
            "Config is not enabled for this setup!"
        )))
    }
}

fn commands_command(call: &mut Call, commands: &Arc<CommandDictionary>) -> CommandResult {
    let command_list = commands.commands();
    let mut result = String::new();

    result += format!("{:<30} {:>10} {:>20}\n", "Name", "Calls", "Duration").as_str();
    result += crate::response::SEPARATOR;

    for cmd in command_list {
        result += format!(
            "{:<30} {:>10} {:>20}\n",
            &cmd.name,
            cmd.call_count(),
            format_short_duration(cmd.avg_duration())
        )
        .as_str();
    }
    result += crate::response::SEPARATOR;

    call.response.bulk(result)?;

    Ok(())
}

fn mem_command(call: &mut Call) -> CommandResult {
    let _ = jemalloc_ctl::epoch::advance();

    let allocated = jemalloc_ctl::stats::allocated::mib()
        .and_then(|mib| mib.read())
        .unwrap_or(0);
    let resident = jemalloc_ctl::stats::resident::mib()
        .and_then(|mib| mib.read())
        .unwrap_or(0);

    if call.request.parameter_count() == 1 {
        call.response.array(2)?;
        call.response.number(allocated as i64)?;
        call.response.number(resident as i64)?;
    } else {
        let mut result = "Use 'SYS.MEM raw' to obtain the raw values.\n\n".to_owned();
        result += format!("{:20} {:>10}\n", "Used Memory:", format_size(allocated)).as_str();
        result += format!("{:20} {:>10}\n", "Allocated Memory:", format_size(resident)).as_str();

        call.response.bulk(result)?;
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn panic_command(_call: &mut Call) -> CommandResult {
    panic!("A program termination has been requested!");
}

#[cfg(test)]
mod tests {
    use crate::builder::Builder;
    use crate::server::{resp_protocol_loop, RespPayload};
    use crate::testing::{query_redis_async, test_async};
    use apollo_framework::config::Config;
    use apollo_framework::server::Server;

    #[test]
    fn integration_test() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            //  Setup and create a platform...
            let platform = Builder::new().enable_all().build().await;

            // Specify a minimal config so that we run on a different port than a
            // production instance.
            platform
                .require::<Config>()
                .load_from_string(
                    "
                           server:
                                port: 1503
                         ",
                    None,
                )
                .unwrap();

            // Normally we'd directly run the event loop here:
            // platform.require::<Server>().event_loop().await;

            // However, as we want to run some examples, we fork the server in an
            // separate thread..
            Server::fork_and_await(
                &platform.require::<Server<RespPayload>>(),
                &resp_protocol_loop,
            )
            .await;

            // Invoke some diagnostics...
            assert_eq!(
                query_redis_async(|con| redis::cmd("SYS.COMMANDS").query::<String>(con))
                    .await
                    .is_some(),
                true
            );
            assert_eq!(
                query_redis_async(|con| redis::cmd("SYS.CONNECTIONS").query::<String>(con))
                    .await
                    .is_some(),
                true
            );
            assert_eq!(
                query_redis_async(|con| redis::cmd("SYS.MEM").query::<String>(con))
                    .await
                    .is_some(),
                true
            );

            assert_eq!(
                query_redis_async(|con| redis::cmd("SYS.SET_CONFIG")
                    .arg(
                        "
                        server:
                            port: 1503
                        "
                    )
                    .query::<String>(con))
                .await
                .is_some(),
                true
            );

            // try to deleted the test-config so that it doesn't interfere with other tests...
            let _ = std::fs::remove_file("settings.yml");

            // KILL requires at least one parameter...
            assert_eq!(
                query_redis_async(|con| redis::cmd("SYS.KILL").query::<String>(con))
                    .await
                    .is_none(),
                true
            );

            platform.terminate();
        });
    }
}
