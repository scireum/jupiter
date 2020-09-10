use std::sync::Arc;

use crate::commands::{queue, CommandDictionary, CommandError};
use crate::platform::Platform;

pub fn install(platform: &Arc<Platform>) {
    let commands = platform.require::<CommandDictionary>();
    let (queue, mut endpoint) = queue();
    tokio::spawn(async move {
        loop {
            match endpoint.recv().await {
                Some(mut call) => match call.response.simple("PONG") {
                    Ok(_) => call.complete(Ok(())),
                    Err(error) => call.complete(Err(CommandError::OutputError(error))),
                },
                _ => (),
            }
        }
    });

    commands.register_command("PING", queue, 0);
}
