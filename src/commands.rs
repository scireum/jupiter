use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use bytes::BytesMut;

use crate::platform::Platform;
use crate::request::Request;
use crate::response::{OutputError, Response};
use crate::watch::{Average, Watch};

#[derive(Debug)]
pub enum CommandError {
    OutputError(OutputError),
    ClientError(anyhow::Error),
    ServerError(anyhow::Error),
}

#[macro_export]
macro_rules! server_error {
    ($err:expr $(,)?) => ({
        use anyhow::anyhow;
        jupiter::commands::CommandError::ServerError(anyhow!($err))
    });
    ($fmt:expr, $($arg:tt)*) => {
        use anyhow::anyhow;
        jupiter::commands::CommandError::ServerError(anyhow!($fmt, $($arg)*))
    };
}

#[macro_export]
macro_rules! client_error {
    ($err:expr $(,)?) => ({
        use anyhow::anyhow;
        jupiter::commands::CommandError::ClientError(anyhow!($err))
    });
    ($fmt:expr, $($arg:tt)*) => {
        use anyhow::anyhow;
        jupiter::commands::CommandError::ClientError(anyhow!($fmt, $($arg)*))
    };
}

impl From<OutputError> for CommandError {
    fn from(output_error: OutputError) -> Self {
        CommandError::OutputError(output_error)
    }
}

impl From<anyhow::Error> for CommandError {
    fn from(error: anyhow::Error) -> Self {
        CommandError::ClientError(error)
    }
}

pub type CommandResult = std::result::Result<(), CommandError>;

pub trait ResultExt {
    fn complete(self, call: Call);
}

impl ResultExt for CommandResult {
    fn complete(self, call: Call) {
        call.complete(self);
    }
}

pub struct Call {
    pub request: Request,
    pub response: Response,
    pub token: usize,
    callback: tokio::sync::oneshot::Sender<Result<BytesMut, OutputError>>,
}

impl Call {
    pub fn complete(mut self, result: CommandResult) {
        let result = match result {
            Ok(_) => self.response.complete(),
            Err(CommandError::OutputError(error)) => Err(error),
            Err(CommandError::ClientError(error)) => {
                if let Err(error) = self.response.error(&format!("CLIENT: {}", error)) {
                    Err(error)
                } else {
                    self.response.complete()
                }
            }
            Err(CommandError::ServerError(error)) => {
                if let Err(error) = self.response.error(&format!("SERVER: {}", error)) {
                    Err(error)
                } else {
                    self.response.complete()
                }
            }
        };

        if let Err(_) = self.callback.send(result) {
            log::error!("Failed to submit a result to a oneshot callback channel!");
        }
    }
}

type Queue = tokio::sync::mpsc::Sender<Call>;
type Endpoint = tokio::sync::mpsc::Receiver<Call>;

pub fn queue() -> (Queue, Endpoint) {
    tokio::sync::mpsc::channel(1024)
}

pub struct Command {
    pub name: &'static str,
    queue: Queue,
    token: usize,
    call_metrics: Average,
}

impl Command {
    pub fn call_count(&self) -> i32 {
        self.call_metrics.count() as i32
    }

    pub fn avg_duration(&self) -> i32 {
        self.call_metrics.avg() as i32
    }
}

pub struct CommandDictionary {
    commands: Mutex<HashMap<&'static str, Arc<Command>>>,
}

pub struct Dispatcher {
    commands: HashMap<&'static str, (Arc<Command>, Queue)>,
}

impl CommandDictionary {
    pub fn new() -> Self {
        CommandDictionary {
            commands: Mutex::new(HashMap::default()),
        }
    }

    pub fn install(platform: &Arc<Platform>) -> Arc<Self> {
        let commands = Arc::new(CommandDictionary::new());
        platform.register::<CommandDictionary>(commands.clone());

        commands
    }

    pub fn register_command(&self, name: &'static str, queue: Queue, token: usize) {
        let mut commands = self.commands.lock().unwrap();
        if commands.get(name).is_some() {
            log::error!("Not going to register command {} as there is already a command present for this name",
                   name);
        } else {
            log::debug!("Registering command {}...", name);
            commands.insert(
                name,
                Arc::new(Command {
                    name,
                    queue,
                    token,
                    call_metrics: Average::new(),
                }),
            );
        }
    }

    pub fn commands(&self) -> Vec<Arc<Command>> {
        let mut result = Vec::new();
        for command in self.commands.lock().unwrap().values() {
            result.push(command.clone());
        }

        return result;
    }

    pub fn dispatcher(&self) -> Dispatcher {
        let commands = self.commands.lock().unwrap();
        let mut cloned_commands = HashMap::with_capacity(commands.len());
        for command in commands.values() {
            cloned_commands.insert(command.name, (command.clone(), command.queue.clone()));
        }

        Dispatcher {
            commands: cloned_commands,
        }
    }
}

impl Dispatcher {
    pub async fn invoke(&mut self, request: Request) -> Result<BytesMut, OutputError> {
        let mut response = Response::new();
        match self.commands.get_mut(request.command()) {
            Some((command, queue)) => {
                Dispatcher::invoke_command(command, queue, request, response).await
            }
            _ => {
                response.error(&format!("CLIENT: Unknown command: {}", request.command()))?;
                Ok(response.complete()?)
            }
        }
    }

    async fn invoke_command(
        command: &Arc<Command>,
        queue: &mut Queue,
        request: Request,
        response: Response,
    ) -> Result<BytesMut, OutputError> {
        let (callback, promise) = tokio::sync::oneshot::channel();
        let task = Call {
            request,
            response,
            callback,
            token: command.token,
        };

        let watch = Watch::start();
        if let Err(_) = queue.send(task).await {
            Err(OutputError::ProtocolError(anyhow!(
                "Failed to submit command into queue!"
            )))
        } else {
            match promise.await {
                Ok(result) => {
                    command.call_metrics.add(watch.micros());
                    result
                }
                _ => Err(OutputError::ProtocolError(anyhow!(
                    "Command {} did not yield any result!",
                    command.name
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use num_derive::FromPrimitive;
    use num_traits::FromPrimitive;

    use crate::commands::{queue, Call, CommandDictionary, CommandError, CommandResult, ResultExt};
    use crate::request::Request;

    fn ping(task: &mut Call) -> CommandResult {
        task.response.simple("PONG")?;
        Ok(())
    }

    fn test(task: &mut Call) -> CommandResult {
        task.response.simple("OK")?;
        Ok(())
    }

    #[derive(FromPrimitive)]
    enum TestCommands {
        Ping,
        Test,
    }

    #[test]
    fn a_command_can_be_executed() {
        tokio_test::block_on(async {
            let (queue, mut endpoint) = queue();
            tokio::spawn(async move {
                loop {
                    match endpoint.recv().await {
                        Some(mut call) => match TestCommands::from_usize(call.token) {
                            Some(TestCommands::Ping) => ping(&mut call).complete(call),
                            Some(TestCommands::Test) => test(&mut call).complete(call),
                            _ => call.complete(Err(CommandError::ServerError(anyhow::anyhow!(
                                "Unknown token received!"
                            )))),
                        },
                        _ => return,
                    }
                }
            });

            let commands = CommandDictionary::new();
            commands.register_command("PING", queue.clone(), TestCommands::Ping as usize);
            commands.register_command("TEST", queue.clone(), TestCommands::Test as usize);
            let mut dispatcher = commands.dispatcher();

            let request = Request::parse(&mut BytesMut::from("*1\r\n$4\r\nPING\r\n"))
                .unwrap()
                .unwrap();
            let result = dispatcher.invoke(request).await.unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+PONG\r\n");

            let request = Request::parse(&mut BytesMut::from("*1\r\n$4\r\nTEST\r\n"))
                .unwrap()
                .unwrap();
            let result = dispatcher.invoke(request).await.unwrap();
            assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");
        });
    }
}
