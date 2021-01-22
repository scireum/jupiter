//! Provides the dispatcher which delegates an incoming request to the matching implementation.
//!
//! At its core, a [CommandsCommandDictionary](CommandDictionary) dictionary is simply a hash map.
//! For each known command, it contains an appropriate entry. This however, doesn't point to a trait
//! impl or the like, rather it points to a [Queue](Queue) and keeps a numeric index along. If the
//! matching command is received, the request, a pre-initialized response and the numeric index are
//! all wrapped in a [Call](Call). This call is also equipped with the **Sender** of a oneshot
//! channel through which the final response will be send. This call is then sent to the actor which
//! is responsible for handling the command using the queue in the dictionary. Once the actor
//! completed handling the request, the response is sent back (most probably with the help of
//! [ResultExt](ResultExt)) and the response will be then returned.
//!
//! This approach seems complex at first sight but provides many advantages. First of all, using
//! queues from and to the actor permits us to write 100% async/await code without having to
//! hack around async traits (which aren't directly supported in Rust right now and would require
//! an allocation anyway). Furthermore using a single queue for all commands of an actor (hence the
//! token which can be specified per command) makes the actor internally behave like a single
//! threaded task which greatly simplifies concurrency and synchronization requirements.
//!
//! Note that actually the **Commands** is only a container to collect and keep all known commands.
//! Each client will request a [Dispatcher](Dispatcher) from it, which can then dispatch incoming
//! requests and return the appropriate results. This is used so that no locking or other
//! synchronization primitives are required during operation (once the server is setup, the commands
//! dictionary will never change anyway).
//!
//! # Errors
//!
//! When handling a command, a function which returns a [CommandResult](CommandResult) should be
//! used. This way, one can simply call `my_handler(&mut call).complete(call)` which will return
//! either the response or the error into the callback within **call**. Note that two macros
//! [server_error](server_error) and [client_error](client_error) are provided. The former should
//! be used if a server-sided problem occurs, the latter if the client sent inconsistent or
//! incompatible data. Note also that any **anyhow::Error** can be transformed into a CommandError,
//! which will then be treat just like a **client_error**.
//!
//! # Examples
//!
//! Registering a command:
//! ```no_run
//! # use jupiter::commands::{CommandDictionary, queue, CommandError};
//!
//! // Create a new command queue to listen to....
//! let (queue, mut endpoint) = queue();
//!
//! // Bind a simple actor which only responds to a single command to the endpoint
//! // of the queue.
//! tokio::spawn(async move {
//!     loop {
//!         match endpoint.recv().await {
//!             // Because of the simple implementation, we directly respond with "PONG" and perform
//!             // the error handling / completion manually...
//!             Some(mut call) => match call.response.simple("PONG") {
//!                 Ok(_) => call.complete(Ok(())),
//!                 Err(error) => call.complete(Err(CommandError::OutputError(error))),
//!              },
//!              _ => return,
//!         }
//!     }
//! });
//!
//! // Register the queue for the "PING" command. We can pass "0" here as token, as the actor
//! // doesn't check it anyway as only a single command is handled via this queue...
//! let commands = CommandDictionary::new();
//! commands.register_command("PING", queue, 0);
//! ```
//!
//! When handling several commands via a single queue, using **num_traits** and **num_derive** is
//! recommended. This way one can define all commands as an enum as done in the following example:
//!
//! ```
//! # use bytes::BytesMut;
//! # use num_derive::FromPrimitive;
//! # use num_traits::FromPrimitive;
//! # use jupiter::commands::{queue, Call, CommandDictionary, CommandError, CommandResult, ResultExt};
//! # use jupiter::request::Request;
//!
//! // A simple command with responds PONG for every PING...
//! fn ping(task: &mut Call) -> CommandResult {
//!     task.response.simple("PONG")?;
//!     Ok(())
//! }
//!
//! // Another simple command which simply replies with "OK"...
//! fn test(task: &mut Call) -> CommandResult {
//!     task.response.simple("OK")?;
//!     Ok(())
//! }
//!
//! /// Defines an enum to list all supported commands...
//! #[derive(num_derive::FromPrimitive)]
//! enum TestCommands {
//!     Ping,
//!     Test,
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!
//!     // Defines an actor which delegates incoming calls to the proper function as defined
//!     // above...
//!     let (queue, mut endpoint) = queue();
//!     tokio::spawn(async move {
//!         loop {
//!             match endpoint.recv().await {
//!                 Some(mut call) => match TestCommands::from_usize(call.token) {
//!                     Some(TestCommands::Ping) => ping(&mut call).complete(call),
//!                     Some(TestCommands::Test) => test(&mut call).complete(call),
//!                     _ => call.handle_unknown_token(),
//!                 },
//!                 _ => return,
//!             }
//!         }
//!     });
//!
//!     // Build a dictionary, register both commands and obtain a dispatcher from it...
//!     let commands = CommandDictionary::new();
//!     commands.register_command("PING", queue.clone(), TestCommands::Ping as usize);
//!     commands.register_command("TEST", queue.clone(), TestCommands::Test as usize);
//!     let mut dispatcher = commands.dispatcher();
//!
//!     // Send a "PING" as RESP request and expect a "PONG" as RESP response...
//!     let request = Request::example(vec!("PING"));
//!     let result = dispatcher.invoke(request, None).await.unwrap();
//!     assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+PONG\r\n");
//!
//!     // Send a "TEST" as RESP request and expect a "OK" as RESP response...
//!     let request = Request::example(vec!("TEST"));
//!     let result = dispatcher.invoke(request, None).await.unwrap();
//!     assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+OK\r\n");
//! }
//! ```
//!
//! More examples can be found here: [CORE commands](crate::core::install).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::anyhow;
use bytes::BytesMut;

use crate::average::Average;
use crate::platform::Platform;
use crate::request::Request;
use crate::response::{OutputError, Response};
use crate::server::Connection;

/// Represents an error when executing a command.
///
/// We mainly distinguish three cases: **OutputErrors** mostly occur due to IO / network errors
/// if the [Response](crate::response::Response) fails to write data into the internal buffer
/// or onto the underlying socket. A **ServerError** signals that a server-sided problem or
/// program error occurred which is sort of unexpected. Finally the common case, a **ClientError**
/// signals that the data passed in from the client was invalid or inconsistent or didn't match the
/// expectations of the server.
#[derive(Debug)]
pub enum CommandError {
    OutputError(OutputError),
    ClientError(anyhow::Error),
    ServerError(anyhow::Error),
}

/// Provides a simple way of creating a **CommandError** which respesents a **ServerError**.
///
/// # Example
///
/// ```
/// use jupiter::commands::{Call, CommandResult};
/// fn my_command(call: &mut Call) -> CommandResult {
///     Err(jupiter::server_error!("We forgot to implement this command."))
/// }
/// ```
#[macro_export]
macro_rules! server_error {
    ($err:expr $(,)?) => ({
        jupiter::commands::CommandError::ServerError(anyhow::anyhow!($err))
    });
    ($fmt:expr, $($arg:tt)*) => {
        jupiter::commands::CommandError::ServerError(anyhow::anyhow!($fmt, $($arg)*))
    };
}

/// Provides a simple way of creating a **CommandError** which respesents a **ClientError**.
///
/// # Example
///
/// ```
/// use jupiter::commands::{Call, CommandResult};
/// fn my_command(call: &mut Call) -> CommandResult {
///     if call.request.parameter_count() > 2 {
///         Err(jupiter::client_error!(
///             "This command only accepts 2 parameters but {} were provided",
///             call.request.parameter_count()
///         ))
///     } else {
///         call.response.ok()?;
///         Ok(())
///     }
/// }   
/// ```
#[macro_export]
macro_rules! client_error {
    ($err:expr $(,)?) => ({
        jupiter::commands::CommandError::ClientError(anyhow::anyhow!($err))
    });
    ($fmt:expr, $($arg:tt)*) => {
        jupiter::commands::CommandError::ClientError(anyhow::anyhow!($fmt, $($arg)*))
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

/// Represents the return type of command invocations.
///
/// These are either an empty result (as the real result is passed through via the response within
/// the call) or a **CommandError* to signal that either an IO / output error, a server error or a
/// client error occurred.
pub type CommandResult = std::result::Result<(), CommandError>;

/// Provides an extension trait on [CommandResult](CommandResult) so that **complete** can be
/// directly invoked on it. This reduces some boilerplate as:
///
/// ```compile_fail
/// # let call = Call::new();
/// let result = my_command(&mut call);
/// call.complete(result);
/// ```
/// becomes:
/// ```compile_fail
/// my_command(&mut call).complete(call)
/// ```
///
/// Note that this has be be defined as trait so that we can "attach" it to
/// [CommandResult](CommandResult) which is internally a normal Rust **Result**. Note that the
/// **ResultExt** trait has to be visible (used) so that the compiler permits to invoke this
/// method.
pub trait ResultExt {
    /// Completes the given call with the wrapped result.
    ///
    /// This is equivalent to `call.complete(self)` but more compact to write.
    fn complete(self, call: Call);
}

impl ResultExt for CommandResult {
    fn complete(self, call: Call) {
        call.complete(self);
    }
}

/// Represents the invocation of a command.
///
/// This wraps the request, a pre-initialized result and the command index (token) in a single
/// struct which is then sent to the actor executing the command using a queue. It also contains
/// the callback which is another (oneshot) queue to send back the result - however this is
/// completely handled internally, therefore only **complete** has to be called for this call to
/// finish processing a command.
pub struct Call {
    /// Contains the request as sent by the client.
    pub request: Request,

    /// Contains the response to be filled with the response data
    pub response: Response,

    /// Contains the index of the commend being called.
    ///
    /// This is required as commonly a bunch of commands share a single queue, so that the
    /// invocation of these commands is "single threaded" from the view of the actor.
    pub token: usize,

    callback: tokio::sync::oneshot::Sender<Result<BytesMut, OutputError>>,
}

impl Call {
    /// Marks the command represented by this call as handled.
    ///
    /// In case of a successful completion, this simply closes the response and sends the serialized
    /// data back to the caller. In case of an error, an appropriate representation is generated
    /// and sent back as alternative response. However, in case of an **OutputError**, we directly
    /// send error back to the caller, as the connection might be in an inconsistent state. The
    /// caller then will most probably try to write an error and then close the connection to
    /// prevent any further inconsistencies.
    pub fn complete(mut self, result: CommandResult) {
        // Transforms the current result / response to either a byte representation of the
        // RESP response or, in case of an OutputError, to an error to be send back to the caller..
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

        if self.callback.send(result).is_err() {
            log::error!("Failed to submit a result to a oneshot callback channel!");
        }
    }

    pub fn handle_unknown_token(self) {
        let token = self.token;
        self.complete(Err(CommandError::ServerError(anyhow::anyhow!(
            "Unknown token received: {}!",
            token
        ))));
    }
}

/// Represents a queue which can be stored in a [CommandDictionary](CommandDictionary) in order
/// to receive [Calls](Call) to be handled.
///
/// A queue can be created using the [queue()](queue) function.
pub type Queue = tokio::sync::mpsc::Sender<Call>;

/// Represents an endpoint of a [Queue](Queue) which is moved into an actor in order to receive
/// [Calls](Call) there.
///
/// A queue can be created using the [queue()](queue) function.
pub type Endpoint = tokio::sync::mpsc::Receiver<Call>;

/// Creates a new queue which connects an actor to the [CommandDictionary](CommandDictionary).
///
/// Note that these queues are limited in size (1024) which should provide plenty of room for
/// queuing incoming commands (actually it only needs to be the size of the expected number of
/// clients). We do not use an unbounded queue, as we'd rather start rejecting incoming commands
/// than crashing the whole server while running out of memory in an overload condition.
pub fn queue() -> (Queue, Endpoint) {
    tokio::sync::mpsc::channel(1024)
}

/// Wraps a command which as previously been registered.
///
/// This is made public so that the management APIs can provide access to the utilization metrics.
pub struct Command {
    pub name: &'static str,
    queue: Queue,
    token: usize,
    call_metrics: Average,
}

impl Command {
    /// Returns the number of invocations of this command.
    pub fn call_count(&self) -> u64 {
        self.call_metrics.count()
    }

    /// Returns the average call duration in micro seconds.
    pub fn avg_duration(&self) -> i32 {
        self.call_metrics.avg() as i32
    }
}

/// Represents an internally mutable dictionary which maps commands to queues.
///
/// A command dictionary is used by the server to keep track of all known commands and to determine
/// which queue is used to handle a certain command.
///
/// Note that the dictionary itself isn't used to actually dispatch a command. This is the job of
/// the [Dispatcher](Dispatcher) which is a readonly copy of the dictionary which can be used
/// without any synchronization overhead.
#[derive(Default)]
pub struct CommandDictionary {
    commands: Mutex<HashMap<&'static str, Arc<Command>>>,
}

/// Provides a readonly view of a [CommandDictionary](CommandDictionary) used to actually dispatch
/// calls of the appropriate queue.
///
/// Being a readonly copy, a dispatch can operate without any locking or synchronization overheads.
pub struct Dispatcher {
    commands: HashMap<&'static str, (Arc<Command>, Queue)>,
}

impl CommandDictionary {
    /// Creates a new and empty dictionary.
    ///
    /// Note that most probably a dictionary is already present and can be obtained from the
    /// [Platform](crate::platform::Platform):
    ///
    /// ```
    /// # use jupiter::platform::Platform;
    /// # use jupiter::commands::CommandDictionary;
    /// # use jupiter::builder::Builder;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let platform = Builder::new().enable_commands().build().await;
    /// let commands = platform.require::<CommandDictionary>();
    /// # }
    /// ```
    pub fn new() -> Self {
        CommandDictionary {
            commands: Mutex::new(HashMap::default()),
        }
    }

    /// Creates and installs the default dictionary into the given **Platform**.
    ///
    /// Note that this is automatically performed by the [Builder](crate::builder::Builder) unless
    /// disabled.
    pub fn install(platform: &Arc<Platform>) -> Arc<Self> {
        let commands = Arc::new(CommandDictionary::new());
        platform.register::<CommandDictionary>(commands.clone());

        commands
    }

    /// Registers a command for the given name to be dispatched into the given queue.
    ///
    /// As most probably multiple commands are dispatched to a single queue, their calls can
    /// be disambiguated by using the given token.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::commands::{CommandDictionary, queue, CommandError};
    ///
    /// let (queue, mut endpoint) = queue();
    ///
    /// // Attach an actor to the endpoint of the queue...
    /// // tokio::spawn(...)
    ///
    /// // Register the command...
    /// let commands = CommandDictionary::new();
    /// commands.register_command("PING", queue, 0);
    /// ```
    ///
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

    /// Reports the usage metrics (and names) of all known commands.
    pub fn commands(&self) -> Vec<Arc<Command>> {
        let mut result = Vec::new();
        for command in self.commands.lock().unwrap().values() {
            result.push(command.clone());
        }

        result
    }

    /// Creates a readonly copy of the known commands and returns them as dispatcher.
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
    /// Actually dispatches the given request to the appropriate queue and returns the result.
    ///
    /// The result is either a RESP response already marshalled into a byte buffer or an error.
    /// Note that application level errors have already been marshalled into RESP errors like
    /// "-CLIENT: ...." or "-SERVER: ....". These errors which are explicitly reported here always
    /// signal that the protocol or IO channel might be inconsistent and therefore that the
    /// underlying connection to the client should be terminated.
    ///
    /// If this command has been issued via a Connection, a reference to it can be passed in.
    /// Otherwise (which is most probably testing), None can be used. This connection is only used
    /// by built-in commands (QUIT and CLIENT) as it has to interact with the inner workings of
    /// the server.
    ///
    /// We could always check for these commands before dispatching, but this would have a direct
    /// impact on latency and these commands are rare and not performance critical at all. Therefore
    /// we delay checking for them as much as possible.
    ///
    /// # Example
    ///
    /// ```
    /// # use jupiter::builder::Builder;
    /// # use jupiter::request::Request;
    /// # use jupiter::platform::Platform;
    /// # use jupiter::commands::CommandDictionary;
    /// # use bytes::BytesMut;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// // Create a new platform with a commands dictionary and a SYS.COMMANDS command...
    /// let platform = Builder::new()
    ///                         .enable_server()
    ///                         .enable_commands()
    ///                         .enable_core_commands()
    ///                         .build()
    ///                         .await;
    ///
    /// // Obtain the dispatcher...
    /// let mut dispatcher = platform.require::<CommandDictionary>().dispatcher();
    ///
    /// // Create a "SYS.COMMANDS" request...
    /// let request = Request::example(vec!("SYS.COMMANDS"));
    /// // Actually invoke the command via the dispatcher...
    /// let result = dispatcher.invoke(request, None).await;
    /// // We expect to receive a PONG result for our PING command...
    /// assert_eq!(result.is_ok(), true);    
    ///
    /// // Create a "PING" request...
    /// let request = Request::example(vec!("PING"));
    /// // Actually invoke the command via the dispatcher...
    /// let result = dispatcher.invoke(request, None).await.unwrap();
    /// // We expect to receive a PONG result for our PING command...
    /// assert_eq!(std::str::from_utf8(&result[..]).unwrap(), "+PONG\r\n");
    /// # }
    ///
    /// ```
    pub async fn invoke(
        &mut self,
        request: Request,
        connection: Option<&Arc<Connection>>,
    ) -> Result<BytesMut, OutputError> {
        let response = Response::new();
        match self.commands.get_mut(request.command()) {
            Some((command, queue)) => {
                Dispatcher::invoke_command(command, queue, request, response).await
            }
            // We handle built-ins here, which is quite late. However, since these commands are
            // rare and not performance critical, we want them off of the hot path with the
            // additional cost of having to pass an inner data structure of the server (the
            // connection) around. Note that this isn't a performance penalty it just looks a bit
            // ugly from a software architecture perspective.
            _ => self.handle_built_in(request, response, connection).await,
        }
    }

    /// Tries to emulate commonly used Redis commands.
    ///
    /// This provides basic support for the following commands as these are used by some Redis
    /// client libraries like "jedis":
    /// * **QUIT**: Terminates the connection to the client
    /// * **CLIENT SETNAME**: Stores the name of the connected machine for this connection
    /// * **PING**: Creates a simple string reply (either PONG or the first parameter).
    async fn handle_built_in(
        &mut self,
        request: Request,
        mut response: Response,
        connection: Option<&Arc<Connection>>,
    ) -> Result<BytesMut, OutputError> {
        match request.command().to_uppercase().as_str() {
            "QUIT" => {
                if let Some(connection) = connection {
                    connection.quit();
                }
                response.ok()?;
            }
            "CLIENT" => {
                if request.str_parameter(0)?.to_uppercase() == "SETNAME" {
                    if let Some(connection) = connection {
                        connection.set_name(request.str_parameter(1)?);
                    }
                }
                response.ok()?;
            }
            "PING" => {
                if request.parameter_count() > 0 {
                    response.bulk(request.str_parameter(0)?)?;
                } else {
                    response.simple("PONG")?;
                }
            }
            _ => response.error(&format!("CLIENT: Unknown command: {}", request.command()))?,
        }

        Ok(response.complete()?)
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

        let watch = Instant::now();
        if queue.send(task).await.is_err() {
            Err(OutputError::ProtocolError(anyhow!(
                "Failed to submit command into queue!"
            )))
        } else {
            match promise.await {
                Ok(result) => {
                    command.call_metrics.add(watch.elapsed().as_micros() as i32);
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
