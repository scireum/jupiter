//! Contains the server component of Jupiter.
//!
//! Opens a server-socket on the specified port (**server.port** in the config or 2410 as fallback)
//! and binds it to the selected IP (**server.host** in the config or 0.0.0.0 as fallback). Each
//! incoming client is expected to send RESP requests and will be provided with the appropriate
//! responses.
//!
//! Note that in order to achieve zero downtime / ultra high availability demands, the sever will
//! periodically try to bind the socket to the selected port, therefore an "new" instance can
//! be started and the "old" once can bleed out and the port will be "handed through" with minimal
//! downtime. Also, this will listen to change events of the config and will relocate to another
//! port or host if changed.
//!
//! # Example
//!
//! ```no_run
//! use jupiter::builder::Builder;
//! use tokio::time::Duration;
//! use jupiter::config::Config;
//! use jupiter::server::Server;
//!
//! #[tokio::main]
//! async fn main() {
//!     //  Setup and create a platform...
//! let platform = Builder::new().enable_all().build().await;
//!
//!     // Specify a minimal config so that we run on a different port than a
//!     // production instance.
//!     platform.require::<Config>().load_from_string("
//!         server:
//!             port: 1503
//!     ", None);
//!
//!     // Run the platform...
//!     platform.require::<Server>().event_loop().await;
//! }
//! ```
use crate::average::Average;
use crate::spawn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::commands::CommandDictionary;
use crate::config::Config;
use crate::platform::Platform;
use crate::request::Request;
use crate::response::OutputError;
use arc_swap::ArcSwap;
use std::sync::Mutex;
use tokio::net::tcp::WriteHalf;

/// Specifies the timeout when waiting for incoming data on a client connection.
///
/// When waiting for incoming data we need to interrupt this every once in a while to check
/// if either the platform is being shut down or if the connection was killed manually.
const READ_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// Determines the pre-allocated receive buffer size for incoming requests. Most requests will /
/// should fit into this buffer so that no additional allocations are required when handling a
/// command.
const DEFAULT_BUFFER_SIZE: usize = 8192;

/// Specifies the timeout when waiting for a new incoming connection.
///
/// When waiting for a new connection we need to interrupt this every once in a while so that
/// we can check if the platform has been shut down.
const CONNECT_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

/// Represents a client connection.
pub struct Connection {
    peer_address: String,
    active: AtomicBool,
    commands: Average,
    name: ArcSwap<Option<String>>,
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_address == other.peer_address
    }
}

impl Connection {
    /// Determines if the connection is active or if a termination has been requested.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Terminates the connection.
    pub fn quit(&self) {
        self.active.store(false, Ordering::Release);
    }

    /// Stores the name of the connected client.
    pub fn set_name(&self, name: &str) {
        self.name.store(Arc::new(Some(name.to_owned())));
    }

    /// Retrieves the name of the connected client (if known).
    pub fn get_name(&self) -> Arc<Option<String>> {
        self.name.load().clone()
    }

    /// Provides a an average recording the runtime of commands.
    pub fn commands(&self) -> &Average {
        &self.commands
    }
}

/// Provides some metadata for a client connection.
pub struct ConnectionInfo {
    /// Contains the peer address of the client being connected.
    pub peer_address: String,

    /// Contains the name of the connected client.
    pub client: String,

    /// Contains the number of commands which have been received along
    /// with their average runtime.
    pub commands: Average,
}

/// Represents a server which manages all TCP connections.
pub struct Server {
    running: AtomicBool,
    current_address: Mutex<Option<String>>,
    platform: Arc<Platform>,
    connections: Mutex<Vec<Arc<Connection>>>,
}

impl Server {
    /// Creates and installs a **Server** into the given **Platform**.
    ///
    /// Note that this is called by the [Builder](crate::builder::Builder) unless disabled.
    ///
    /// Also note, that this will not technically start the server. This has to be done manually
    /// via [event_loop](Server::event_loop) as it is most probable done in the main thread.
    pub fn install(platform: &Arc<Platform>) -> Arc<Self> {
        let server = Arc::new(Server {
            running: AtomicBool::new(false),
            current_address: Mutex::new(None),
            platform: platform.clone(),
            connections: Mutex::new(Vec::new()),
        });

        platform.register::<Server>(server.clone());

        server
    }

    /// Lists all currently active connections.
    pub fn connections(&self) -> Vec<ConnectionInfo> {
        let mut result = Vec::new();
        for connection in self.connections.lock().unwrap().iter() {
            result.push(ConnectionInfo {
                peer_address: connection.peer_address.clone(),
                commands: connection.commands.clone(),
                client: connection
                    .name
                    .load()
                    .as_deref()
                    .map(|name| name.to_string())
                    .unwrap_or_else(|| "".to_string()),
            });
        }

        result
    }

    /// Kills the connection of the given peer address.
    pub fn kill(&self, peer_address: &str) -> bool {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.peer_address == peer_address)
            .map(|c| c.active.store(false, Ordering::Release))
            .is_some()
    }

    /// Adds a newly created client connection.
    ///
    /// Note that this involves locking a **Mutex**. However, we expect our clients to use
    /// connection pooling, so that only a few rather long running connections are present.
    fn add_connection(&self, connection: Arc<Connection>) {
        self.connections.lock().unwrap().push(connection);
    }

    /// Removes a connection after it has been closed by either side.
    fn remove_connection(&self, connection: Arc<Connection>) {
        let mut mut_connections = self.connections.lock().unwrap();
        if let Some(index) = mut_connections
            .iter()
            .position(|other| *other == connection)
        {
            let _ = mut_connections.remove(index);
        }
    }

    /// Determines if the server socket should keep listening for incoming connections.
    ///
    /// In contrast to **Platform::is_running** this is not used to control the shutdown of the
    /// server. Rather we toggle this flag to false if a config and therefore address change was
    /// detected. This way **server_loop** will exit and a new server socket for the appropriate
    /// address will be setup by the **event_loop**.
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Determines the server address based on the current configuration.
    ///
    /// If no, an invalid or a partial config is present, fallback values are used. By default we
    /// use port 2410 and bind to "0.0.0.0".
    fn address(&self) -> String {
        self.platform
            .find::<Config>()
            .map(|config| {
                let handle = config.current();
                format!(
                    "{}:{}",
                    handle.config()["server"]["host"]
                        .as_str()
                        .unwrap_or("0.0.0.0"),
                    handle.config()["server"]["port"]
                        .as_i64()
                        .filter(|port| port > &0 && port <= &(u16::MAX as i64))
                        .unwrap_or(2410)
                )
            })
            .unwrap_or_else(|| "0.0.0.0:2410".to_owned())
    }

    /// Starts the event loop in a separate thread.
    ///
    /// This is most probably used by test scenarios where the tests itself run in the main thread.
    pub fn fork(server: &Arc<Server>) {
        let cloned_server = server.clone();
        spawn!(async move {
            cloned_server.event_loop().await;
        });
    }

    /// Starts the event loop in a separate thread and waits until the server is up and running.
    ///
    /// Just like **fork** this is intended to be used in test environments.
    pub async fn fork_and_await(server: &Arc<Server>) {
        Server::fork(server);

        while !server.is_running() {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Tries to open a server socket on the specified address to serve incoming client connections.
    ///
    /// The task of this loop is to bind the server socket to the specified address. Once this was
    /// successful, we enter the [server_loop](Server::server_loop) to actually handle incoming
    /// connections. Once this loop returns, either the platform is no longer running and we should
    /// exit, or the config has changed and we should try to bind the server to the new address.
    pub async fn event_loop(&self) {
        let mut address = String::new();
        let mut last_bind_error_reported = Instant::now();

        while self.platform.is_running() {
            // If the sever is started for the first time or if it has been restarted due to a
            // config change, we need to reload the address...
            if !self.is_running() {
                address = self.address();
                self.running.store(true, Ordering::Release);
            }

            // Bind and hopefully enter the server_loop...
            if let Ok(mut listener) = TcpListener::bind(&address).await {
                log::info!("Opened server socket on {}...", &address);
                *self.current_address.lock().unwrap() = Some(address.clone());
                self.server_loop(&mut listener).await;
                log::info!("Closing server socket on {}.", &address);
            } else {
                // If we were unable to bind to the server, we log this every once in a while
                // (every 5s). Otherwise we would jam the log as re retry every 500ms.
                if Instant::now()
                    .duration_since(last_bind_error_reported)
                    .as_secs()
                    > 5
                {
                    log::error!(
                        "Cannot open server address: {}. Retrying every 500ms...",
                        &address
                    );
                    last_bind_error_reported = Instant::now();
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    /// Runs the main server loop which processes incoming connections.
    ///
    /// This also listens on config changes and exits to the event_loop if necessary (server
    /// address changed...).   
    async fn server_loop(&self, listener: &mut TcpListener) {
        let mut config_changed_flag = self.platform.require::<Config>().notifier();

        while self.platform.is_running() && self.is_running() {
            tokio::select! {
                // We use a timeout here so that the while condition (esp. platform.is_running())
                // is checked every once in a while...
                timeout_stream = tokio::time::timeout(CONNECT_WAIT_TIMEOUT, listener.accept()) => {
                    // We're only interested in a positive result here, as an Err simply indicates
                    // that the timeout was hit - in this case we do nothing as the while condition
                    // is all the needs to be checked...
                    if let Ok(stream) = timeout_stream {
                        // If a stream is present, we treat this as new connection and eventually
                        // start a client_loop for it...
                        if let Ok((stream, _)) = stream {
                            self.handle_new_connection(stream);
                        } else {
                            // Otherwise the socket has been closed therefore we exit to the
                            // event_loop which will either complete exit or try to re-create
                            // the socket.
                            return;
                        }
                    }
                }
                _ = config_changed_flag.recv() => {
                    // If the config was changed, we need to check if the address itself changed...
                    let new_address = self.address();
                    if let Some(current_address) = &*self.current_address.lock().unwrap() {
                       if current_address != &new_address {
                           log::info!("Server address has changed. Restarting server socket...");

                           // Force the event_loop to re-evaluate the expected server address...
                           self.running.store(false, Ordering::Release);

                           // Return to event_loop so that the server socket is re-created...
                           return;
                       }
                    }
               }
            }
        }
    }

    /// Handles a new incoming connection.
    ///
    /// This will register the connection in the list of client connections and then fork a
    /// "thread" which mainly simply executes the **resp_protocol_loop** for this connection.
    fn handle_new_connection(&self, stream: TcpStream) {
        let platform = self.platform.clone();
        spawn!(async move {
            // Mark the connection as nodelay, as we already optimize all writes as far as possible.
            let _ = stream.set_nodelay(true);

            // Register the new connection to that the can report it in the maintenance utilities...
            let server = platform.require::<Server>();
            let connection = Arc::new(Connection {
                peer_address: stream
                    .peer_addr()
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_owned()),
                active: AtomicBool::new(true),
                commands: Average::new(),
                name: ArcSwap::new(Arc::new(None)),
            });
            log::debug!("Opened connection from {}...", connection.peer_address);
            server.add_connection(connection.clone());

            // Executes the client loop for this connection....
            if let Err(error) = resp_protocol_loop(platform, connection.clone(), stream).await {
                log::debug!(
                    "An IO error occurred in connection {}: {}",
                    connection.peer_address,
                    error
                );
            }

            // Removes the connection as it has been closed...
            log::debug!("Closing connection to {}...", connection.peer_address);
            server.remove_connection(connection);
        });
    }
}

/// Executed per client to process incoming RESP commands.
async fn resp_protocol_loop(
    platform: Arc<Platform>,
    connection: Arc<Connection>,
    mut stream: TcpStream,
) -> anyhow::Result<()> {
    // Acquire a dispatcher to have a lock free view of all known commands...
    let mut dispatcher = platform.require::<CommandDictionary>().dispatcher();
    // Pre-allocate a buffer for incoming requests. This will only be re-allocated if a request
    // was larger than 8 KB...
    let mut input_buffer = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
    let (mut reader, mut writer) = stream.split();

    while platform.is_running() && connection.is_active() {
        // We apply a timeout here, so that the condition of the while loop is checked every once in a while...
        match tokio::time::timeout(READ_WAIT_TIMEOUT, reader.read_buf(&mut input_buffer)).await {
            // Best case, we read some bytes from the socket..
            Ok(Ok(bytes_read)) if bytes_read > 0 => match Request::parse(&input_buffer) {
                // aaand we were able to parse a RESP Request from the given data in the buffer...
                Ok(Some(request)) => {
                    log::debug!("Received {}", request.command());

                    let watch = Instant::now();
                    let request_len = request.len();
                    match dispatcher.invoke(request, Some(&connection)).await {
                        Ok(response_data) => {
                            // We only update the monitoring infos if the command was successfully executed...
                            connection.commands.add(watch.elapsed().as_micros() as i32);
                            writer.write_all(response_data.as_ref()).await?;
                            writer.flush().await?;
                        }
                        Err(error) => {
                            handle_error(error, &mut writer).await?;

                            // Return from the loop to effectively close the connection...
                            return Ok(());
                        }
                    }

                    input_buffer = clear_input_buffer(input_buffer, request_len);
                }
                Err(error) => {
                    handle_protocol_error(error, &mut writer).await?;

                    // Return from the loop to effectively close the connection...
                    return Ok(());
                }
                // A partial RESP request is present - do nothing so that we keep on reading...
                _ => (),
            },

            // Reading from the client return a zero length result -> the client wants to close the connection.
            // We therefore return from this loop.
            Ok(Ok(0)) => return Ok(()),

            // An IO error occurred while reading - notify our called and abort...
            Ok(Err(error)) => {
                return Err(anyhow::anyhow!(
                    "An error occurred while reading from the client: {}",
                    error
                ));
            }

            // The timeout elapsed before any data was read => do nothing, all we want to do is to re-evaluate
            // our while condition anyway...
            _ => (),
        }
    }

    Ok(())
}

async fn handle_error(error: OutputError, writer: &mut WriteHalf<'_>) -> anyhow::Result<()> {
    // OutputErrors are the only kind of errors which are escalated up to this loop.
    // Everything else is handled internally by the dispatcher. The reason for this
    // behaviour is that an OutputError indicates that either an IO error or a protocol
    // error occurred - therefore we rather close this connection as it might be in an
    // inconsistent state...

    // Try to send an error message if the protocol is malformed. In case of an
    // IO error there is no point in sending yet another message, as will most
    // probably fail anyway, so we just close the connection...
    if let OutputError::ProtocolError(error) = error {
        let error_message = error.to_string().replace(['\r', '\n'], " ");
        writer
            .write_all(format!("-SERVER: {}\r\n", error_message).as_bytes())
            .await?;
        writer.flush().await?;
    }

    Ok(())
}

async fn handle_protocol_error(
    error: anyhow::Error,
    writer: &mut WriteHalf<'_>,
) -> anyhow::Result<()> {
    // We received an invalid/malformed RESP request - send an appropriate error message
    // and close the connection...
    writer
        .write_all(
            format!(
                "-CLIENT: A malformed RESP request was received: {}\r\n",
                error
            )
            .as_bytes(),
        )
        .await?;
    writer.flush().await?;
    Ok(())
}

fn clear_input_buffer(mut input_buffer: BytesMut, request_len: usize) -> BytesMut {
    // If the input buffer has grown in order to accommodate a large request, we shrink
    // it here again. Otherwise we clear the buffer to make room for the next request..
    if input_buffer.capacity() > DEFAULT_BUFFER_SIZE || input_buffer.len() > request_len {
        let previous_buffer = input_buffer;
        input_buffer = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);

        // If the previous buffer contains trailing data, we transfer it to the
        // new buffer.
        if previous_buffer.len() > request_len {
            input_buffer.put_slice(&previous_buffer[request_len..]);
        }
    } else {
        input_buffer.truncate(0);
    }

    input_buffer
}

#[cfg(test)]
mod tests {
    use crate::builder::Builder;
    use crate::config::Config;
    use crate::server::Server;
    use crate::testing::{query_redis_async, test_async};

    #[test]
    fn integration_test() {
        // We want exclusive access to both, the test-repo and the 1503 port on which we fire up
        // a test-server for our integration tests...
        log::info!("Acquiring shared resources...");
        let _guard = crate::testing::SHARED_TEST_RESOURCES.lock().unwrap();
        log::info!("Successfully acquired shared resources.");

        test_async(async {
            //  Setup and create a platform...
            let platform = Builder::new().enable_all().disable_config().build().await;
            let _ = crate::config::install(platform.clone(), false).await;

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
            Server::fork_and_await(&platform.require::<Server>()).await;

            // Fire up a redis client and invoke our PING command...
            let result = query_redis_async(|con| redis::cmd("PING").query::<String>(con))
                .await
                .unwrap();
            assert_eq!(result, "PONG");

            platform.terminate();
        });
    }
}
