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
//! use apollo_framework::config::Config;
//! use apollo_framework::server::Server;
//! use jupiter::server::{RespPayload, resp_protocol_loop};
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
//!     platform.require::<Server<RespPayload>>().event_loop(&resp_protocol_loop).await;
//! }
//! ```
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::commands::CommandDictionary;
use crate::request::Request;
use crate::response::OutputError;
use apollo_framework::average::Average;
use apollo_framework::platform::Platform;
use apollo_framework::server::Connection;
use arc_swap::ArcSwap;
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

/// Contains the payload used to fulfill the RESP protocol.
///
/// Most notably, this contains the name as reported by the client as well as a counter to
/// measure the performance of each client (both used by SYS.CONNECTIONS).
pub struct RespPayload {
    commands: Average,
    name: ArcSwap<Option<String>>,
}

impl Clone for RespPayload {
    fn clone(&self) -> Self {
        RespPayload {
            commands: self.commands.clone(),
            name: ArcSwap::new(self.name.load().clone()),
        }
    }
}

impl Default for RespPayload {
    fn default() -> Self {
        RespPayload {
            commands: Average::new(),
            name: ArcSwap::new(Arc::new(None)),
        }
    }
}

impl RespPayload {
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

/// Executed per client to process incoming RESP commands.
pub async fn resp_protocol_loop(
    platform: Arc<Platform>,
    connection: Arc<Connection<RespPayload>>,
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
                            connection
                                .payload()
                                .commands
                                .add(watch.elapsed().as_micros() as i32);
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
        let error_message = error.to_string().replace("\r", " ").replace("\n", " ");
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
            let platform = Builder::new().enable_all().disable_config().build().await;
            let _ = apollo_framework::config::install(platform.clone(), false).await;

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

            // Fire up a redis client and invoke our PING command...
            let result = query_redis_async(|con| redis::cmd("PING").query::<String>(con))
                .await
                .unwrap();
            assert_eq!(result, "PONG");

            platform.terminate();
        });
    }
}
