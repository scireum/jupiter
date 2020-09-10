use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::StreamExt;

use crate::commands::CommandDictionary;
use crate::flag::Flag;
use crate::platform::Platform;
use crate::request::Request;
use crate::watch::{Average, Watch};

const READ_WAIT_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_BUFFER_SIZE: usize = 8192;

struct Connection {
    peer_address: String,
    commands: Average,
    active: Flag,
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_address == other.peer_address
    }
}

impl Connection {
    fn is_active(&self) -> bool {
        self.active.read()
    }
}

pub struct ConnectionInfo {
    pub peer_address: String,
    pub commands: Average,
}

pub struct Server {
    running: Flag,
    current_address: Mutex<Option<String>>,
    platform: Arc<Platform>,
    connections: Mutex<Vec<Arc<Connection>>>,
}

impl Server {
    pub fn install(platform: &Arc<Platform>) -> Arc<Self> {
        let server = Arc::new(Server {
            running: Flag::new(false),
            current_address: Mutex::new(None),
            platform: platform.clone(),
            connections: Mutex::new(Vec::new()),
        });

        platform.register::<Server>(server.clone());

        server
    }

    pub fn connections(&self) -> Vec<ConnectionInfo> {
        let mut result = Vec::new();
        for connection in self.connections.lock().unwrap().iter() {
            result.push(ConnectionInfo {
                peer_address: connection.peer_address.clone(),
                commands: connection.commands.clone(),
            });
        }

        result
    }

    pub fn kill(&self, peer_address: &str) -> bool {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .find(|c| &c.peer_address == peer_address)
            .map(|c| c.active.change(false))
            .is_some()
    }

    fn add_connection(&self, connection: Arc<Connection>) {
        self.connections.lock().unwrap().push(connection);
    }

    fn remove_connection(&self, connection: Arc<Connection>) {
        let mut mut_connections = self.connections.lock().unwrap();
        if let Some(index) = mut_connections
            .iter()
            .position(|other| *other == connection)
        {
            mut_connections.remove(index);
        }
    }

    fn is_running(&self) -> bool {
        self.running.read()
    }

    fn address(&self) -> String {
        "0.0.0.0:2410".to_owned()
        // format!("{}:{}",
        //         self.config.string("server.host", "0.0.0.0"),
        //         self.config.checked_number("server.port",
        //                                    |port| port >= 0 && port <= std::u16::MAX as i64,
        //                                    2410))
    }

    pub fn fork(server: &Arc<Server>) {
        let cloned_server = server.clone();
        tokio::spawn(async move {
            cloned_server.event_loop().await;
        });
    }

    pub async fn fork_and_await(server: &Arc<Server>) {
        Server::fork(server);

        while !server.running.read() {
            tokio::time::delay_for(Duration::from_secs(1)).await;
        }
    }

    pub async fn event_loop(&self) {
        let mut address = String::new();

        while self.platform.is_running.read() {
            if !self.is_running() {
                address = self.address();
                self.running.change(true);
            }

            if let Ok(mut listener) = TcpListener::bind(&address).await {
                log::info!("Opened server socket on {}...", &address);
                *self.current_address.lock().unwrap() = Some(address.clone());
                self.server_loop(&mut listener).await;
                log::info!("Closing server socket on {}.", &address);
            } else {
                log::error!(
                    "Cannot open server address: {}... Retrying in 5s.",
                    &address
                );
                tokio::time::delay_for(Duration::from_secs(5)).await;
            }
        }
    }

    async fn server_loop(&self, listener: &mut TcpListener) {
        let mut incoming = listener.incoming();
        let mut kill_flag = self.platform.is_running.listener();
        // let mut config_changed_flag = platform.config().on_change();

        while self.platform.is_running.read() && self.is_running() {
            tokio::select! {
                stream = incoming.next() => {
                    if let Some(Ok(stream)) = stream {
                        self.handle_new_connection(stream);
                    } else {
                        return;
                    }
                }
                // _ = config_changed_flag.recv() => {
                //      let new_address = server.address();
                //      if let Some(current_address) = &*server.current_address.lock().unwrap() {
                //         if current_address != &new_address {
                //             info!("Server address has changed. Restarting server socket...");
                //             server.running.store(false, Ordering::SeqCst);
                //             return;
                //         }
                //      }
                // }
                _ = kill_flag.expect() => { return; }
            }
        }
    }

    fn handle_new_connection(&self, stream: TcpStream) {
        let connection = Arc::new(Connection {
            peer_address: stream
                .peer_addr()
                .map(|addr| addr.to_string())
                .unwrap_or("<unknown>".to_string()),
            commands: Average::new(),
            active: Flag::new(true),
        });

        let platform = self.platform.clone();
        tokio::spawn(async move {
            let server = platform.require::<Server>();
            log::info!("Opened connection from {}...", connection.peer_address);
            server.add_connection(connection.clone());
            if let Err(error) = Server::client_loop(platform, connection.clone(), stream).await {
                log::warn!(
                    "An IO error occurred in connection {}: {}",
                    connection.peer_address,
                    error
                );
            }
            log::info!("Closing connection to {}...", connection.peer_address);
            server.remove_connection(connection);
        });
    }

    async fn client_loop(
        platform: Arc<Platform>,
        connection: Arc<Connection>,
        mut stream: TcpStream,
    ) -> anyhow::Result<()> {
        let mut dispatcher = platform.require::<CommandDictionary>().dispatcher();
        let mut input_buffer = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
        let (mut reader, mut writer) = stream.split();

        while platform.is_running.read() && connection.is_active() {
            match tokio::time::timeout(READ_WAIT_TIMEOUT, reader.read_buf(&mut input_buffer)).await
            {
                Ok(Ok(bytes_read)) if bytes_read > 0 => match Request::parse(&mut input_buffer) {
                    Ok(Some(request)) => {
                        let watch = Watch::start();
                        match dispatcher.invoke(request).await {
                            Ok(response_data) => {
                                connection.commands.add(watch.micros());
                                writer.write_all(response_data.as_ref()).await?;
                                writer.flush().await?;
                            }
                            Err(error) => {
                                let error_message = error
                                    .to_string()
                                    .replace("\r", " ")
                                    .to_string()
                                    .replace("\n", " ");
                                writer
                                    .write_all(format!("-SERVER: {}\r\n", error_message).as_bytes())
                                    .await?;
                                writer.flush().await?;
                                return Ok(());
                            }
                        }

                        if input_buffer.len() == 0 && input_buffer.capacity() > DEFAULT_BUFFER_SIZE
                        {
                            input_buffer = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
                        }
                    }
                    Err(error) => return Err(error),
                    _ => (),
                },
                Ok(Ok(0)) => return Ok(()),
                Ok(Err(error)) => {
                    return Err(anyhow::anyhow!(
                        "An error occurred while reading from the client: {}",
                        error
                    ));
                }
                _ => (),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use crate::commands::CommandDictionary;
    use crate::ping;
    use crate::platform::Platform;
    use crate::server::Server;
    use crate::watch::Watch;

    #[test]
    fn commands_can_be_issued() {
        tokio_test::block_on(async {
            let platform = Platform::new();
            CommandDictionary::install(&platform);
            ping::install(&platform);
            Server::fork_and_await(&Server::install(&platform)).await;

            tokio::task::spawn_blocking(move || {
                let client = redis::Client::open("redis://127.0.0.1:2410/").unwrap();
                let mut con = client
                    .get_connection_with_timeout(Duration::from_secs(10))
                    .unwrap();
                let watch = Watch::start();
                let result: String = redis::cmd("PING").query(&mut con).unwrap();
                assert_eq!(result, "PONG");
                println!("Request took: {}", watch);

                platform.is_running.change(false);
            })
            .await
            .unwrap();
        });
    }
}
