use crate::resp::bytes_to_resp;
use crate::server::handshake;
use crate::server_result::{ServerMessage, ServerValue};
use crate::{request::Request, server_result::ServerError};
use std::fmt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc,
};

#[derive(Debug)]
pub enum ConnectionError {
    ServerError(ServerError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::ServerError(e) => {
                write!(f, "{}", format!("Server error: {}", e))
            }
        }
    }
}

#[derive(Debug)]
pub enum ConnectionMessage {
    Request(Request),
}

// Run a task that listens to the bound TCP port
// for incoming connection from clients and
// spawns a connection handler for each one of them.
pub async fn run_listener(host: String, port: u16, server_sender: mpsc::Sender<ConnectionMessage>) {
    // Create the TCP listener, bound to the given port.
    let listener = TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap();

    loop {
        // Process each incoming connection.
        tokio::select! {
            // Process a new connection.
            connection = listener.accept() => {
                match connection {
                    // The connection is valid, handle it.
                    Ok((stream, _)) => {
                        // Spawn a task to take care of this connection.
                        tokio::spawn(handle_connection(stream, server_sender.clone()));
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        continue;
                    }
                }
            }
        }
    }
}

// Initialise and manage a connection with the master.
pub async fn run_master_listener(
    host: String,
    port: u16,
    server_sender: mpsc::Sender<ConnectionMessage>,
) {
    // Actively connect to the master
    let stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .unwrap();

    // Run the handshake protocol
    if let Err(e) = handshake().await {
        eprintln!("Handshake failed: {}", e.to_string());
        std::process::exit(1);
    }

    // Spawn a task to take care of this connection.
    tokio::spawn(async move { handle_connection(stream, server_sender.clone()).await });
}

// The main entry point for valid TCP connections.
pub async fn handle_connection(
    mut stream: TcpStream,
    server_sender: mpsc::Sender<ConnectionMessage>,
) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    // Create the MPSC channel to communicate with the connection.
    let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

    loop {
        select! {
            // Data is available in the stream, read it.
            result = stream.read(&mut buffer) => {
                // Check if the incoming data is valid
                // and act accordingly.
                match result {
                    // If the stream returned some data,
                    // process the request.
                    Ok(size) if size != 0 => {
                        // Initialise the index to start at the
                        // beginning of the buffer.
                        let mut index: usize = 0;

                        // Process the bytes in the buffer according to
                        // the content and extract the request. Update the index.
                        let resp = match bytes_to_resp(&buffer[..size].to_vec(), &mut index) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Error: {}", e);
                                return;
                            }
                        };

                        // Create the request.
                        let request = Request {
                            value: resp,
                            sender: connection_sender.clone(),
                        };

                        // Send the request to the server.
                        match server_sender.send(ConnectionMessage::Request(request)).await {
                            Ok(()) => {},
                            Err(e) => {
                                eprintln!("Error sending request: {}", e);
                                return;
                            }
                        }
                    }
                    // If the stream returned no data
                    // the connection has bee closed.
                    Ok(_) => {
                        println!("Connection closed");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                        break;
                    }
                }
            }

            // A response arrived from the server.
            Some(response) = connection_receiver.recv() => {
                let _ = match response {
                    ServerMessage::Data(ServerValue::RESP(v)) => stream.write_all(v.to_string().as_bytes()).await,
                    ServerMessage::Data(ServerValue::None) => Ok(()),
                    ServerMessage::Error(e) => {
                        eprintln!("Error: {}", ConnectionError::ServerError(e));
                        return;
                    }
                };
            }

        }
    }
}
