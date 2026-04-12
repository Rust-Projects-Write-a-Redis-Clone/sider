use crate::resp::{bytes_to_resp, RESP};
use crate::server::{handshake, ServerInfo};
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
    CannotReadFromStream(String),
    CannotWriteToStream(String),
    MalformedRESP(String),
    RequestFailed(String, String),
    ServerError(ServerError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::CannotReadFromStream(string) => {
                write!(f, "Cannot read from stream: {}.", string)
            }
            ConnectionError::CannotWriteToStream(string) => {
                write!(f, "Cannot write to stream: {}.", string)
            }
            ConnectionError::MalformedRESP(string) => {
                write!(f, "Cannot convert bytes to RESP: {}.", string)
            }
            ConnectionError::RequestFailed(request, e) => {
                write!(f, "Request {} failed: {}.", request, e)
            }
            ConnectionError::ServerError(e) => {
                write!(f, "{}", format!("Server error: {}", e))
            }
        }
    }
}

type ConnectionResult<T> = Result<T, ConnectionError>;

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
    info: &ServerInfo,
    server_sender: mpsc::Sender<ConnectionMessage>,
) {
    // Actively connect to the master
    let mut stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .unwrap();

    // Run the handshake protocol
    if let Err(e) = handshake(&mut stream, info).await {
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
                    ServerMessage::Data(ServerValue::Binary(data)) => stream.write_all(&data).await,
                    ServerMessage::Error(e) => {
                        eprintln!("Error: {}", ConnectionError::ServerError(e));
                        return;
                    }
                };
            }

        }
    }
}

// Write RESP data to the stream.
pub async fn stream_write_resp(stream: &mut TcpStream, data: &RESP) -> ConnectionResult<usize> {
    // Convert the RESP message to a String.
    let string_data = data.to_string();

    // Convert the String into bytes.
    let bytes = string_data.as_bytes();

    // Write the bytes to the TCP stream.
    match stream.write_all(bytes).await {
        Ok(_) => Ok(bytes.len()),
        Err(e) => Err(ConnectionError::CannotWriteToStream(e.to_string())),
    }
}

// Read RESP data from the stream.
async fn stream_read_resp(stream: &mut TcpStream, buffer: &mut [u8]) -> ConnectionResult<RESP> {
    // Read bytes from the TCP stream.
    match stream.read(buffer).await {
        Ok(size) => Ok(size),
        Err(e) => Err(ConnectionError::CannotReadFromStream(e.to_string())),
    }?;

    // Set the index to start reading from the first byte.
    let mut index: usize = 0;

    // Convert bytes to RESP.
    bytes_to_resp(&buffer, &mut index).map_err(|e| ConnectionError::MalformedRESP(e.to_string()))
}

// Write a RESP request to the stream and read the RESP response.
pub async fn stream_send_receive_resp(
    stream: &mut TcpStream,
    data: &RESP,
    buffer: &mut [u8],
) -> ConnectionResult<RESP> {
    // Write the RESP command to the stream.
    stream_write_resp(stream, &data)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))?;

    // Read the response in RESP format from the stream.
    stream_read_resp(stream, buffer)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))
}
