use crate::request::Request;
use crate::resp::{bytes_to_resp, RESP};
use crate::storage::Storage;
use connection::ConnectionMessage;
use server_result::ServerMessage;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc,
};

mod connection;
mod request;
mod resp;
mod resp_result;
mod server;
mod server_result;
mod set;
mod storage;
mod storage_result;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create the TCP listener, bound to the
    // standard Redis port.
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // Create a storage and protect it against concurrency issues.
    let storage = Arc::new(Mutex::new(Storage::new()));

    // Create a timer that expires every 10 milliseconds.
    let mut interval_timer = tokio::time::interval(Duration::from_millis(10));

    // Create the MPSC channel to communicate with the server.
    let (server_sender, _) = mpsc::channel::<ConnectionMessage>(32);

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
                        println!("Error: {}", e);
                        continue;
                    }
                }
            }

            // Process the expired timer.
            _ = interval_timer.tick() => {
                tokio::spawn(expire_keys(storage.clone()));
            }
        }
    }
}

// The main entry point for valid TCP connections.
async fn handle_connection(mut stream: TcpStream, server_sender: mpsc::Sender<ConnectionMessage>) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    // Create the MPSC channel to communicate with the connection.
    let (connection_sender, _) = mpsc::channel::<ServerMessage>(32);

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
        }
    }
}

// The entry point for key expiry operations.
async fn expire_keys(storage: Arc<Mutex<Storage>>) {
    // Acquire a lock on the storage.
    let mut guard = storage.lock().unwrap();

    // Trigger the expiry process.
    guard.expire_keys();
}
