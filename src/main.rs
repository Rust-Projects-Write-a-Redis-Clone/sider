use crate::resp::{bytes_to_resp, Resp};
use crate::server::process_request;
use crate::storage::Storage;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod resp;
mod resp_result;
mod server;
mod storage;
mod storage_result;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create the TCP listener, bound to the
    // standard Redis port.
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    // Create a storage and protect it against concurrency issues.
    let storage = Arc::new(Mutex::new(Storage::new()));

    loop {
        // Process each incoming connection.
        match listener.accept().await {
            // The connection is valid, handle it.
            Ok((stream, _)) => {
                // Spawn a task to take care of this connection.
                tokio::spawn(handle_connection(stream, storage.clone()));
            }
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        }
    }
}

// The main entry point for valid TCP connections.
async fn handle_connection(mut stream: TcpStream, storage: Arc<Mutex<Storage>>) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    loop {
        // Read from the stream into the buffer.
        match stream.read(&mut buffer).await {
            // If the stream returned some data,
            // process the request.
            Ok(size) if size != 0 => {
                // Initialise the index to start at the
                // beginning of the buffer.
                let mut index: usize = 0;

                // Process the bytes in the buffer according to
                // the content and extract the request. Update the index.
                let request = match bytes_to_resp(&buffer[..size], &mut index) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                };

                // Process the request.
                let response = match process_request(request, storage.clone()) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Error parsing command: {}", e);
                        return;
                    }
                };

                // Write the response to the stream.
                if let Err(e) = stream.write_all(response.to_string().as_bytes()).await {
                    eprintln!("Error writing to socket: {}", e);
                }
            }
            // If the stream returned no data
            // the connection has been closed.
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
