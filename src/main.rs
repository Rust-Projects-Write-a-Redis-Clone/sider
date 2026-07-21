use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod resp;
mod resp_result;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Create the TCP listener, bound to the
    // standard Redis port.
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        // Process each incoming connection.
        match listener.accept().await {
            // The connection is valid, handle it.
            Ok((stream, _)) => {
                // Spawn a task to take care of this connection.
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        }
    }
}

// The main entry point for valid TCP connections.
async fn handle_connection(mut stream: TcpStream) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    loop {
        // Read from the stream into the buffer.
        match stream.read(&mut buffer).await {
            // If the stream returned some data,
            // process the request.
            Ok(size) if size != 0 => {
                // Hardcoded response.
                let response = "+PONG\r\n";

                // Write the response to the stream.
                if let Err(e) = stream.write_all(response.as_bytes()).await {
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
