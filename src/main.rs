use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn main() {
    // Create the TCP listener, bound to the
    // standard Redis port.
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    // Process each incoming connection.
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                // The connection is valid, handle it.
                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

// The main entry point for valid TCP connections.
fn handle_connection(stream: &mut TcpStream) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    // Read from the stream into the buffer.
    stream.read(&mut buffer).unwrap();

    // Hardcoded response.
    let response = "+PONG\r\n";

    // Write the response to the stream.
    stream.write(response.as_bytes()).unwrap();

    // Make sure the stream is flushed.
    stream.flush().unwrap();
}
