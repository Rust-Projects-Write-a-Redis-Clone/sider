use crate::resp::{bytes_to_resp, Resp};
use crate::server::{handshake, ServerInfo};
use crate::server_result::{ServerMessage, ServerValue};
use crate::{request::Request, server_result::ServerError};
use std::fmt;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    select,
    sync::mpsc,
};

#[derive(Debug)]
pub enum ConnectionError {
    BufferTooSmall(usize),
    CannotReadFromStream(String),
    CannotWriteToStream(String),
    MalformedResp(String),
    RequestFailed(String, String),
    ServerError(ServerError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall(size) => {
                write!(f, "Buffer too small, size {}.", size)
            }
            Self::CannotReadFromStream(string) => {
                write!(f, "Cannot read from stream: {}.", string)
            }
            Self::CannotWriteToStream(string) => {
                write!(f, "Cannot write to stream: {}.", string)
            }
            Self::MalformedResp(string) => {
                write!(f, "Cannot convert bytes to RESP: {}.", string)
            }
            Self::RequestFailed(request, e) => {
                write!(f, "Request {} failed: {}.", request, e)
            }
            Self::ServerError(e) => {
                write!(f, "Server error: {}", e)
            }
        }
    }
}

type ConnectionResult<T> = Result<T, ConnectionError>;

#[derive(Debug)]
pub enum ConnectionMessage {
    NewClient(mpsc::Sender<ServerMessage>),
    BlockedClientTimeout { key: String, client_id: u64 },
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
                        // Mark the connection as coming from a client.
                        tokio::spawn(handle_connection(stream, server_sender.clone(), false));
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
        eprintln!("Handshake failed: {}", e);
        std::process::exit(1);
    }

    // Spawn a task to take care of this connection.
    // Mark the connection as coming from the master.
    tokio::spawn(async move { handle_connection(stream, server_sender, true).await });
}

// The main entry point for valid TCP connections.
pub async fn handle_connection(
    mut stream: TcpStream,
    server_sender: mpsc::Sender<ConnectionMessage>,
    master_connection: bool,
) {
    // Create a buffer to host incoming data.
    let mut buffer = [0; 512];

    // Create the MPSC channel to communicate with the connection.
    let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

    // Tell the server that a new client has connected.
    if let Err(e) = server_sender
        .send(ConnectionMessage::NewClient(connection_sender.clone()))
        .await
    {
        eprintln!("Error sending request: {}", e);
        return;
    }

    // This is the unique client ID that the
    // connection handler receives from the server.
    let client_id: u64 = match connection_receiver.recv().await {
        Some(ServerMessage::ClientInit(id)) => id,
        _ => {
            eprintln!("Error initialising client");
            return;
        }
    };

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

                        // Keep reading until we processed the full
                        // amount of data read from the stream.
                        while index < size {
                            let start: usize = index;

                            // Process the bytes in the buffer according to
                            // the content and extract the request. Update the index.
                            let resp = match bytes_to_resp(&buffer[..size], &mut index) {
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
                                binary: buffer[start..index].to_vec(),
                                client_id,
                                master_connection,
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

            // A response arrived from the server.
            Some(response) = connection_receiver.recv() => {
                let _ = match response {
                    // We shouldn't receive this
                    // message again from the server.
                    ServerMessage::ClientInit(_) => {
                        eprintln!("ClientInit received twice");
                        return;
                    },
                    ServerMessage::Data(ServerValue::Resp(v)) => stream.write_all(v.to_string().as_bytes()).await,
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
pub async fn stream_write_resp<W: AsyncWrite + Unpin>(
    stream: &mut W,
    data: &Resp,
) -> ConnectionResult<usize> {
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
async fn stream_read_resp<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
) -> ConnectionResult<Resp> {
    // Read bytes from the stream. Remember how many
    // bytes we actually got, so that we only hand the
    // parser the valid portion of the buffer.
    let size = stream
        .read(buffer)
        .await
        .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

    // Set the index to start reading from the first byte.
    let mut index: usize = 0;

    // Convert bytes to RESP, only looking at the slice
    // that actually holds data read from the stream.
    bytes_to_resp(&buffer[..size], &mut index)
        .map_err(|e| ConnectionError::MalformedResp(e.to_string()))
}

// Write a RESP request to the stream and read the RESP response.
pub async fn stream_send_receive_resp<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    data: &Resp,
    buffer: &mut [u8],
) -> ConnectionResult<Resp> {
    // Write the RESP command to the stream.
    stream_write_resp(stream, data)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))?;

    // Read the response in RESP format from the stream.
    stream_read_resp(stream, buffer)
        .await
        .map_err(|e| ConnectionError::RequestFailed(data.to_string(), e.to_string()))
}

// Read bytes from the stream until the \r\n is reached (including those bytes).
pub async fn stream_read_line<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
) -> ConnectionResult<usize> {
    let mut byte = [0; 1];
    let mut index = 0;

    // Track the byte we read in the previous iteration
    // so that we can detect the terminator \r\n without
    // ever indexing the buffer backwards. We initialise
    // it to 0, which is neither \r nor \n.
    let mut previous_byte: u8 = 0;

    // Keep reading every byte until we issue a break.
    loop {
        // Make sure we still have room in the buffer
        // before consuming another byte from the stream.
        if index >= buffer.len() {
            return Err(ConnectionError::BufferTooSmall(buffer.len()));
        }

        // Read one byte.
        stream
            .read_exact(&mut byte)
            .await
            .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

        // Store the byte in the buffer.
        buffer[index] = byte[0];

        // Check if we reached the end of the line.
        if byte[0] == b'\n' && previous_byte == b'\r' {
            break;
        }

        previous_byte = byte[0];
        index += 1;
    }

    Ok(index)
}

// Read the given amount of bytes from the stream
pub async fn stream_read_data_length<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut [u8],
    length: usize,
) -> ConnectionResult<usize> {
    // Make sure the caller's buffer can hold the
    // requested amount of bytes before we touch the
    // stream. This avoids panicking when slicing and
    // mirrors the check we use in stream_read_line.
    if length > buffer.len() {
        return Err(ConnectionError::BufferTooSmall(buffer.len()));
    }

    // Read exactly `length` bytes into the head of
    // the buffer. read_exact returns an error if the
    // stream closes before enough bytes are available.
    stream
        .read_exact(&mut buffer[..length])
        .await
        .map_err(|e| ConnectionError::CannotReadFromStream(e.to_string()))?;

    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    // Test that the function stream_read_line
    // correctly detects the \r\n, writes the bytes
    // (terminator included) into the buffer, and
    // returns the index of the final \n.
    async fn test_stream_read_line() {
        let mut stream: &[u8] = b"OK\r\n";
        let mut buffer = [0u8; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"OK\r\n");
        assert_eq!(index, 3);
    }

    #[tokio::test]
    // Test that the function stream_read_line does
    // not panic when the stream sends \n as the very
    // first byte. A lone \n is not preceded by a \r
    // and must not be interpreted as the terminator.
    async fn test_stream_read_line_leading_newline() {
        let mut stream: &[u8] = b"\n\r\n";
        let mut buffer = [0u8; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"\n\r\n");
        assert_eq!(index, 2);
    }

    #[tokio::test]
    // Test that the function stream_read_line returns
    // a BufferTooSmall error when the caller's buffer
    // cannot hold the whole line, instead of panicking
    // or writing past the end of the buffer.
    async fn test_stream_read_line_buffer_too_small() {
        let mut stream: &[u8] = b"HELLO\r\n";
        let mut buffer = [0u8; 3];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::BufferTooSmall(size)) => {
                assert_eq!(size, 3);
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Make sure the function stream_read_line doesn't
    // have any hardcoded values testing a different
    // use case.
    async fn test_stream_read_line_longer_string() {
        let mut stream: &[u8] = b"ECHO\r\n";
        let mut buffer = [0u8; 16];

        let index = stream_read_line(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(&buffer[..=index], b"ECHO\r\n");
        assert_eq!(index, 5);
    }

    #[tokio::test]
    // Test that the function stream_read_line returns
    // a CannotReadFromStream error when the stream is
    // empty and no bytes can be read at all.
    async fn test_stream_read_line_empty_stream() {
        let mut stream: &[u8] = b"";
        let mut buffer = [0u8; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Test that the function stream_read_line returns
    // a CannotReadFromStream error when the stream
    // ends before the terminator \r\n is reached.
    async fn test_stream_read_line_no_separator() {
        let mut stream: &[u8] = b"OK";
        let mut buffer = [0u8; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Test that the function stream_read_line returns
    // a CannotReadFromStream error when the stream
    // sends only \r and then ends, leaving the
    // terminator \r\n incomplete.
    async fn test_stream_read_line_half_separator() {
        let mut stream: &[u8] = b"OK\r";
        let mut buffer = [0u8; 16];

        match stream_read_line(&mut stream, &mut buffer).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Test that the function stream_write_resp
    // serialises a RESP value and writes the
    // resulting bytes to the stream, returning
    // the number of bytes written.
    async fn test_stream_write_resp() {
        let mut stream: Vec<u8> = Vec::new();
        let data = Resp::simple_string("OK");

        let size = stream_write_resp(&mut stream, &data).await.unwrap();

        assert_eq!(stream, b"+OK\r\n");
        assert_eq!(size, 5);
    }

    #[tokio::test]
    // Test that the function stream_read_resp
    // reads bytes from the stream and parses them
    // into a RESP value.
    async fn test_stream_read_resp() {
        let mut stream: &[u8] = b"+OK\r\n";
        let mut buffer = [0u8; 16];

        let resp = stream_read_resp(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(resp, Resp::simple_string("OK"));
    }

    #[tokio::test]
    // Test that the function stream_read_resp
    // passes to the parser the bytes it has
    // actually read and not any stale bytes
    // left in the buffer.
    async fn test_stream_read_resp_short_message_in_large_buffer() {
        let mut stream: &[u8] = b"+OK\r\n";

        // Pre-fill the buffer with garbage that would
        // confuse the parser if it were consulted.
        let mut buffer = [b'X'; 64];

        let resp = stream_read_resp(&mut stream, &mut buffer).await.unwrap();

        assert_eq!(resp, Resp::simple_string("OK"));
    }

    #[tokio::test]
    // Test that the function stream_read_resp returns
    // a MalformedResp error when the bytes in the
    // stream are not a valid RESP message.
    async fn test_stream_read_resp_malformed() {
        let mut stream: &[u8] = b"not resp";
        let mut buffer = [0u8; 16];

        match stream_read_resp(&mut stream, &mut buffer).await {
            Err(ConnectionError::MalformedResp(_)) => {}
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Test that the function stream_send_receive_resp
    // writes the request to the stream and then reads
    // the response from the same stream. We use a
    // tokio duplex pair to simulate a connected peer
    // that reads the request and writes back a canned
    // response.
    async fn test_stream_send_receive_resp() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut client, mut server) = tokio::io::duplex(64);

        // Spawn a task that acts as the remote peer:
        // read whatever the client sends, then write
        // back a canned RESP response.
        let peer = tokio::spawn(async move {
            let mut request_buffer = [0u8; 64];
            let n = server.read(&mut request_buffer).await.unwrap();

            // Sanity check: the client must have sent
            // the serialised PING command.
            assert_eq!(&request_buffer[..n], b"+PING\r\n");
            server.write_all(b"+PONG\r\n").await.unwrap();
        });

        let request = Resp::simple_string("PING");
        let mut buffer = [0u8; 64];

        let response = stream_send_receive_resp(&mut client, &request, &mut buffer)
            .await
            .unwrap();

        assert_eq!(response, Resp::simple_string("PONG"));
        peer.await.unwrap();
    }

    #[tokio::test]
    // Test that the function stream_read_data_length
    // reads exactly the requested number of bytes
    // from the stream and stores them in the buffer.
    async fn test_stream_read_data_length() {
        let mut stream: &[u8] = b"HELLO WORLD";
        let mut buffer = [0u8; 16];

        let size = stream_read_data_length(&mut stream, &mut buffer, 5)
            .await
            .unwrap();

        assert_eq!(size, 5);
        assert_eq!(&buffer[..size], b"HELLO");
    }

    #[tokio::test]
    // Test that the function stream_read_data_length
    // returns a BufferTooSmall error when the caller
    // asks for more bytes than the buffer can hold,
    // without consuming any bytes from the stream.
    async fn test_stream_read_data_length_buffer_too_small() {
        let mut stream: &[u8] = b"HELLO";
        let mut buffer = [0u8; 3];

        match stream_read_data_length(&mut stream, &mut buffer, 5).await {
            Err(ConnectionError::BufferTooSmall(size)) => {
                assert_eq!(size, 3);
            }
            _ => panic!(),
        }
    }

    #[tokio::test]
    // Test that the function stream_read_data_length
    // returns a CannotReadFromStream error when the
    // stream ends before the requested amount of
    // bytes have been read.
    async fn test_stream_read_data_length_short_stream() {
        let mut stream: &[u8] = b"HI";
        let mut buffer = [0u8; 16];

        match stream_read_data_length(&mut stream, &mut buffer, 5).await {
            Err(ConnectionError::CannotReadFromStream(_)) => {}
            _ => panic!(),
        }
    }
}
