use crate::commands::{echo, get, info, ping, psync, replconf, set};
use crate::connection::{
    stream_read_data_length, stream_read_line, stream_send_receive_resp, ConnectionMessage,
};
use crate::replication::ReplicationConfig;
use crate::request::Request;
use crate::resp::{bytes_to_resp, resp_extract_length, resp_remove_type};
use crate::server_result::{ServerError, ServerMessage, ServerResult, ServerValue};
use crate::storage::Storage;
use crate::Resp;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::{io::AsyncWriteExt, net::TcpStream};

pub struct ServerInfo {
    #[expect(dead_code)]
    pub host: String,
    pub port: u16,
}

pub struct Server {
    pub info: ServerInfo,
    pub storage: Option<Storage>,
    pub replication: ReplicationConfig,
    pub replica_senders: Vec<mpsc::Sender<ServerMessage>>,
}

impl Server {
    pub fn new(host: String, port: u16) -> Self {
        Self {
            info: ServerInfo { host, port },
            storage: None,
            replication: ReplicationConfig::new_master(),
            replica_senders: Vec::new(),
        }
    }

    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = Some(storage);
    }

    pub fn set_replication(&mut self, config: ReplicationConfig) {
        self.replication = config;
    }

    // The entry point for key expiry operations.
    pub fn expire_keys(&mut self) {
        // Get the storage contained in the server.
        let storage = match self.storage.as_mut() {
            Some(storage) => storage,
            None => return,
        };

        // Trigger the expiry process.
        storage.expire_keys();
    }

    pub fn generate_rdb(&self) -> Vec<u8> {
        let v: Vec<u8> = vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];

        v
    }
}

// Listen for incoming messages and process the contained requests.
pub async fn run_server(mut server: Server, mut crx: mpsc::Receiver<ConnectionMessage>) {
    let mut interval_timer = tokio::time::interval(Duration::from_millis(10));

    loop {
        tokio::select! {
            // Keep listening for messages on the incoming channel.
            Some(message) = crx.recv() => {
                match message {
                    // If the message contains a request, extract it.
                    ConnectionMessage::Request(request) => {
                        // Process the request.
                        process_request(request, &mut server).await;
                    }
                }
            }

            _ = interval_timer.tick() => {
                server.expire_keys();
            }
        }
    }
}

// Forward the given request to all replicas known to the server.
pub async fn send_request_to_replicas(request: &Request, server: &Server) {
    for replica_sender in server.replica_senders.iter() {
        let _ = replica_sender
            .send(ServerMessage::Data(ServerValue::Binary(
                request.binary.clone(),
            )))
            .await;
    }
}

// Process an incoming request and return a result.
pub async fn process_request(request: Request, server: &mut Server) {
    // Check if the request is expressed using
    // a RESP array and extract the elements.
    let elements = match &request.value {
        Resp::Array(v) => v,
        _ => {
            request.error(ServerError::IncorrectData).await;
            return;
        }
    };

    // The vector that contains all the commands we need to process.
    let mut command = Vec::new();

    // Check that each element of the array is a
    // bulk string, extract the content, and add it
    // to the vector.
    for elem in elements.iter() {
        match elem {
            Resp::BulkString(v) => command.push(v.clone()),
            _ => {
                request.error(ServerError::IncorrectData).await;
                return;
            }
        }
    }

    // Extract the command name to route the request.
    let command_name = command[0].to_lowercase();

    // Process the request using the requested command.
    match command_name.as_str() {
        "echo" => {
            echo::command(server, &request, &command).await;
        }
        "get" => {
            get::command(server, &request, &command).await;
        }
        "info" => {
            info::command(server, &request, &command).await;
        }
        "ping" => {
            ping::command(server, &request, &command).await;
        }
        "psync" => {
            psync::command(server, &request, &command).await;
        }
        "replconf" => {
            replconf::command(server, &request, &command).await;
        }
        "set" => {
            set::command(server, &request, &command).await;

            // Forward the request to all replicas.
            send_request_to_replicas(&request, server).await;
        }
        _ => {
            request
                .error(ServerError::CommandNotAvailable(command[0].clone()))
                .await;
        }
    }
}

// The implementation of the handshake process
// between a replica and the master.
pub async fn handshake(stream: &mut TcpStream, info: &ServerInfo) -> ServerResult {
    // Create a buffer to contain the response bytes.
    let mut buffer = [0; 512];

    /////////////////////////////////////
    // Send PING

    // Prepare the RESP PING command.
    let ping = Resp::bulk_array(vec!["PING"]);

    // Send the command and read the response.
    let resp = stream_send_receive_resp(stream, &ping, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    // Check that the response is correct.
    if resp != Resp::simple_string("PONG") {
        return Err(ServerError::HandshakeFailed(String::from("PING failed")));
    };

    /////////////////////////////////////
    // Send REPLCONF listening-port xxx

    // Prepare the RESP REPLCONF command.
    let replconf = Resp::bulk_array(vec!["REPLCONF", "listening-port", &info.port.to_string()]);

    // Send the command and read the response.
    let resp = stream_send_receive_resp(stream, &replconf, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    // Check that the response is correct.
    if resp != Resp::simple_string("OK") {
        return Err(ServerError::HandshakeFailed(format!(
            "Sending {} - Wrong server answer: {}",
            replconf, resp
        )));
    };

    /////////////////////////////////////
    // Send REPLCONF capa psync2

    // Prepare the RESP REPLCONF command.
    let replconf = Resp::bulk_array(vec!["REPLCONF", "capa", "psync2"]);

    // Send the command and read the response.
    let resp = stream_send_receive_resp(stream, &replconf, &mut buffer)
        .await
        .map_err(|e| ServerError::HandshakeFailed(e.to_string()))?;

    // Check that the response is correct.
    if resp != Resp::simple_string("OK") {
        return Err(ServerError::HandshakeFailed(format!(
            "Sending {} - Wrong server answer: {}",
            replconf, resp
        )));
    };

    /////////////////////////////////////
    // Send PSYNC ? -1

    // Prepare the RESP PSYNC command.
    let psync = Resp::bulk_array(vec!["PSYNC", "?", "-1"]);

    // Send the PSYNC command.
    stream
        .write_all(psync.to_string().as_bytes())
        .await
        .map_err(|e| {
            ServerError::HandshakeFailed(format!(
                "Sending {} - Cannot write to stream: {}",
                psync, e
            ))
        })?;

    /////////////////////////////////////
    // The response will be
    // * The FULLRESYNC: `+FULLRESYNC ID 0\r\n`
    // * The RDB: `$LEN\r\nBINARY`

    let mut index: usize = 0;

    // Read the FULLRESYNC line.
    if stream_read_line(stream, &mut buffer).await.is_err() {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, no FULLRESYNC",
        )));
    }

    // Convert bytes into RESP.
    let response = bytes_to_resp(&buffer, &mut index).ok();
    match response {
        Some(Resp::SimpleString(_)) => {}
        _ => {
            return Err(ServerError::HandshakeFailed(String::from(
                "PSYNC failed, wrong FULLRESYNC response",
            )));
        }
    };

    // Read the RDB length.
    if stream_read_line(stream, &mut buffer).await.is_err() {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, cannot read RDB length",
        )));
    }

    index = 0;

    // Remove the dollar sign.
    if resp_remove_type('$', &buffer, &mut index).is_err() {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, RDB doesn't start with $",
        )));
    }

    // Convert bytes into RDB length.
    let length = resp_extract_length(&buffer, &mut index).map_err(|_| {
        ServerError::HandshakeFailed(String::from("PSYNC failed, malformed RDB length"))
    })?;

    // Read the RDB data.
    if stream_read_data_length(stream, &mut buffer, length as usize)
        .await
        .is_err()
    {
        return Err(ServerError::HandshakeFailed(String::from(
            "PSYNC failed, cannot read RDB",
        )));
    }

    Ok(ServerValue::None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::{ServerMessage, ServerValue};

    #[test]
    // Test that the structure Server can
    // be initialised with an empty storage.
    fn test_create_new() {
        let server: Server = Server::new("localhost".to_string(), 6379);

        assert!(server.storage.is_none());
    }

    #[test]
    // Test that an existing Storage structure
    // can be associated with a Server.
    fn test_set_storage() {
        let storage = Storage::new();

        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        assert!(server.storage.is_some());
    }

    #[tokio::test]
    // Test that the function process_request
    // processes a PING request and that it
    // responds with a PONG.
    async fn test_process_request_ping() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_array(vec!["PING"]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("PONG")))
        );
    }

    #[tokio::test]
    // Test that the function process_request
    // processes an ECHO request and that it
    // responds with a copy of the input.
    async fn test_process_request_echo() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_array(vec!["ECHO", "42"]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_string("42")))
        );
    }

    #[tokio::test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that doesn't contain
    // a RESP array.
    async fn test_process_request_not_array() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::bulk_string("PING"),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::IncorrectData)
        );
    }

    #[tokio::test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that contains a RESP array
    // but the content of the array is not
    // a bulk string.
    async fn test_process_request_not_bulkstrings() {
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Array(vec![Resp::simple_string("PING")]),
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        let storage = Storage::new();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::IncorrectData)
        );
    }
}
