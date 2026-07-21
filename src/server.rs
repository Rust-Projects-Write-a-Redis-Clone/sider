use crate::commands::{echo, get, ping, set};
use crate::connection::ConnectionMessage;
use crate::request::Request;
use crate::server_result::ServerError;
use crate::storage::Storage;
use crate::Resp;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Server {
    pub storage: Option<Storage>,
}

impl Server {
    pub fn new() -> Self {
        Self { storage: None }
    }

    pub fn set_storage(&mut self, storage: Storage) {
        self.storage = Some(storage);
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
        "ping" => {
            ping::command(server, &request, &command).await;
        }
        "set" => {
            set::command(server, &request, &command).await;
        }
        _ => {
            request
                .error(ServerError::CommandNotAvailable(command[0].clone()))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::{ServerMessage, ServerValue};

    #[test]
    // Test that the structure Server can
    // be initialised with an empty storage.
    fn test_create_new() {
        let server: Server = Server::new();

        assert!(server.storage.is_none());
    }

    #[test]
    // Test that an existing Storage structure
    // can be associated with a Server.
    fn test_set_storage() {
        let storage = Storage::new();

        let mut server: Server = Server::new();
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
        };

        let storage = Storage::new();

        let mut server: Server = Server::new();
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
        };

        let storage = Storage::new();

        let mut server: Server = Server::new();
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
        };

        let storage = Storage::new();

        let mut server: Server = Server::new();
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
        };

        let storage = Storage::new();

        let mut server: Server = Server::new();
        server.set_storage(storage);

        process_request(request, &mut server).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::IncorrectData)
        );
    }
}
