use crate::connection::ConnectionMessage;
use crate::request::Request;
use crate::storage::Storage;
use crate::Resp;
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
}

// Listen for incoming messages and process the contained requests.
pub async fn run_server(mut server: Server, mut crx: mpsc::Receiver<ConnectionMessage>) {
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
        }
    }
}

// Process an incoming request and return a result.
pub async fn process_request(request: Request, server: &mut Server) {
    // Check if the request is expressed using
    // a RESP array and extract the elements.
    let elements = match request.value {
        Resp::Array(v) => v,
        _ => {
            panic!()
        }
    };

    // The vector that contains all the commands we need to process.
    let mut command = Vec::new();

    // Check that each element of the array is a
    // bulk string, extract the content, and add it
    // to the vector.
    for elem in elements.into_iter() {
        match elem {
            Resp::BulkString(v) => command.push(v),
            _ => {
                panic!()
            }
        }
    }

    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => panic!(),
    };

    // Process the command contained in the request.
    let _response = storage.process_command(&command);
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
