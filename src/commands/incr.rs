use crate::request::Request;
use crate::server::Server;
use crate::server_result::ServerError;

pub async fn command(server: &mut Server, request: &Request, _command: &[String]) {
    // Extract the storage from the server.
    let _storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::Resp;
    use crate::server_result::{ServerError, ServerMessage};
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the storage is not initialised.
    async fn test_storage_not_initialised() {
        let mut server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![String::from("incr"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }
}
