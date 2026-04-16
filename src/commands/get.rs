use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};

pub async fn command(server: &mut Server, request: &Request, command: &Vec<String>) {
    // Extract the storage from the server.
    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };

    // Check that the command received a single argument.
    if command.len() != 2 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Get the value of the key from the storage.
    let output = storage.get(command[1].clone());

    match output {
        Ok(Some(v)) => request.data(ServerValue::RESP(RESP::BulkString(v))).await,
        Ok(None) => request.data(ServerValue::RESP(RESP::Null)).await,
        Err(_) => {
            request
                .error(ServerError::CommandInternalError(command.join(" ")))
                .await
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::set::SetArgs;
    use crate::storage::Storage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // a `GET` request and that it
    // responds with the value of the key.
    async fn test_command() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "value".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("get"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: connection_sender,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::BulkString(String::from("value"))))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `GET` request and that it
    // returns the correct error when
    // the storage is not initialised.
    async fn test_storage_not_initialised() {
        let mut server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![String::from("get"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: connection_sender,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `GET` request and that it
    // returns the correct error when
    // the key is not specified.
    async fn test_wrong_syntax_missing_key() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("get")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: connection_sender,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError("get".to_string()))
        );
    }
}
