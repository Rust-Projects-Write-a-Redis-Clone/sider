use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::set::parse_set_arguments;

pub async fn command(server: &mut Server, request: &Request, command: &Vec<String>) {
    // Extract the storage from the server.
    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };

    // Check that the command received at least 2 arguments.
    if command.len() < 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Extract the key, the value, and the arguments
    // supported by the command `SET`.
    let key = command[1].clone();
    let value = command[2].clone();
    let args = match parse_set_arguments(&command[3..].to_vec()) {
        Ok(args) => args,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    // Set the value of the key in the storage.
    if let Err(_) = storage.set(key, value, args) {
        request
            .error(ServerError::CommandInternalError(command.join(" ")))
            .await;
        return;
    };

    request
        .data(ServerValue::RESP(RESP::SimpleString(String::from("OK"))))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // a `SET` request and that it
    // responds with the correct message.
    async fn test_command() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: request_channel_tx.clone(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::SimpleString(String::from("OK"))))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `SET` request and that it
    // returns the correct error when
    // the storage is not initialised.
    async fn test_storage_not_initialised() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: request_channel_tx.clone(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `SET` request and that it
    // returns the correct error when
    // the value is not specified.
    async fn test_wrong_syntax_missing_key() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![String::from("set"), String::from("key")];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: request_channel_tx.clone(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError("set key".to_string()))
        );
    }
}
