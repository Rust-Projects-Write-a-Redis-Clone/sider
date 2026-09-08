use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::storage_result::StorageError;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    // Extract the storage from the server.
    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };

    // Check that the command received a key and a value.
    if command.len() < 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Extract the key and the values.
    let key = &command[1];
    let value = &command[2..];

    let result = storage.rpush(key, value);

    match result {
        Ok(size) => {
            request
                .data(ServerValue::Resp(Resp::Integer(size as i64)))
                .await;

            // If any client is waiting on a BLPOP,
            // send the first element.
            server.notify_blocked_clients(key).await;
        }
        Err(StorageError::WrongType) => {
            request
                .data(ServerValue::Resp(Resp::wrong_type_error()))
                .await
        }
        Err(_) => request.error(ServerError::IncorrectData).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // an `RPUSH` request with a single value.
    async fn test_command_single_value() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("rpush"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(1)))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `RPUSH` request with multiple values
    // to an existing key which is already a list.
    async fn test_command_multiple_values() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("rpush"),
            String::from("key"),
            String::from("value0"),
            String::from("value1"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(2)))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called on a server that
    // has no storage attached.
    async fn test_command_no_storage() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![
            String::from("rpush"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called with the wrong
    // number of parameters.
    async fn test_command_wrong_number_of_parameters() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![String::from("rpush"), String::from("key")];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from("rpush key")))
        );
    }
}
