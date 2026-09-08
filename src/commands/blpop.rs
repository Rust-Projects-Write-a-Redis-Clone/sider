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

    // Check that the command received 3 arguments.
    if command.len() != 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Extract the key.
    let key = &command[1];

    // Extract the timeout.
    let timeout_str = &command[2];

    // Convert the timeout from string to number.
    let timeout: f64 = match timeout_str.parse() {
        Ok(n) => n,

        // The value is not a valid number.
        Err(_) => {
            request
                .data(ServerValue::Resp(Resp::simple_error(
                    "ERR timeout is not a number",
                )))
                .await;
            return;
        }
    };

    // Run a standard lpop on the storage.
    let result = storage.lpop(key, 1);

    match result {
        Ok(v) => {
            // If the list is empty we register the
            // client in the server, associated with
            // the requested key. When the list
            // is populated, the server will notify
            // the client.
            if v.is_empty() {
                server.block_client(timeout, key, request.client_id, request.sender.clone());
                return;
            }

            request
                .data(ServerValue::Resp(Resp::bulk_array([key, &v[0]])))
                .await;
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
    use crate::set::SetArgs;
    use crate::storage::Storage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // a `BLPOP` request without timeout.
    // The list contains a value.
    async fn test_command() {
        let mut storage = Storage::new();
        storage
            .rpush("key", &[String::from("value0"), String::from("value1")])
            .unwrap();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("0"),
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
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(["key", "value0"])))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // a `BLPOP` request.
    // The list is empty.
    async fn test_command_empty_list() {
        let mut storage = Storage::new();
        storage.rpush("key", &[]).unwrap();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("0"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        // Wrap the command in a short timeout.
        // When the timeout expires, the channel
        // should be empty as the command is
        // waiting indefinitely.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            command(&mut server, &request, &cmd),
        )
        .await;

        assert!(request_channel_rx.try_recv().is_err());
    }

    #[tokio::test]
    // Test that the function command processes
    // a `BLPOP` request.
    // The key does not exist, and BLPOP treats
    // it as an empty list.
    async fn test_command_key_does_not_exist() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("0"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        // Wrap the command in a short timeout.
        // When the timeout expires, the channel
        // should be empty as the command is
        // waiting indefinitely.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            command(&mut server, &request, &cmd),
        )
        .await;

        assert!(request_channel_rx.try_recv().is_err());
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called on a server that
    // has no storage attached.
    async fn test_command_no_storage() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("0"),
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
    // No key is specified.
    async fn test_command_wrong_number_of_parameters_no_key() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![String::from("blpop")];

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
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from("blpop")))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called with the wrong
    // number of parameters.
    // No timeout is specified.
    async fn test_command_wrong_number_of_parameters_no_timeout() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![String::from("blpop"), String::from("key")];

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
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from("blpop key")))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the timeout is not a number.
    async fn test_command_timeout_not_number() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("apple"),
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
            ServerMessage::Data(ServerValue::Resp(Resp::simple_error(
                "ERR timeout is not a number"
            )))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the value of the key
    // is not a list.
    async fn test_command_key_is_not_list() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "value".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("blpop"),
            String::from("key"),
            String::from("0"),
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
            ServerMessage::Data(ServerValue::Resp(Resp::wrong_type_error()))
        );
    }
}
