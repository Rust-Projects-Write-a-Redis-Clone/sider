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

    // Check that the command received either 1 or 2 arguments.
    if command.len() < 2 || command.len() > 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Extract the key.
    let key = &command[1];

    // Make sure we have a default
    // number of elements to pop.
    let num_items_str = &command.get(2).map_or("1", |v| v).to_string();

    // Convert the number of elements
    // from string to number.
    let num_items: i64 = match num_items_str.parse() {
        Ok(n) => n,

        // The value is not an integer.
        Err(_) => {
            request
                .data(ServerValue::Resp(Resp::simple_error(
                    "ERR num value is not an integer or out of range",
                )))
                .await;
            return;
        }
    };

    let result = storage.lpop(key, num_items);

    match result {
        Ok(v) => {
            if v.is_empty() {
                request.data(ServerValue::Resp(Resp::Null)).await;
                return;
            }

            if v.len() == 1 {
                request
                    .data(ServerValue::Resp(Resp::BulkString(v[0].clone())))
                    .await;
                return;
            }

            request.data(ServerValue::Resp(Resp::bulk_array(v))).await
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
    // an `LPOP` request.
    async fn test_command() {
        let mut storage = Storage::new();
        storage
            .rpush("key", &[String::from("value0"), String::from("value1")])
            .unwrap();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("lpop"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_string("value0")))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // an `LPOP` request.
    async fn test_command_empty_list() {
        let mut storage = Storage::new();
        storage.rpush("key", &[]).unwrap();

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("lpop"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::Null))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // an `LPOP` request.
    async fn test_command_key_does_not_exist() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("lpop"), String::from("key")];

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
            ServerMessage::Data(ServerValue::Resp(Resp::Null))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called on a server that
    // has no storage attached.
    async fn test_command_no_storage() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![String::from("lpop"), String::from("key")];

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

        let cmd = vec![String::from("lpop")];

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
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from("lpop")))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the number of elements
    // is not a number.
    async fn test_command_number_elements_not_number() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("lpop"),
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
                "ERR num value is not an integer or out of range"
            )))
        );
    }
}
