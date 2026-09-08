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
    if command.len() != 4 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Extract the key.
    let key = &command[1];

    // Extract start index.
    let start: i64 = match command[2].parse() {
        Ok(n) => n,

        // The value is not an integer.
        Err(_) => {
            request
                .data(ServerValue::Resp(Resp::simple_error(
                    "ERR start value is not an integer or out of range",
                )))
                .await;
            return;
        }
    };

    // Extract stop index.
    let stop: i64 = match command[3].parse() {
        Ok(n) => n,

        // The value is not an integer.
        Err(_) => {
            request
                .data(ServerValue::Resp(Resp::simple_error(
                    "ERR stop value is not an integer or out of range",
                )))
                .await;
            return;
        }
    };

    let result = storage.lrange(key, start, stop);

    match result {
        Ok(v) => {
            let resp: Vec<Resp> = v.iter().cloned().map(Resp::BulkString).collect();
            request.data(ServerValue::Resp(Resp::Array(resp))).await
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
    // an `LRANGE` request.
    async fn test_command() {
        let mut storage = Storage::new();
        let _ = storage.rpush(
            "key",
            &[
                String::from("value0"),
                String::from("value1"),
                String::from("value2"),
                String::from("value3"),
                String::from("value4"),
            ],
        );

        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("lrange"),
            String::from("key"),
            String::from("1"),
            String::from("3"),
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
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(vec![
                "value1", "value2", "value3"
            ])))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when called on a server that
    // has no storage attached.
    async fn test_command_no_storage() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![
            String::from("lrange"),
            String::from("key"),
            String::from("1"),
            String::from("2"),
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
    // number of arguments.
    async fn test_command_wrong_arguments() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("lrange"),
            String::from("key"),
            String::from("1"),
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
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from(
                "lrange key 1"
            )))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the start index is
    // not a number.
    async fn test_command_start_index_not_number() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("lrange"),
            String::from("key"),
            String::from("apple"),
            String::from("4"),
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
                "ERR start value is not an integer or out of range"
            )))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the stop index is
    // not a number.
    async fn test_command_stop_index_not_number() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![
            String::from("lrange"),
            String::from("key"),
            String::from("4"),
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
                "ERR stop value is not an integer or out of range"
            )))
        );
    }
}
