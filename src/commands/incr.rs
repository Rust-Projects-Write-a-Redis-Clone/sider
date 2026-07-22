use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::set::SetArgs;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
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

    // Extract the key from the command line.
    let key = &command[1];

    // Find the value of the key.
    let output = storage.get(key);

    // Check the value of the key.
    let result = match output {
        // The key exists but the value might be None.
        // If it is None, we act as if the key
        // was correctly read and has value "0",
        // which will then be incremented to 1,
        // stored, and sent back to the user.
        Ok(storage_value) => {
            // Convert None into a "0".
            let storage_value = storage_value.unwrap_or_else(|| String::from("0"));

            // Parse the storage value into an i64.
            match storage_value.parse::<i64>() {
                // The value was successfully parsed.
                Ok(n) => {
                    // Increment the value.
                    let value = n + 1;

                    // Try to store the incremented value.
                    match storage.set(key.clone(), value.to_string(), SetArgs::new()) {
                        // Incremented value successfully stored,
                        // send it back to the client.
                        Ok(_) => Ok(ServerValue::Resp(Resp::Integer(value))),
                        // An error occurred.
                        Err(_) => Err(ServerError::CommandInternalError(command.join(" "))),
                    }
                }
                // The value is not an integer.
                Err(_) => Ok(ServerValue::Resp(Resp::simple_error(
                    "ERR value is not an integer or out of range",
                ))),
            }
        }
        // An error occurred while reading the key.
        Err(_) => Err(ServerError::CommandInternalError(command.join(" "))),
    };

    request.result(result).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;
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
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the syntax is wrong.
    async fn test_wrong_syntax() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError("incr".to_string()))
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // an `INCR` request when the key exists and
    // contains a numeric value.
    async fn test_command_key_exists() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "42".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(43)))
        );

        assert_eq!(
            server.storage.as_mut().unwrap().get("key").unwrap(),
            Some("43".to_string())
        );
    }

    #[tokio::test]
    // Test that the function command processes
    // an `INCR` request when the key does not exist
    // and creates it with value 1.
    async fn test_command_key_does_not_exist() {
        let storage = Storage::new();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(1)))
        );
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when the key contains a
    // non-numeric value.
    async fn test_command_key_is_not_numeric() {
        let mut storage = Storage::new();
        storage
            .set("key".to_string(), "answer".to_string(), SetArgs::new())
            .unwrap();

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        let cmd = vec![String::from("incr"), String::from("key")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_error(
                "ERR value is not an integer or out of range"
            )))
        );
    }
}
