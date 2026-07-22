use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::set::SetArgs;
use crate::storage::StorageValue;

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

    // Resolve the stored string. The key might exist or not,
    // and if it exists it might not be a StorageValue::String.
    let string_value = match output {
        // The key exists and holds a string.
        Ok(Some(StorageValue::String(s))) => s,

        // The key doesn't exist. Pretend it is "0".
        Ok(None) => String::from("0"),

        // The key exists but isn't a string, or reading it failed.
        Ok(Some(_)) | Err(_) => {
            request
                .error(ServerError::CommandInternalError(command.join(" ")))
                .await;
            return;
        }
    };

    // Parse the string value into an i64.
    let result = match string_value.parse::<i64>() {
        // The conversion succeeded.
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
            Some(StorageValue::String(String::from("43")))
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
