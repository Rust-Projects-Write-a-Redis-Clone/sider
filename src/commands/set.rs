use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerMessage, ServerValue};
use crate::set::parse_set_arguments;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    let storage = match server.storage.as_mut() {
        Some(storage) => storage,
        None => {
            request.error(ServerError::StorageNotInitialised).await;
            return;
        }
    };

    if command.len() < 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    let key = command[1].clone();
    let value = command[2].clone();
    let args = match parse_set_arguments(&command[3..]) {
        Ok(args) => args,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    // Set the value of the key in the storage.
    if storage.set(key, value, args).is_err() {
        request
            .error(ServerError::CommandInternalError(command.join(" ")))
            .await;
        return;
    };

    request
        .data(ServerValue::Resp(Resp::simple_string("OK")))
        .await;

    // Forward the given request to all replicas known to the server.
    for replica_sender in server.replica_senders.iter() {
        let _ = replica_sender
            .send(ServerMessage::Data(ServerValue::Binary(
                request.binary.clone(),
            )))
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;
    use tokio::sync::mpsc;

    #[tokio::test]
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
            value: Resp::Null,
            sender: request_channel_tx,
            binary: Vec::new(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK")))
        );
    }

    #[tokio::test]
    async fn test_storage_not_initialised() {
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx,
            binary: Vec::new(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::StorageNotInitialised)
        );
    }

    #[tokio::test]
    async fn test_wrong_syntax() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);

        server.set_storage(storage);

        let cmd = vec![String::from("set"), String::from("key")];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx,
            binary: Vec::new(),
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError("set key".to_string()))
        );
    }
}
