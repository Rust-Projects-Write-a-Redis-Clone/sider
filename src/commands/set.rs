use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};
use crate::set::parse_set_arguments;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    // A replica only accepts SET from the master.
    // Any client that sends SET to a replica gets
    // a CommandNotAvailable error.
    if server.replication.is_replica() && !request.master_connection {
        request
            .error(ServerError::CommandNotAvailable(command[0].clone()))
            .await;
        return;
    }

    // Extract the storage from the server.
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

    // If the server is the master, toggle the
    // flag to signal that at least one write
    // operation has happened.
    if !server.replication.is_replica() {
        server.replication.pending_write_operations = true;
    }

    // When the master forwards a SET command to
    // a replica the replica must stay silent.
    if request.master_connection {
        return;
    }

    request
        .data(ServerValue::Resp(Resp::simple_string("OK")))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use crate::storage::Storage;
    use crate::ReplicationConfig;
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
            client_id: 1,
            master_connection: false,
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
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError("set key".to_string()))
        );
    }

    #[tokio::test]
    // Test that a replica rejects a SET command
    // received from a regular client connection.
    async fn test_command_replica_rejects_client_set() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));
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
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandNotAvailable(String::from("set")))
        );
    }

    #[tokio::test]
    // Test that the function command stays silent
    // when a SET is propagated from the master
    // down the replication link.
    async fn test_command_master_connection_is_silent() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));
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
            client_id: 1,
            master_connection: true,
        };

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        );
    }

    #[tokio::test]
    // Test that a successful SET on a master toggles
    // pending_write_operations.
    async fn test_command_master_sets_pending_write_operations() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_storage(storage);

        assert!(!server.replication.pending_write_operations);

        let cmd = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, _request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        assert!(server.replication.pending_write_operations);
    }

    #[tokio::test]
    // Test that a SET propagated from the master to
    // a replica does NOT toggle the replica's own
    // pending_write_operations flag.
    async fn test_command_replica_does_not_set_pending_write_operations() {
        let storage = Storage::new();
        let mut server: Server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));
        server.set_storage(storage);

        let cmd = vec![
            String::from("set"),
            String::from("key"),
            String::from("value"),
        ];

        let (request_channel_tx, _request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: request_channel_tx,
            binary: Vec::new(),
            client_id: 1,
            master_connection: true,
        };

        command(&mut server, &request, &cmd).await;

        assert!(!server.replication.pending_write_operations);
    }
}
