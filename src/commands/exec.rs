use crate::request::Request;
use crate::resp::Resp;
use crate::server::{exec_handler, Server};
use crate::server_result::{ServerError, ServerValue};

pub async fn command(server: &mut Server, request: &Request, _command: &[String]) {
    // Find the client corresponding to
    // the ID contained in the request.
    let client = match server.clients.get_mut(&request.client_id) {
        Some(client) => client,
        None => {
            request
                .error(ServerError::CommandInternalError(String::from(
                    "Cannot find client ID",
                )))
                .await;
            return;
        }
    };

    // Check if the client is inside a transaction.
    if !client.multi {
        // Client multi is off, so EXEC cannot be executed.
        request
            .data(ServerValue::Resp(Resp::simple_error(
                "ERR EXEC without MULTI",
            )))
            .await;
        return;
    };

    // Client multi is on, so we need to execute the
    // transaction. After this, multi should be off.
    client.multi = false;

    // If there are no requests it's pointless to
    // spawn the exec handler.
    if client.requests.is_empty() {
        request
            .data(ServerValue::Resp(Resp::Array(Vec::new())))
            .await;
        return;
    }

    // To execute the transaction the server
    // needs to have a self sender. This should be
    // initialised but as it's optional we need to check.
    let server_sender = match &server.self_sender {
        Some(sender) => sender.clone(),
        None => {
            request
                .error(ServerError::CommandInternalError(String::from(
                    "Server self sender not initialised",
                )))
                .await;
            return;
        }
    };

    // Spawn the Exec Handler.
    tokio::spawn(exec_handler(
        client.requests.clone(),
        server_sender,
        client.sender.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use crate::client::Client;
    use crate::server_result::ServerMessage;

    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command returns an error
    // when the client ID is not found.
    async fn test_command_without_client() {
        let cmd = vec![String::from("exec")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let mut server = Server::new("localhost".to_string(), 6379);

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
            ServerMessage::Error(ServerError::CommandInternalError(String::from(
                "Cannot find client ID"
            ))),
        );
    }

    #[tokio::test]
    // Test that the function command returns an error
    // when EXEC is called without MULTI.
    async fn test_command_without_multi() {
        let cmd = vec![String::from("exec")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let mut server = Server::new("localhost".to_string(), 6379);

        let client = Client::new(1, connection_sender.clone());
        server.clients.insert(1, client);

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
                "ERR EXEC without MULTI"
            )))
        );
    }

    #[tokio::test]
    // Test that the function command returns an
    // empty array when EXEC is called after MULTI.
    async fn test_command_with_multi() {
        let cmd = vec![String::from("exec")];

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let mut server = Server::new("localhost".to_string(), 6379);

        let mut client = Client::new(1, connection_sender.clone());
        client.multi = true;
        server.clients.insert(1, client);

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
            ServerMessage::Data(ServerValue::Resp(Resp::Array(Vec::new())))
        );
    }
}
