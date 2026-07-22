use crate::request::Request;
use crate::resp::Resp;
use crate::server::{wait_handler, Server};
use crate::server_result::{ServerError, ServerMessage, ServerValue};
use tokio::sync::mpsc;

pub async fn command(server: &mut Server, request: &Request, command: &[String]) {
    // If no write has happened since the last WAIT
    // there is nothing for the replicas to
    // acknowledge. Report the replica count right
    // away and skip the handler machinery.
    if !server.replication.pending_write_operations {
        request
            .data(ServerValue::Resp(Resp::Integer(
                server.replica_senders.len() as i64,
            )))
            .await;
        return;
    }

    // The command comes in the form `WAIT numreplicas timeout`.
    let numreplicas: i64 = match command[1].parse() {
        Ok(v) => v,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    // The command comes in the form `WAIT numreplicas timeout`.
    let timeout: u64 = match command[2].parse() {
        Ok(v) => v,
        Err(_) => {
            request
                .error(ServerError::CommandSyntaxError(command.join(" ")))
                .await;
            return;
        }
    };

    // WAIT will process all pending operations.
    // Reset the flag.
    server.replication.pending_write_operations = false;

    // Prepare the message for all replicas.
    let getack = Resp::bulk_array(vec!["REPLCONF", "GETACK", "*"]);

    // Ask every replica to send back an ACK with
    // its current offset.
    for replica in server.replica_senders.iter() {
        let _ = replica
            .send(ServerMessage::Data(ServerValue::Resp(getack.clone())))
            .await;
    }

    // Create a channel that will receive replica ACKs.
    // The wait handler will listen there.
    let (wait_handler_sender, wait_handler_receiver) = mpsc::channel::<ServerMessage>(32);

    // Add the wait handler sender to the server.
    server.wait_handler_sender = Some(wait_handler_sender);

    // Spawn the wait handler. It owns the receiver
    // and a clone of the client's reply channel, so
    // it can answer the client directly.
    tokio::spawn(wait_handler(
        timeout,
        numreplicas,
        wait_handler_receiver,
        request.sender.clone(),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command replies with
    // 0 when there are no replicas registered on
    // the server.
    async fn test_command_wait_no_replicas() {
        let cmd = vec![String::from("wait"), String::from("0"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);
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
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(0)))
        )
    }

    #[tokio::test]
    // Test that the function command replies with
    // the number of replicas currently registered
    // on the server.
    async fn test_command_wait_counts_replicas() {
        let cmd = vec![String::from("wait"), String::from("0"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        // Register two fake replica channels. The
        // receivers stay alive for the duration of
        // the test so the senders remain valid.
        let (replica_a_tx, _replica_a_rx) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, _replica_b_rx) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

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
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(2)))
        )
    }

    #[tokio::test]
    // Test that when pending_write_operations is
    // set, WAIT sends a `REPLCONF GETACK *` request
    // to every registered replica, consumes the
    // flag and installs wait_handler_sender. The
    // handler runs in the background but has no
    // ACKs to report yet, so nothing is sent back
    // to the client during the command execution.
    async fn test_command_wait_pending_writes() {
        let cmd = vec![String::from("wait"), String::from("2"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        // Register two fake replica channels.
        let (replica_a_tx, mut replica_a_rx) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, mut replica_b_rx) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

        // Simulate a write having happened since
        // the last WAIT.
        server.replication.pending_write_operations = true;

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        // Each replica has received a GETACK.
        let getack = ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(vec![
            "REPLCONF", "GETACK", "*",
        ])));
        assert_eq!(replica_a_rx.try_recv().unwrap(), getack);
        assert_eq!(replica_b_rx.try_recv().unwrap(), getack);

        // The flag has been consumed.
        assert!(!server.replication.pending_write_operations);

        // A wait handler sender has been installed.
        assert!(server.wait_handler_sender.is_some());

        // The handler is still waiting for ACKs:
        // nothing has been forwarded to the client
        // yet.
        assert_eq!(
            connection_receiver.try_recv().unwrap_err(),
            mpsc::error::TryRecvError::Empty
        );
    }

    #[tokio::test]
    // Test that the wait handler replies to the
    // client with the number of ACKs received once
    // the requested threshold is reached.
    async fn test_command_wait_handler_replies_on_threshold() {
        let cmd = vec![String::from("wait"), String::from("2"), String::from("100")];
        let mut server = Server::new("localhost".to_string(), 6379);

        // Register two fake replica channels.
        let (replica_a_tx, _replica_a_rx) = mpsc::channel::<ServerMessage>(32);
        let (replica_b_tx, _replica_b_rx) = mpsc::channel::<ServerMessage>(32);
        server.replica_senders.push(replica_a_tx);
        server.replica_senders.push(replica_b_tx);

        server.replication.pending_write_operations = true;

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&mut server, &request, &cmd).await;

        // Feed two ACKs through the installed
        // wait_handler_sender, simulating what
        // replconf::command would do when two real
        // replicas reply.
        let wait_sender = server.wait_handler_sender.clone().unwrap();
        wait_sender
            .send(ServerMessage::Data(ServerValue::None))
            .await
            .unwrap();
        wait_sender
            .send(ServerMessage::Data(ServerValue::None))
            .await
            .unwrap();

        // The handler should reply to the client
        // with the collected count.
        let response = connection_receiver.recv().await.unwrap();
        assert_eq!(
            response,
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(2)))
        );
    }
}
