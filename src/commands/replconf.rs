use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerMessage, ServerResult, ServerValue};
use tokio::sync::mpsc;

pub async fn command(server: &Server, request: &Request, command: &[String]) {
    let result = if server.replication.is_replica() {
        command_replica(server, command)
    } else {
        command_master(server.wait_handler_sender.clone(), command).await
    };

    request.result(result).await;
}

// On a master, the REPLCONF is received either
// from the handshake process (in which case we
// reply with OK) or as a `REPLCONF ACK <offset>`
// sent by a replica in response to a GETACK we
// previously issued, which shouldn't produce
// any reply.
async fn command_master(
    wait_handler_sender: Option<mpsc::Sender<ServerMessage>>,
    command: &[String],
) -> ServerResult {
    // A replica is reporting its offset in
    // response to a GETACK. Send an empty
    // message to the wait handler and stay
    // silent on the replica's own channel.
    if command.len() >= 2 && command[1].to_uppercase() == "ACK" {
        if let Some(tx) = wait_handler_sender {
            // The wait handler might have already
            // dropped its receiver.
            // Ignore the send error.
            let _ = tx.send(ServerMessage::Data(ServerValue::None)).await;
        }

        return Ok(ServerValue::None);
    }

    Ok(ServerValue::Resp(Resp::simple_string("OK")))
}

// On a replica, the master sends `REPLCONF GETACK *`
// and expects a `REPLCONF ACK <offset>` reply
// reporting how many bytes of the replication stream
// the replica processed.
fn command_replica(server: &Server, command: &[String]) -> ServerResult {
    // On a replica, the master sends `REPLCONF GETACK *`
    // and expects a `REPLCONF ACK <offset>` reply.
    if command.len() != 3 {
        return Err(ServerError::CommandSyntaxError(command.join(" ")));
    }

    // The only subcommand that REPLCONF supports
    // on a replica is GETACK. Reject anything else.
    if command[1].to_uppercase() != "GETACK" {
        return Err(ServerError::CommandSyntaxError(command.join(" ")));
    }

    // Create the response for the server.
    let resp = Resp::bulk_array(vec![
        "REPLCONF",
        "ACK",
        &server.replication.repl_offset.to_string(),
    ]);

    Ok(ServerValue::Resp(resp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::ReplicationConfig;
    use crate::server_result::ServerMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // a `REPLCONF listening-port` request on a master
    // and that it responds with the correct value.
    async fn test_command_replconf_listening_port() {
        let cmd = vec![
            String::from("replconf"),
            String::from("listening-port"),
            String::from("1234"),
        ];
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK")))
        )
    }

    #[tokio::test]
    // Test that the function command processes
    // a `REPLCONF capa` request on a master and
    // that it responds with the correct value.
    async fn test_command_replconf_capa() {
        let cmd = vec![
            String::from("replconf"),
            String::from("capa"),
            String::from("psync"),
        ];
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK")))
        )
    }

    #[tokio::test]
    // Test that the function command processes
    // a `REPLCONF GETACK *` request on a replica
    // and that it responds with `REPLCONF ACK 0`.
    async fn test_command_replconf_getack_on_replica() {
        let cmd = vec![
            String::from("replconf"),
            String::from("GETACK"),
            String::from("*"),
        ];

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(vec![
                "REPLCONF", "ACK", "0",
            ])))
        )
    }

    #[tokio::test]
    // Test that the function command processes
    // a lowercase `REPLCONF getack *` request on a
    // replica and that it responds with `REPLCONF ACK 0`.
    async fn test_command_replconf_getack_on_replica_lowercase() {
        let cmd = vec![
            String::from("replconf"),
            String::from("getack"),
            String::from("*"),
        ];

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::bulk_array(vec![
                "REPLCONF", "ACK", "0",
            ])))
        )
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when a replica receives a
    // `REPLCONF` request with the wrong number of
    // arguments.
    async fn test_command_replconf_on_replica_wrong_length() {
        let cmd = vec![String::from("replconf"), String::from("GETACK")];

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from(
                "replconf GETACK"
            )))
        )
    }

    #[tokio::test]
    // Test that the function command returns the
    // correct error when a replica receives a
    // `REPLCONF` request with an unknown subcommand.
    async fn test_command_replconf_on_replica_wrong_subcommand() {
        let cmd = vec![
            String::from("replconf"),
            String::from("something"),
            String::from("*"),
        ];

        let mut server = Server::new("localhost".to_string(), 6379);
        server.set_replication(ReplicationConfig::new_replica(
            "otherhost".to_string(),
            1234,
        ));

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Error(ServerError::CommandSyntaxError(String::from(
                "replconf something *"
            )))
        )
    }

    #[tokio::test]
    // Test that a master receiving `REPLCONF ACK`
    // sends an empty message to the wait handler
    // and sends nothing back on the replica's own
    // channel.
    async fn test_command_replconf_ack_on_master_forwards_to_wait_handler() {
        let cmd = vec![
            String::from("replconf"),
            String::from("ACK"),
            String::from("42"),
        ];

        let mut server = Server::new("localhost".to_string(), 6379);

        // Install a wait handler sender so the
        // master has somewhere to forward the ACK.
        let (wait_tx, mut wait_rx) = mpsc::channel::<ServerMessage>(32);
        server.wait_handler_sender = Some(wait_tx);

        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            wait_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::None)
        );

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::None)
        );
    }

    #[tokio::test]
    // Test that a master receiving `REPLCONF ACK`
    // while no wait handler is listening simply
    // drops the ACK without replying.
    async fn test_command_replconf_ack_on_master_no_wait_handler() {
        let cmd = vec![
            String::from("replconf"),
            String::from("ACK"),
            String::from("42"),
        ];

        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: Resp::Null,
            sender: connection_sender,
            binary: Vec::new(),
            client_id: 1,
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::None)
        );
    }
}
