use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::{ServerError, ServerValue};

pub async fn command(server: &Server, request: &Request, command: &[String]) {
    // On a master, the REPLCONF is received only
    // from the handshake process. In that case we
    // always reply with OK.
    if !server.replication.is_replica() {
        request
            .data(ServerValue::Resp(Resp::simple_string("OK")))
            .await;
        return;
    }

    // On a replica, the master sends `REPLCONF GETACK *`
    // and expects a `REPLCONF ACK <offset>` reply.
    if command.len() != 3 {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // The only subcommand that REPLCONF supports
    // on a replica is GETACK. Reject anything else.
    if command[1].to_uppercase() != "GETACK" {
        request
            .error(ServerError::CommandSyntaxError(command.join(" ")))
            .await;
        return;
    }

    // Create the response for the server.
    let resp = Resp::bulk_array(vec![
        "REPLCONF",
        "ACK",
        &server.replication.repl_offset.to_string(),
    ]);

    request.data(ServerValue::Resp(resp)).await;
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
}
