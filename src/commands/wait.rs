use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(server: &Server, request: &Request, _command: &Vec<String>) {
    // Report how many replicas are currently connected.
    request
        .data(ServerValue::RESP(RESP::Integer(
            server.replica_senders.len() as i64,
        )))
        .await
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
        let server = Server::new("localhost".to_string(), 6379);
        let (connection_sender, mut connection_receiver) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::Integer(0)))
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
            value: RESP::Null,
            sender: connection_sender,
            binary: Vec::new(),
            master_connection: false,
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::Integer(2)))
        )
    }
}
