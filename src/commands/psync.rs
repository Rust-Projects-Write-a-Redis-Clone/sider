use crate::request::Request;
use crate::resp::RESP;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(server: &mut Server, request: &Request, _command: &Vec<String>) {
    // Reset the master replica offset.
    server.replication.repl_offset = 0;

    // Create the FULLRESYNC message.
    let resp = ServerValue::RESP(RESP::SimpleString(format!(
        "FULLRESYNC {} {}",
        server.replication.replid.clone(),
        server.replication.repl_offset.to_string()
    )));

    request.data(resp).await;

    // Generate the RDB data for the server.
    let rdb = server.generate_rdb();

    // Calculate the length of the RDB data.
    let rdb_len = RESP::RDBPrefix(rdb.len());

    // Send the RDB length.
    request.data(ServerValue::RESP(rdb_len)).await;

    // Send the RDB data.
    request.data(ServerValue::Binary(rdb)).await;

    // Add the replica to the pool.
    server.replica_senders.push(request.sender.clone());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::{ServerMessage, ServerValue};
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command can send
    // a `FULLRESYNC` request and that the server
    // responds with the correct value.
    async fn test_command() {
        let mut server = Server::new("localhost".to_string(), 6379);

        let cmd = vec![String::from("psync"), String::from("?"), String::from("-1")];

        let (request_channel_tx, mut request_channel_rx) = mpsc::channel::<ServerMessage>(32);

        let request = Request {
            value: RESP::Null,
            sender: request_channel_tx.clone(),
            binary: Vec::new(),
        };

        server.replication.replid = String::from("some_repl_id");
        server.replication.repl_offset = 1234;

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::RESP(RESP::SimpleString(String::from(
                "FULLRESYNC some_repl_id 0"
            ))))
        );
    }
}
