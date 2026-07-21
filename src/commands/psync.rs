use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(server: &mut Server, request: &Request, _command: &[String]) {
    // Reset the master replica offset.
    server.replication.repl_offset = 0;

    // Create the FULLRESYNC message.
    let resp = ServerValue::Resp(Resp::simple_string(format!(
        "FULLRESYNC {} {}",
        server.replication.replid, server.replication.repl_offset
    )));

    request.data(resp).await;
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
            value: Resp::Null,
            sender: request_channel_tx,
        };

        server.replication.replid = String::from("some_repl_id");
        server.replication.repl_offset = 1234;

        command(&mut server, &request, &cmd).await;

        assert_eq!(
            request_channel_rx.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string(
                "FULLRESYNC some_repl_id 0"
            )))
        );
    }
}
