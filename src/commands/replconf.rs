use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &Server, request: &Request, _command: &[String]) {
    request
        .data(ServerValue::Resp(Resp::simple_string("OK")))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command processes
    // a `REPLCONF listening-port` request
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
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK")))
        )
    }

    #[tokio::test]
    // Test that the function command processes
    // a `REPLCONF capa` request and
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
        };

        command(&server, &request, &cmd).await;

        assert_eq!(
            connection_receiver.try_recv().unwrap(),
            ServerMessage::Data(ServerValue::Resp(Resp::simple_string("OK")))
        )
    }
}
