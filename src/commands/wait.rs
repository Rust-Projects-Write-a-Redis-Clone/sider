use crate::request::Request;
use crate::resp::Resp;
use crate::server::Server;
use crate::server_result::ServerValue;

pub async fn command(_server: &Server, request: &Request, _command: &[String]) {
    // At this stage WAIT always reports zero replicas.
    request.data(ServerValue::Resp(Resp::Integer(0))).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_result::ServerMessage;
    use tokio::sync::mpsc;

    #[tokio::test]
    // Test that the function command replies with
    // the integer 0 regardless of the arguments.
    async fn test_command_wait_no_replicas() {
        let cmd = vec![String::from("wait"), String::from("0"), String::from("100")];
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
            ServerMessage::Data(ServerValue::Resp(Resp::Integer(0)))
        )
    }
}
