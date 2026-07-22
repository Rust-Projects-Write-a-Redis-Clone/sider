use crate::{
    resp::Resp,
    server_result::{ServerError, ServerMessage, ServerResult, ServerValue},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct Request {
    pub value: Resp,
    pub sender: mpsc::Sender<ServerMessage>,
    pub binary: Vec<u8>,
    pub client_id: u64,
    pub master_connection: bool,
}

impl Request {
    // The pattern `let _ = self.sender.send(...).await` here
    // is justified by the fact that send fails only if the
    // client dropped the connection while waiting for a
    // command to be completed.
    // Using unwrap() would kill the task that is handling
    // the connection, but it feels cleaner to basically say
    // "I sent the message, it's OK if it fails because no one
    // is listening".

    pub async fn error(&self, e: ServerError) {
        let _ = self.sender.send(ServerMessage::Error(e)).await;
    }

    pub async fn data(&self, d: ServerValue) {
        let _ = self.sender.send(ServerMessage::Data(d)).await;
    }

    pub async fn result(&self, r: ServerResult) {
        let _ = self.sender.send(ServerMessage::from(r)).await;
    }
}
