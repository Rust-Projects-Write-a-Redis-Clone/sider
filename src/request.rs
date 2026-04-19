use crate::{
    resp::RESP,
    server_result::{ServerError, ServerMessage, ServerResult, ServerValue},
};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Request {
    pub value: RESP,
    pub sender: mpsc::Sender<ServerMessage>,
    pub binary: Vec<u8>,
    pub client_id: u64,
    pub master_connection: bool,
}

impl Request {
    pub async fn error(&self, e: ServerError) {
        self.sender.send(ServerMessage::Error(e)).await.unwrap();
    }

    pub async fn data(&self, d: ServerValue) {
        self.sender.send(ServerMessage::Data(d)).await.unwrap();
    }

    pub async fn result(&self, r: ServerResult) {
        self.sender.send(ServerMessage::from(r)).await.unwrap();
    }
}
