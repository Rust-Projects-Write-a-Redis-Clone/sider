use crate::{resp::Resp, server_result::ServerMessage};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct Request {
    pub value: Resp,
    pub sender: mpsc::Sender<ServerMessage>,
}
