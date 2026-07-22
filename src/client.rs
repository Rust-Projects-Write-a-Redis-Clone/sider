use crate::request::Request;
use crate::server_result::ServerMessage;

use tokio::sync::mpsc;

pub struct Client {
    #[expect(dead_code)]
    pub client_id: u64,
    pub sender: mpsc::Sender<ServerMessage>,
    pub requests: Vec<Request>,
    pub multi: bool,
}

impl Client {
    pub fn new(client_id: u64, sender: mpsc::Sender<ServerMessage>) -> Self {
        Self {
            client_id,
            sender,
            requests: Vec::new(),
            multi: false,
        }
    }
}
