use crate::resp::RESP;

#[derive(Debug, PartialEq)]
pub enum ServerError {}

#[derive(Debug)]
pub enum ServerMessage {
    Data(RESP),
    Error(ServerError),
}
