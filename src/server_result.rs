use crate::resp::Resp;

#[derive(Debug, PartialEq)]
pub enum ServerError {}

#[derive(Debug)]
pub enum ServerMessage {
    Data(Resp),
    Error(ServerError),
}
