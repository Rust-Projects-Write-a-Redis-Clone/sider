use crate::resp::RESP;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ServerError {}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            _ => todo!(),
        }
    }
}

#[derive(Debug)]
pub enum ServerValue {
    RESP(RESP),
}

#[derive(Debug)]
pub enum ServerMessage {
    Data(ServerValue),
    Error(ServerError),
}
