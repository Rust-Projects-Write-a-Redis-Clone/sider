use crate::resp::Resp;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ServerError {}

impl fmt::Display for ServerError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {}
    }
}

#[derive(Debug)]
pub enum ServerValue {
    Resp(Resp),
}

#[derive(Debug)]
pub enum ServerMessage {
    Data(ServerValue),
    Error(ServerError),
}
