use crate::resp::Resp;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ServerError {
    IncorrectData,
    StorageNotInitialised,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncorrectData => {
                write!(f, "Data received from stream is incorrect.")
            }
            Self::StorageNotInitialised => {
                write!(f, "Storage has not been initialised.")
            }
        }
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
