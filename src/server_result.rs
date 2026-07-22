use crate::resp::Resp;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ServerError {
    CommandInternalError(String),
    CommandNotAvailable(String),
    CommandSyntaxError(String),
    HandshakeFailed(String),
    IncorrectData,
    StorageNotInitialised,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandInternalError(string) => {
                write!(f, "Internal error while processing {}.", string)
            }

            Self::CommandNotAvailable(c) => {
                write!(f, "The requested command {} is not available.", c)
            }

            Self::CommandSyntaxError(string) => {
                write!(f, "Syntax error while processing {}.", string)
            }

            Self::HandshakeFailed(string) => {
                write!(f, "Handshake process failed: {}.", string)
            }

            Self::IncorrectData => {
                write!(f, "Data received from stream is incorrect.")
            }

            Self::StorageNotInitialised => {
                write!(f, "Storage has not been initialised.")
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ServerValue {
    None,
    Resp(Resp),
    Binary(Vec<u8>),
}

pub type ServerResult = Result<ServerValue, ServerError>;

#[derive(Debug, PartialEq)]
pub enum ServerMessage {
    ClientInit(u64),
    Data(ServerValue),
    Error(ServerError),
}

impl From<ServerResult> for ServerMessage {
    fn from(result: ServerResult) -> Self {
        match result {
            Ok(v) => Self::Data(v),
            Err(e) => Self::Error(e),
        }
    }
}
