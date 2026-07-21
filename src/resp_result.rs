use std::fmt;
use std::string::FromUtf8Error;

#[derive(Debug, PartialEq)]
pub enum RespError {
    FromUtf8,
    OutOfBounds(usize),
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FromUtf8 => write!(f, "Cannot convert from UTF-8"),
            Self::OutOfBounds(index) => write!(f, "Out of bounds at index {}", index),
        }
    }
}

impl From<FromUtf8Error> for RespError {
    fn from(_err: FromUtf8Error) -> Self {
        Self::FromUtf8
    }
}

pub type RespResult<T> = Result<T, RespError>;
