use std::fmt;
use std::num;
use std::string::FromUtf8Error;

#[derive(Debug, PartialEq)]
pub enum RespError {
    FromUtf8,
    IncorrectLength(RespLength),
    OutOfBounds(usize),
    ParseInt,
    Unknown,
    WrongType,
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FromUtf8 => write!(f, "Cannot convert from UTF-8"),
            Self::IncorrectLength(length) => write!(f, "Incorrect length {}", length),
            Self::OutOfBounds(index) => write!(f, "Out of bounds at index {}", index),
            Self::ParseInt => write!(f, "Cannot parse string into integer"),
            Self::Unknown => write!(f, "Unknown format for RESP string"),
            Self::WrongType => write!(f, "Wrong prefix for RESP type"),
        }
    }
}

impl From<FromUtf8Error> for RespError {
    fn from(_err: FromUtf8Error) -> Self {
        Self::FromUtf8
    }
}

impl From<num::ParseIntError> for RespError {
    fn from(_err: num::ParseIntError) -> Self {
        Self::ParseInt
    }
}

pub type RespResult<T> = Result<T, RespError>;

pub type RespLength = i32;
