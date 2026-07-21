use std::fmt;

#[derive(Debug)]
pub enum RespError {
    OutOfBounds(usize),
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfBounds(index) => write!(f, "Out of bounds at index {}", index),
        }
    }
}

pub type RespResult<T> = Result<T, RespError>;
