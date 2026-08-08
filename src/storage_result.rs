use std::fmt;

#[derive(Debug, PartialEq)]
pub enum StorageError {
    IncorrectRequest,
    CommandNotAvailable(String),
    CommandSyntaxError(String),
    CommandInternalError(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncorrectRequest => {
                write!(f, "The client sent an incorrect request!")
            }
            Self::CommandNotAvailable(c) => {
                write!(f, "The requested command {} is not available!", c)
            }
            Self::CommandSyntaxError(string) => {
                write!(f, "Syntax error while processing {}!", string)
            }
            Self::CommandInternalError(string) => {
                write!(f, "Internal error while processing {}!", string)
            }
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;
