use std::fmt;

#[derive(Debug, PartialEq)]
pub enum StorageError {
    CommandSyntaxError(String),
    WrongType,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandSyntaxError(string) => {
                write!(f, "Syntax error while processing {}!", string)
            }
            Self::WrongType => {
                write!(f, "Wrong type")
            }
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;
