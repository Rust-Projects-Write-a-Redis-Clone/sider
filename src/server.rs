use crate::RESP;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ServerError {
    CommandError,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::CommandError => write!(f, "Error while processing!"),
        }
    }
}

pub type ServerResult<T> = Result<T, ServerError>;

// Process an incoming request and return a result.
pub fn process_request(request: RESP) -> ServerResult<RESP> {
    // Check if the request is expressed using
    // a RESP array and extract the elements.
    let elements = match request {
        RESP::Array(v) => v,
        _ => {
            return Err(ServerError::CommandError);
        }
    };

    // The vector that contains all the commands we need to process.
    let mut command = Vec::new();

    // Check that each element of the array is a
    // bulk string, extract the content, and add it
    // to the vector.
    for elem in elements.iter() {
        match elem {
            RESP::BulkString(v) => command.push(v),
            _ => {
                return Err(ServerError::CommandError);
            }
        }
    }

    // Match the first element of the vector (the name
    // of the command) with the code that implements
    // that command.
    match command[0].to_lowercase().as_str() {
        "ping" => Ok(RESP::SimpleString(String::from("PONG"))),
        _ => {
            return Err(ServerError::CommandError);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function process_request
    // processes a PING request and that it
    // responds with a PONG.
    fn test_process_request_ping() {
        let request = RESP::Array(vec![RESP::BulkString(String::from("PING"))]);

        let output = process_request(request).unwrap();

        assert_eq!(output, RESP::SimpleString(String::from("PONG")));
    }

    #[test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that doesn't contain
    // a RESP array.
    fn test_process_request_not_array() {
        let request = RESP::BulkString(String::from("PING"));

        let error = process_request(request).unwrap_err();

        assert_eq!(error, ServerError::CommandError);
    }

    #[test]
    // Test that the function process_request
    // returns the correct error when it is
    // given a request that contains a RESP array
    // but the content of the array is not
    // a bulk string.
    fn test_process_request_not_bulkstrings() {
        let request = RESP::Array(vec![RESP::SimpleString(String::from("PING"))]);

        let error = process_request(request).unwrap_err();

        assert_eq!(error, ServerError::CommandError);
    }
}
