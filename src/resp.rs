use crate::resp_result::{RespError, RespResult};
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Resp {
    SimpleString(String),
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SimpleString(data) => write!(f, "+{}\r\n", data),
        }
    }
}

impl Resp {
    pub fn simple_string(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }
}

// A function that parses a slice of bytes at the given index into a Resp value.
type RespParser = fn(&[u8], &mut usize) -> RespResult<Resp>;

// Extracts bytes from the buffer until a `\r\n` is reached.
fn binary_extract_line(buffer: &[u8], index: &mut usize) -> RespResult<Vec<u8>> {
    // The extracted bytes will be stored in this vector.
    let mut output = Vec::new();

    // Exit with an error if we are trying to read
    // after the end of the buffer.
    if *index >= buffer.len() {
        return Err(RespError::OutOfBounds(*index));
    }

    // Since the terminator \r\n is made of two characters,
    // we keep track of the previous element we read.
    // We start with a clone of the initial element of
    // the buffer.
    let mut previous_elem: u8 = buffer[*index];

    // A flag to signal that we found the characters \r\n.
    let mut separator_found: bool = false;

    // We don't want to change the index until
    // we are sure the function succeeds.
    // This is a temporary index to keep track
    // of what we are reading in the buffer.
    let mut final_index: usize = *index;

    // Scan the whole buffer looking for \r\n.
    // Keep track of the current index and
    // of the previous element.
    for &elem in buffer[*index..].iter() {
        // Advance the index, consider the
        // current element valid.
        final_index += 1;

        // Check if we just passed the terminator \r\n.
        if elem == b'\n' && previous_elem == b'\r' {
            // Toggle the flag to signal that all is good.
            separator_found = true;

            break;
        }

        // Store the current element for the next loop.
        previous_elem = elem;
    }

    // If the previous element is not \n
    // we are out of bounds
    if !separator_found {
        *index = final_index;
        return Err(RespError::OutOfBounds(*index));
    }

    // Copy the bytes from the buffer to the output vector.
    // Skip the final \r\n removing two bytes.
    output.extend_from_slice(&buffer[*index..final_index - 2]);

    // Make sure the index passed to the function
    // is updated with the final position,
    // including the terminator \r\n.
    *index = final_index;

    Ok(output)
}

// Extracts bytes from the buffer until a `\r\n` is reached and converts them into a string.
pub fn binary_extract_line_as_string(buffer: &[u8], index: &mut usize) -> RespResult<String> {
    // Extract all possible bytes updating the index.
    let line = binary_extract_line(buffer, index)?;

    // Convert the bytes to a UTF-8 String.
    Ok(String::from_utf8(line)?)
}

// Checks that the first character of a RESP buffer is the given one and removes it.
pub fn resp_remove_type(value: char, buffer: &[u8], index: &mut usize) -> RespResult<()> {
    // Check if the character at the given position
    // of the buffer corresponds to the provided value.
    if buffer[*index] != value as u8 {
        return Err(RespError::WrongType);
    }

    // Increment the index to skip the type character.
    *index += 1;

    Ok(())
}

// Parse a simple string in the form `+VALUE\r\n`.
fn parse_simple_string(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    // Remove the type for a simple string.
    resp_remove_type('+', buffer, index)?;

    // Read all possible bytes and convert them into UTF-8.
    let line: String = binary_extract_line_as_string(buffer, index)?;

    // Add the String to a custom RESP type.
    Ok(Resp::SimpleString(line))
}

// Select the correct parsing function according to the data type found
// in the buffer at the given index.
fn parser_router(buffer: &[u8], index: &usize) -> Option<RespParser> {
    // Get the character at the current index
    // and associate it with a parsing function.
    match buffer[*index] {
        b'+' => Some(parse_simple_string),
        _ => None,
    }
}

// Parse the bytes of the buffer at the given index and return a RESP data type.
pub fn bytes_to_resp(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    // Call the selected parsing function and
    // manage the result.
    match parser_router(buffer, index) {
        Some(parse_func) => {
            let result: Resp = parse_func(buffer, index)?;
            Ok(result)
        }
        None => Err(RespError::Unknown),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function binary_extract_line
    // correctly detects the \r\n, extracts the
    // bytes before it, and updates the index.
    fn test_binary_extract_line() {
        let buffer = b"OK\r\n";
        let mut index: usize = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, b"OK");
        assert_eq!(index, 4);
    }

    #[test]
    // Make sure the function binary_extract_line
    // doesn't have any hardcoded values testing
    // a different use case.
    fn test_binary_extract_line_longer_string() {
        let buffer = b"ECHO\r\n";
        let mut index: usize = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, b"ECHO");
        assert_eq!(index, 6);
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read an empty buffer.
    fn test_binary_extract_line_empty_buffer() {
        let buffer = b"";
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 0);
            }
            _ => panic!(),
        }
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read a buffer that doesn't contain the
    // terminator \r\n.
    fn test_binary_extract_line_no_separator() {
        let buffer = b"OK";
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read bytes starting with an index that is
    // already greater than the length of the buffer.
    fn test_binary_extract_line_index_too_advanced() {
        let buffer = b"OK";
        let mut index: usize = 1;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 2);
            }
            _ => panic!(),
        }
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read a buffer that contains only \r.
    fn test_binary_extract_line_half_separator() {
        let buffer = b"OK\r";
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read a buffer that contains only \n.
    fn test_binary_extract_line_incorrect_separator() {
        let buffer = b"OK\n";
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RespError::OutOfBounds(index)) => {
                assert_eq!(index, 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    // Test that the function binary_extract_line_as_string
    // correctly converts a buffer with a terminator
    // into a String.
    fn test_binary_extract_line_as_string() {
        let buffer = b"OK\r\n";
        let mut index: usize = 0;

        let output = binary_extract_line_as_string(buffer, &mut index).unwrap();

        assert_eq!(output, "OK");
        assert_eq!(index, 4);
    }

    #[test]
    // Test that the function binary_extract_line_as_string
    // returns the correct error when we try to
    // read a buffer that contains an invalid UTF-8
    // sequence of bytes.
    fn test_binary_extract_line_as_string_invalid_utf8() {
        let buffer: Vec<u8> = vec![0xFF, 0xFE, b'\r', b'\n'];
        let mut index: usize = 0;

        let error = binary_extract_line_as_string(&buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::FromUtf8);
    }

    #[test]
    // Test that the function resp_remove_type
    // checks and returns the given type from a buffer
    // and updates the index.
    fn test_resp_remove_type() {
        let buffer = b"+OK\r\n";
        let mut index: usize = 0;

        resp_remove_type('+', buffer, &mut index).unwrap();

        assert_eq!(index, 1);
    }

    #[test]
    // Test that the function resp_remove_type
    // returns the correct error when we try to
    // read a given type from a buffer that
    // contains a different type.
    fn test_resp_remove_type_error() {
        let buffer = b"*OK\r\n";
        let mut index: usize = 0;

        let error = resp_remove_type('+', buffer, &mut index).unwrap_err();

        assert_eq!(index, 0);
        assert_eq!(error, RespError::WrongType);
    }

    #[test]
    // Test that the function parse_simple_string
    // returns the correct Resp variant when
    // we read a buffer that contains a
    // Resp simple string.
    fn test_parse_simple_string() {
        let buffer = b"+OK\r\n";
        let mut index: usize = 0;

        let output = parse_simple_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::simple_string("OK"));
        assert_eq!(index, 5);
    }

    #[test]
    // Test the parsing process end-to-end, checking
    // that the function bytes_to_resp can parse
    // a buffer with a RESP simple string.
    fn test_bytes_to_resp_simple_string() {
        let buffer = b"+OK\r\n";
        let mut index: usize = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::simple_string("OK"));
        assert_eq!(index, 5);
    }

    #[test]
    // Test the parsing process end-to-end, checking
    // that the function bytes_to_resp can returns
    // the correct error when used to parse a
    // buffer with an unknown data type.
    fn test_bytes_to_resp_unknown() {
        let buffer = b"?OK\r\n";
        let mut index: usize = 0;

        let error = bytes_to_resp(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::Unknown);
        assert_eq!(index, 0);
    }
}
