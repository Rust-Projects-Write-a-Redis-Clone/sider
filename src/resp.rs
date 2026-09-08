use crate::resp_result::{RespError, RespLength, RespResult};
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum Resp {
    Array(Vec<Self>),
    BulkString(String),
    Integer(i64),
    Null,
    NullArray,
    RDBPrefix(usize),
    SimpleError(String),
    SimpleString(String),
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Array(data) => {
                write!(f, "*{}\r\n", data.len())?;
                for elem in data.iter() {
                    write!(f, "{}", elem)?;
                }
                Ok(())
            }
            Self::BulkString(data) => write!(f, "${}\r\n{}\r\n", data.len(), data),
            Self::Integer(data) => write!(f, ":{}\r\n", data),
            Self::Null => write!(f, "$-1\r\n"),
            Self::NullArray => write!(f, "*-1\r\n"),
            Self::RDBPrefix(data) => write!(f, "${}\r\n", data),
            Self::SimpleError(data) => write!(f, "-{}\r\n", data),
            Self::SimpleString(data) => write!(f, "+{}\r\n", data),
        }
    }
}

impl Resp {
    pub fn bulk_string(s: impl Into<String>) -> Self {
        Self::BulkString(s.into())
    }

    pub fn simple_error(s: impl Into<String>) -> Self {
        Self::SimpleError(s.into())
    }

    pub fn simple_string(s: impl Into<String>) -> Self {
        Self::SimpleString(s.into())
    }

    pub fn bulk_array<I, T>(items: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self::Array(items.into_iter().map(Self::bulk_string).collect())
    }

    pub fn wrong_type_error() -> Self {
        Self::SimpleError(String::from(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
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

// Extracts a given amount of bytes from the buffer.
fn binary_extract_bytes(buffer: &[u8], index: &mut usize, length: usize) -> RespResult<Vec<u8>> {
    // The output vector that will contain the
    // extracted bytes.
    let mut output = Vec::new();

    // Check if we are allowed to read
    // the requested amount of bytes.
    if *index + length > buffer.len() {
        return Err(RespError::OutOfBounds(buffer.len()));
    }

    // Copy the bytes from the buffer into
    // the output vector.
    output.extend_from_slice(&buffer[*index..*index + length]);

    // Update the index.
    *index += length;

    Ok(output)
}

// Extracts a single line from a RESP buffer and interprets it as length.
// The type used for the number is RespLength.
pub fn resp_extract_length(buffer: &[u8], index: &mut usize) -> RespResult<RespLength> {
    // Extract all bytes until a terminator is found
    // and transform them into a String.
    let line: String = binary_extract_line_as_string(buffer, index)?;

    // Convert the String into RespLength.
    let length: RespLength = line.parse()?;

    Ok(length)
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

// Parse a bulk string in the form `$NUM\r\nVALUE\r\n`.
fn parse_bulk_string(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    // Remove the type for a bulk string.
    resp_remove_type('$', buffer, index)?;

    // Read bytes from the buffer and interpret them
    // as the length of the bulk string.
    let length = resp_extract_length(buffer, index)?;

    // If the length is -1 we are looking
    // at an empty bulk string.
    if length == -1 {
        return Ok(Resp::Null);
    }

    // If the length is negative there is an error.
    if length < -1 {
        return Err(RespError::IncorrectLength(length));
    }

    // Read all possible bytes from the buffer
    let bytes = binary_extract_bytes(buffer, index, length as usize)?;

    // Convert the bytes into UTF-8.
    let data: String = String::from_utf8(bytes)?;

    // Increment the index to skip the \r\n.
    *index += 2;

    Ok(Resp::BulkString(data))
}

// Parse an array in the form `*NUM\r\nELEM1\r\nELEM2\r\n...`.
fn parse_array(buffer: &[u8], index: &mut usize) -> RespResult<Resp> {
    // Remove the type for an array.
    resp_remove_type('*', buffer, index)?;

    // Read bytes from the buffer and interpret them
    // as the number of elements in the array.
    let length = resp_extract_length(buffer, index)?;

    // If the length is negative there is an error.
    if length < 0 {
        return Err(RespError::IncorrectLength(length));
    }

    // The output vector that will contain the elements
    // extracted from the array.
    let mut data = Vec::new();

    // Extract all elements.
    for _ in 0..length {
        // Automatically detect the type of element
        // and parse it.
        match parser_router(buffer, index) {
            Some(parse_func) => {
                // Run the selected parser function
                // for the current element.
                let array_element: Resp = parse_func(buffer, index)?;

                // Store the parsed element in the vector.
                data.push(array_element);
            }
            None => return Err(RespError::Unknown),
        }
    }

    Ok(Resp::Array(data))
}

// Select the correct parsing function according to the data type found
// in the buffer at the given index.
fn parser_router(buffer: &[u8], index: &usize) -> Option<RespParser> {
    // Get the character at the current index
    // and associate it with a parsing function.
    match buffer[*index] {
        b'+' => Some(parse_simple_string),
        b'$' => Some(parse_bulk_string),
        b'*' => Some(parse_array),
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

// Create a bulk string from a vector of strings.
pub fn bulk_string_from_vec(strings: &[String]) -> Resp {
    Resp::BulkString(strings.join("\r\n"))
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
    // Test that the function binary_extract_bytes
    // correctly extracts the requested number of bytes
    // from a buffer, updating the index.
    fn test_binary_extract_bytes() {
        let buffer = b"SOMEBYTES";
        let mut index: usize = 0;

        let output = binary_extract_bytes(buffer, &mut index, 6).unwrap();

        assert_eq!(output, b"SOMEBY");
        assert_eq!(index, 6);
    }

    #[test]
    // Test that the function binary_extract_bytes
    // returns the correct error when we try to
    // read more bytes than are available in the buffer.
    fn test_binary_extract_bytes_out_of_bounds() {
        let buffer = b"SOMEBYTES";
        let mut index: usize = 0;

        let error = binary_extract_bytes(buffer, &mut index, 10).unwrap_err();

        assert_eq!(error, RespError::OutOfBounds(9));
        assert_eq!(index, 0);
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
    // Test that the function parse_bulk_string
    // returns the correct RESP variant when
    // we read a buffer that contains a
    // RESP bulk string.
    fn test_parse_bulk_string() {
        let buffer = b"$2\r\nOK\r\n";
        let mut index: usize = 0;

        let output = parse_bulk_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::bulk_string("OK"));
        assert_eq!(index, 8);
    }

    #[test]
    // Test that the function parse_bulk_string
    // returns the correct RESP variant when
    // we read a buffer that contains an empty
    // RESP bulk string.
    fn test_parse_bulk_string_empty() {
        let buffer = b"$-1\r\n";
        let mut index: usize = 0;

        let output = parse_bulk_string(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::Null);
        assert_eq!(index, 5);
    }

    #[test]
    // Test that the function parse_bulk_string
    // returns the correct error when we try to
    // parse a RESP bulk string with an unparsable length.
    fn test_parse_bulk_string_unparsable_length() {
        let buffer = b"$wrong\r\nOK\r\n";
        let mut index: usize = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::ParseInt);
        assert_eq!(index, 8);
    }

    #[test]
    // Test that the function parse_bulk_string
    // returns the correct error when we try to
    // parse a RESP bulk string with a negative
    // length less than -1.
    fn test_parse_bulk_string_negative_length() {
        let buffer = b"$-7\r\nOK\r\n";
        let mut index: usize = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::IncorrectLength(-7));
        assert_eq!(index, 5);
    }

    #[test]
    // Test that the function parse_bulk_string
    // returns the correct error when we try to
    // parse a RESP bulk string but the buffer
    // doesn't contain enough bytes.
    fn test_parse_bulk_string_data_too_short() {
        let buffer = b"$7\r\nOK\r\n";
        let mut index: usize = 0;

        let error = parse_bulk_string(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::OutOfBounds(8));
        assert_eq!(index, 4);
    }

    #[test]
    // Test that the function parse_array
    // returns the correct RESP variant when
    // we read a buffer that contains an
    // array of two elements, a simple string
    // and a bulk string.
    fn test_parse_array() {
        let buffer = b"*2\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index: usize = 0;

        let output = parse_array(buffer, &mut index).unwrap();

        assert_eq!(
            output,
            Resp::Array(vec![Resp::simple_string("OK"), Resp::bulk_string("VALUE")])
        );
        assert_eq!(index, 20);
    }

    #[test]
    // Test that the function parse_array
    // returns the correct error when we try to
    // parse an array with negative length.
    fn test_parse_array_invalid_length() {
        let buffer = b"*-1\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index: usize = 0;

        let error = parse_array(buffer, &mut index).unwrap_err();

        assert_eq!(error, RespError::IncorrectLength(-1));
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
    // that the function bytes_to_resp can parse
    // a buffer with a RESP bulk string.
    fn test_bytes_to_resp_bulk_string() {
        let buffer = b"$2\r\nOK\r\n";
        let mut index: usize = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(output, Resp::bulk_string("OK"));
        assert_eq!(index, 8);
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

    #[test]
    // Test the parsing process end-to-end, checking
    // that the function bytes_to_resp can parse
    // a buffer with a RESP array.
    fn test_bytes_to_resp_array() {
        let buffer = b"*2\r\n+OK\r\n$5\r\nVALUE\r\n";
        let mut index: usize = 0;

        let output = bytes_to_resp(buffer, &mut index).unwrap();

        assert_eq!(
            output,
            Resp::Array(vec![Resp::simple_string("OK"), Resp::bulk_string("VALUE")])
        );
        assert_eq!(index, 20);
    }

    #[test]
    fn test_bulk_string_from_vec() {
        let strings = vec![
            String::from("First string"),
            String::from("Second string"),
            String::from("Third string"),
        ];

        let bulk_string: Resp = bulk_string_from_vec(&strings);

        assert_eq!(
            bulk_string,
            Resp::bulk_string("First string\r\nSecond string\r\nThird string")
        );
    }
}
