use crate::resp_result::{RESPError, RESPResult};

// Extracts bytes from the buffer until a `\r\n` is reached.
fn binary_extract_line(buffer: &[u8], index: &mut usize) -> RESPResult<Vec<u8>> {
    // The extracted bytes will be stored in this vector.
    let mut output = Vec::new();

    // Exit with an error if we are trying to read
    // after the end of the buffer.
    if *index >= buffer.len() {
        return Err(RESPError::OutOfBounds(*index));
    }

    // Since the terminator \r\n is made of two characters,
    // we keep track of the previous element we read.
    // We start with a clone of the initial element of
    // the buffer.
    let mut previous_elem: u8 = buffer[*index].clone();

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
        previous_elem = elem.clone();
    }

    // If the previous element is not \n
    // we are out of bounds
    if !separator_found {
        *index = final_index;
        return Err(RESPError::OutOfBounds(*index));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function binary_extract_line
    // correctly detects the \r\n, extracts the
    // bytes before it, and updates the index.
    fn test_binary_extract_line() {
        let buffer = "OK\r\n".as_bytes();
        let mut index: usize = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, "OK".as_bytes());
        assert_eq!(index, 4);
    }

    #[test]
    // Make sure the function binary_extract_line
    // doesn't have any hardcoded values testing
    // a different use case.
    fn test_binary_extract_line_longer_string() {
        let buffer = "ECHO\r\n".as_bytes();
        let mut index: usize = 0;

        let output = binary_extract_line(buffer, &mut index).unwrap();

        assert_eq!(output, "ECHO".as_bytes());
        assert_eq!(index, 6);
    }

    #[test]
    // Test that the function binary_extract_line
    // returns the correct error when we try to
    // read an empty buffer.
    fn test_binary_extract_line_empty_buffer() {
        let buffer = "".as_bytes();
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RESPError::OutOfBounds(index)) => {
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
        let buffer = "OK".as_bytes();
        let mut index: usize = 0;

        match binary_extract_line(buffer, &mut index) {
            Err(RESPError::OutOfBounds(index)) => {
                assert_eq!(index, 2);
            }
            _ => panic!(),
        }
    }
}
