use super::*;

impl Storage {
    // Implement the `lrange` operation for the storage.
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> StorageResult<&[String]> {
        match self.store.get(key) {
            // The value is a string, we cannot
            // use lrange on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => Err(StorageError::WrongType),

            // The value is a list. All good,
            // extract the elements and
            // return them.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => {
                // Make sure everything is i64.
                let length = v.len() as i64;

                // If start is negative add the
                // length of the vector to make it
                // relative to the end. Also, make
                // sure 0 is the minimum value.
                let start = if start < 0 {
                    (start + length).max(0)
                } else {
                    start
                };

                // If stop is negative add the
                // length of the vector to make it
                // relative to the end.
                // If the result is still negative
                // keep it as it is. This will
                // make start > stop and trigger
                // the next check.
                let stop = if stop < 0 { stop + length } else { stop };

                // If start is past the end of
                // the list or if start is greater
                // than stop return an empty
                // vector.
                if start >= length || start > stop {
                    return Ok(&[]);
                }

                // If stop is greater than
                // the length of the list,
                // replace it with the
                // index of the last element.
                let stop = min(stop, length - 1);

                // Go back to usize to index
                // the Rust vector.
                let start = start as usize;
                let stop = stop as usize;

                Ok(&v[start..=stop])
            }

            // The key doesn't exist.
            // Return an empty array.
            None => Ok(&[]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function lrange works as expected
    // when the key doesn't exist.
    fn test_lrange_empty_list() {
        let storage: Storage = Storage::new();

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 1, 3).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange
    // returns the correct error when
    // the key exists and is not a list.
    fn test_lrange_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.lrange("akey", 1, 3);

        assert_eq!(error, Err(StorageError::WrongType));
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list, the indexes
    // are valid, and start is less than stop.
    fn test_lrange() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = vec![
            String::from("value1"),
            String::from("value2"),
            String::from("value3"),
        ];

        let output = storage.lrange("akey", 1, 3).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and start > list length.
    fn test_lrange_start_greater_than_length() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 6, 7).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and start == list length.
    fn test_lrange_start_equal_length() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 5, 7).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and start > stop.
    fn test_lrange_start_greater_than_stop() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 3, 2).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and stop > list length.
    fn test_lrange_stop_greater_than_length() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = vec![String::from("value3"), String::from("value4")];

        let output = storage.lrange("akey", 3, 7).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and stop == list length.
    fn test_lrange_stop_equal_length() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = vec![String::from("value3"), String::from("value4")];

        let output = storage.lrange("akey", 3, 5).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // start > stop, and stop > list length.
    // Start is greater than stop BEFORE this is
    // checked against the length of the list.
    fn test_lrange_start_greater_than_stop_before_threshold() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 12, 10).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and stop > list length
    // Start is greater than stop AFTER this is
    // checked against the length of the list.
    fn test_lrange_start_greater_than_stop_after_threshold() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", 12, 15).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // and both start and stop are negative,
    // but within the boundaries.
    fn test_lrange_start_stop_negative() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = vec![
            String::from("value1"),
            String::from("value2"),
            String::from("value3"),
        ];

        let output = storage.lrange("akey", -4, -2).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // both start and stop are negative,
    // and start is out of range.
    fn test_lrange_start_negative_out_of_range() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = vec![
            String::from("value0"),
            String::from("value1"),
            String::from("value2"),
            String::from("value3"),
        ];

        let output = storage.lrange("akey", -100, -2).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // both start and stop are negative,
    // and stop is out of range.
    fn test_lrange_stop_negative_out_of_range() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", -2, -100).unwrap();

        assert_eq!(output, expected);
    }

    #[test]
    // Test that the function lrange works as expected
    // when the key exists and is a list,
    // both start and stop are negative,
    // and both are out of range.
    fn test_lrange_start_stop_negative_out_of_range() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1", "value2", "value3", "value4"]),
        );

        let expected = Vec::<String>::new();

        let output = storage.lrange("akey", -200, -100).unwrap();

        assert_eq!(output, expected);
    }
}
