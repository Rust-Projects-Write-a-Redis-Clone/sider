use super::*;

impl Storage {
    // Implement the `llen` operation for the storage.
    pub fn llen(&self, key: &str) -> StorageResult<usize> {
        // Get an immutable reference to the value
        // of the key and act according to its
        // nature.
        let length = match self.store.get(key) {
            // The value is a string, we cannot
            // use llen on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => return Err(StorageError::WrongType),

            // The value is a list.
            // Return its length.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => v.len(),

            // The key doesn't exist.
            // Return 0
            None => 0,
        };

        Ok(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function llen works as expected
    // when the key doesn't exist.
    fn test_llen_key_does_not_exist() {
        let storage: Storage = Storage::new();

        let output = storage.llen("akey").unwrap();

        assert_eq!(output, 0);
        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function llen works as expected
    // when the key exists and is a list.
    fn test_llen_key_exists_and_is_list() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1"]),
        );

        let output = storage.llen("akey").unwrap();

        assert_eq!(output, 2);
        assert_eq!(storage.store.len(), 1);
    }

    #[test]
    // Test that the function llen
    // returns the correct error when
    // the key exists and is not a list.
    fn test_llen_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.llen("akey");

        assert_eq!(error, Err(StorageError::WrongType));
    }
}
