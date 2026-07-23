use super::*;

impl Storage {
    // Implement the `lpop` operation for the storage.
    pub fn lpop(&mut self, key: &str) -> StorageResult<Option<String>> {
        // Get a mutable reference to the value
        // of the key and act according to its
        // nature.
        let item = match self.store.get_mut(key) {
            // The value is a string, we cannot
            // use lpop on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => return Err(StorageError::WrongType),

            // The value is a list.
            // Return the first element.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => {
                // If the list is empty we
                // cannot remove elements.
                if v.is_empty() {
                    return Ok(None);
                }

                // Remove and return the first element.
                Some(v.remove(0))
            }

            // The key doesn't exist.
            // Return none.
            None => None,
        };

        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function lpop works as expected
    // when the key doesn't exist.
    fn test_lpop_key_does_not_exist() {
        let mut storage: Storage = Storage::new();

        let output = storage.lpop("akey").unwrap();

        assert_eq!(output, None);
        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is a list.
    fn test_lpop_key_exists_and_is_list() {
        let mut storage: Storage = Storage::new();

        storage.store.insert(
            String::from("akey"),
            StorageData::list(["value0", "value1"]),
        );

        let output = storage.lpop("akey").unwrap();

        assert_eq!(output, Some(String::from("value0")));
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list(["value1"])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop works as expected
    // when the key exists and is an empty list.
    fn test_lpop_key_exists_and_is_empty() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::list(["value0"]));

        // Pop the only value.
        let _ = storage.lpop("akey").unwrap();
        let output = storage.lpop("akey").unwrap();

        assert_eq!(output, None);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &StorageData::list::<[_; 0], String>([])),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpop
    // returns the correct error when
    // the key exists and is not a list.
    fn test_lpop_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.lpop("akey");

        assert_eq!(error, Err(StorageError::WrongType));
    }
}
