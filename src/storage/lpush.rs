use super::*;

impl Storage {
    // Implement the `lpush` operation for the storage.
    pub fn lpush(&mut self, key: &str, values: &[String]) -> StorageResult<usize> {
        // Get a mutable reference to the value
        // of the key and act according to its
        // nature.
        let length = match self.store.get_mut(key) {
            // The value is a string, we cannot
            // use lpush on it.
            Some(StorageData {
                value: StorageValue::String(_),
                ..
            }) => return Err(StorageError::WrongType),

            // The value is already a list. Add
            // the new element and return the
            // length of the whole list.
            Some(StorageData {
                value: StorageValue::List(v),
                ..
            }) => {
                // Create an iterator of &String, reverse it,
                // own each element cloning into String.
                // Add elements at index 0.
                v.splice(0..0, values.iter().rev().cloned());
                v.len()
            }

            // The key doesn't exist. Create it,
            // add all elements in reversed order,
            // and return its length.
            None => {
                // Create an iterator of &String, reverse it,
                // own each element cloning into String.
                // Collect the iterator into a vector.
                let reversed: Vec<String> = values.iter().rev().cloned().collect();
                self.store
                    .insert(key.to_string(), StorageData::list(reversed));
                values.len()
            }
        };

        Ok(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function lpush works as expected
    // when the key doesn't exist.
    // The key is created as a list and the value is
    // added to it.
    fn test_lpush_key_does_not_exist() {
        let mut storage: Storage = Storage::new();
        let avalue = StorageData::list(["avalue"]);

        let output = storage.lpush("akey", &[String::from("avalue")]).unwrap();

        assert_eq!(output, 1);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &avalue),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpush works as expected
    // when inserting elements.
    // Elements are prepended in reverse order, so
    // `lpush a b c` prepends `c b a`.
    fn test_lpush_order() {
        let mut storage: Storage = Storage::new();

        let expected = StorageData::list(["c", "b", "a"]);

        let output = storage
            .lpush(
                "akey",
                &[String::from("a"), String::from("b"), String::from("c")],
            )
            .unwrap();

        assert_eq!(output, 3);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &expected),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpush works as expected
    // when the key exists and is a list.
    // The value is inserted at the start.
    fn test_lpush_key_exists_and_is_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::list(["value1"]));

        let list = StorageData::list(["value0", "value1"]);

        let output = storage.lpush("akey", &[String::from("value0")]).unwrap();

        assert_eq!(output, 2);
        assert_eq!(storage.store.len(), 1);
        match storage.store.get("akey") {
            Some(value) => assert_eq!(value, &list),
            None => panic!(),
        }
    }

    #[test]
    // Test that the function lpush
    // returns the correct error when
    // the key exists and is not a list.
    fn test_lpush_key_exists_and_is_not_list() {
        let mut storage: Storage = Storage::new();

        storage
            .store
            .insert(String::from("akey"), StorageData::string("avalue"));

        let error = storage.lpush("akey", &[String::from("value2")]);

        assert_eq!(error, Err(StorageError::WrongType));
    }
}
