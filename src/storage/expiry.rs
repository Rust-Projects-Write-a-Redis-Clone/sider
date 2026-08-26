use super::*;

impl Storage {
    // Turns on the storage active expiry.
    pub fn set_active_expiry(&mut self, value: bool) {
        self.active_expiry = value;
    }

    // Check all keys with an expiry time.
    // If the key has expired remove it from the storage.
    pub fn expire_keys(&mut self) {
        // This function works only if active
        // expiry is turned on.
        if !self.active_expiry {
            return;
        }

        // Get the current time.
        let now = SystemTime::now();

        // Filter all expiry keys and find
        // those that expired in the past.
        let expired_keys: Vec<String> = self
            .expiry
            .iter()
            .filter_map(|(key, &value)| if value < now { Some(key.clone()) } else { None })
            .collect();

        // Remove all expired keys from the storage
        // and from the list of expiry keys.
        for k in expired_keys {
            self.store.remove(&k);
            self.expiry.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the function expire_keys removes
    // keys that have an expiry time in the past.
    fn test_expire_keys() {
        let mut storage: Storage = Storage::new();

        storage
            .set(String::from("akey"), String::from("avalue"), SetArgs::new())
            .unwrap();

        storage.expiry.insert(
            String::from("akey"),
            SystemTime::now() - Duration::from_secs(5),
        );

        storage.expire_keys();
        assert_eq!(storage.store.len(), 0);
    }

    #[test]
    // Test that the function expire_keys doesn't remove
    // keys that have an expiry time in the past.
    // if active expiry is turned off.
    fn test_expire_keys_deactivated() {
        let mut storage: Storage = Storage::new();
        storage.set_active_expiry(false);

        storage
            .set(String::from("akey"), String::from("avalue"), SetArgs::new())
            .unwrap();

        storage.expiry.insert(
            String::from("akey"),
            SystemTime::now() - Duration::from_secs(5),
        );

        storage.expire_keys();
        assert_eq!(storage.store.len(), 1);
    }
}
