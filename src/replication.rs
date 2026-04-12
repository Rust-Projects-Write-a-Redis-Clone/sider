#[derive(Debug, PartialEq)]
pub struct MasterConfig {
    pub host: String,
    pub port: u16,
}

pub struct ReplicationConfig {
    pub master: Option<MasterConfig>,
}

impl ReplicationConfig {
    pub fn new_master() -> Self {
        ReplicationConfig { master: None }
    }

    pub fn new_replica(master_host: String, master_port: u16) -> Self {
        ReplicationConfig {
            master: Some(MasterConfig {
                host: master_host,
                port: master_port,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // Test that the replication configuration
    // can be initialised for a master.
    fn test_replication_config_master() {
        let config = ReplicationConfig::new_master();

        assert_eq!(config.master, None);
    }

    #[test]
    // Test that the replication configuration
    // can be initialised for a replica.
    fn test_replication_config_replica() {
        let config = ReplicationConfig::new_replica(String::from("other"), 1234);

        assert_eq!(
            config.master,
            Some(MasterConfig {
                host: String::from("other"),
                port: 1234
            })
        );
    }
}
