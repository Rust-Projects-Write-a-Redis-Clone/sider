use rand::{
    distr::{Alphanumeric, SampleString},
    rng,
};

#[derive(Debug, PartialEq)]
pub enum Role {
    Master,
    Replica,
}

#[derive(Debug, PartialEq)]
pub struct ReplicationInfo {
    pub role: Role,
    pub replid: String,
    pub repl_offset: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct MasterConfig {
    pub host: String,
    pub port: u16,
}

pub struct ReplicationConfig {
    pub master: Option<MasterConfig>,
    pub replid: String,
    pub repl_offset: usize,
    pub pending_write_operations: bool,
}

impl ReplicationConfig {
    pub fn new_master() -> Self {
        Self {
            master: None,
            replid: Alphanumeric.sample_string(&mut rng(), 40),
            repl_offset: 0,
            pending_write_operations: false,
        }
    }

    pub fn new_replica(master_host: String, master_port: u16) -> Self {
        Self {
            master: Some(MasterConfig {
                host: master_host,
                port: master_port,
            }),
            replid: Alphanumeric.sample_string(&mut rng(), 40),
            repl_offset: 0,
            pending_write_operations: false,
        }
    }

    // Return the current replication configuration.
    pub fn info(&self) -> ReplicationInfo {
        ReplicationInfo {
            role: match &self.master {
                Some(_) => Role::Replica,
                None => Role::Master,
            },
            replid: self.replid.clone(),
            repl_offset: self.repl_offset,
        }
    }

    pub fn is_replica(&self) -> bool {
        self.master.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_config_master() {
        let config = ReplicationConfig::new_master();

        assert_eq!(config.master, None);
        assert_eq!(config.replid.len(), 40);
        assert_eq!(config.repl_offset, 0);
    }

    #[test]
    fn test_replication_config_replica() {
        let config = ReplicationConfig::new_replica(String::from("other"), 1234);

        assert_eq!(
            config.master,
            Some(MasterConfig {
                host: String::from("other"),
                port: 1234
            })
        );
        assert_eq!(config.replid.len(), 40);
        assert_eq!(config.repl_offset, 0);
    }

    #[test]
    // Test that the replication configuration
    // returns the correct information when
    // in master mode.
    fn test_replication_info_master() {
        let config = ReplicationConfig::new_master();

        assert_eq!(config.info().role, Role::Master);
        assert_eq!(config.info().replid.len(), 40);
        assert_eq!(config.info().repl_offset, 0);
    }

    #[test]
    // Test that the replication configuration
    // returns the correct information when
    // in replica mode.
    fn test_replication_info_replica() {
        let config = ReplicationConfig::new_replica(String::from("other"), 1234);

        assert_eq!(config.info().role, Role::Replica);
        assert_eq!(config.info().replid.len(), 40);
        assert_eq!(config.info().repl_offset, 0);
    }
}
