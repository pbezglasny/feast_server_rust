use anyhow::{Result, anyhow};
use serde::de::Deserializer;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum Provider {
    Local,
    AWS,
    GCP,
    Unknown(String),
}

impl Serialize for Provider {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Provider::Local => serializer.serialize_str("local"),
            Provider::AWS => serializer.serialize_str("aws"),
            Provider::GCP => serializer.serialize_str("gcp"),
            Provider::Unknown(other) => serializer.serialize_str(other.as_str()),
        }
    }
}

impl<'de> Deserialize<'de> for Provider {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let normalized = value.to_ascii_lowercase();
        let provider = match normalized.as_str() {
            "local" => Provider::Local,
            "aws" => Provider::AWS,
            "gcp" => Provider::GCP,
            _ => Provider::Unknown(value),
        };
        Ok(provider)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryType {
    #[default]
    File,
    Sql,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(from = "RegistryConfigDef")]
pub struct RegistryConfig {
    pub path: String,
    pub cache_ttl_seconds: Option<u64>,
    pub registry_type: RegistryType,
    pub account: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub role: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum RegistryConfigDef {
    Path(String),
    Detailed {
        path: String,
        #[serde(default)]
        cache_ttl_seconds: Option<u64>,
        #[serde(default)]
        registry_type: Option<RegistryType>,
        #[serde(default)]
        account: Option<String>,
        #[serde(default)]
        user: Option<String>,
        #[serde(default)]
        password: Option<String>,
        #[serde(default)]
        role: Option<String>,
    },
}

impl From<RegistryConfigDef> for RegistryConfig {
    fn from(value: RegistryConfigDef) -> Self {
        match value {
            RegistryConfigDef::Path(path) => RegistryConfig {
                path,
                ..Default::default()
            },
            RegistryConfigDef::Detailed {
                path,
                cache_ttl_seconds,
                registry_type,
                account,
                user,
                password,
                role,
            } => RegistryConfig {
                path,
                cache_ttl_seconds,
                registry_type: registry_type.unwrap_or_default(),
                account,
                user,
                password,
                role,
            },
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum OnlineStoreType {
    #[default]
    Sqlite,
    Redis,
    DynamoDB,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RedisType {
    #[default]
    SingleNode,
    RedisCluster,
    Sentinel,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OnlineStoreConfig {
    Sqlite {
        path: String,
    },
    Redis {
        #[serde(default)]
        redis_type: RedisType,
        connection_string: String,
        sentinel_master: Option<String>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub enum EntityKeySerializationVersion {
    V1,
    V2,
    #[default]
    V3,
}

impl From<EntityKeySerializationVersion> for u64 {
    fn from(value: EntityKeySerializationVersion) -> Self {
        match value {
            EntityKeySerializationVersion::V1 => 1,
            EntityKeySerializationVersion::V2 => 2,
            EntityKeySerializationVersion::V3 => 3,
        }
    }
}

impl TryFrom<u64> for EntityKeySerializationVersion {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EntityKeySerializationVersion::V1),
            2 => Ok(EntityKeySerializationVersion::V2),
            3 => Ok(EntityKeySerializationVersion::V3),
            _ => Err(format!(
                "unsupported entity_key_serialization_version {}",
                value
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepoConfig {
    pub project: String,
    pub project_description: Option<String>,
    pub provider: Option<Provider>,
    pub registry: RegistryConfig,
    pub online_store: OnlineStoreConfig,
    #[serde(default)]
    pub entity_key_serialization_version: EntityKeySerializationVersion,
}

impl RepoConfig {
    pub fn from_yaml_str(yaml: &str) -> Result<Self> {
        if yaml.trim().is_empty() {
            return Err(anyhow!("Empty configuration file"));
        }
        let config: RepoConfig = serde_saphyr::from_str(yaml).map_err(|err| anyhow!(err))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    #[test]
    fn parse_config_local_sqlite() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/test_data/local_sqlite.yaml", project_dir);
        let yaml_str = fs::read_to_string(config_path)?;
        let repo_config = RepoConfig::from_yaml_str(&yaml_str)?;
        assert_eq!(repo_config.project, "local_sqlite");
        let mut expected_registry = RegistryConfig::default();
        expected_registry.registry_type = RegistryType::File;
        expected_registry.path = "data/registry.db".to_string();
        assert_eq!(repo_config.registry, expected_registry);
        let expected_online_store = OnlineStoreConfig::Sqlite {
            path: "data/online_store.db".to_string(),
        };
        assert_eq!(repo_config.online_store, expected_online_store);
        assert_eq!(
            repo_config.entity_key_serialization_version,
            EntityKeySerializationVersion::V2
        );
        Ok(())
    }

    #[test]
    fn parse_config_local_redis() -> Result<()> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/test_data/local_redis.yaml", project_dir);
        let yaml_str = fs::read_to_string(config_path)?;
        let repo_config = RepoConfig::from_yaml_str(&yaml_str)?;
        assert_eq!(repo_config.project, "local_redis");
        let mut expected_registry = RegistryConfig::default();
        expected_registry.registry_type = RegistryType::File;
        expected_registry.path = "data/redis_registry.db".to_string();
        assert_eq!(repo_config.registry, expected_registry);
        let expected_online_store = OnlineStoreConfig::Redis {
            redis_type: RedisType::SingleNode,
            connection_string: "localhost:6379".to_string(),
            sentinel_master: None,
        };
        assert_eq!(repo_config.online_store, expected_online_store);
        assert_eq!(
            repo_config.entity_key_serialization_version,
            EntityKeySerializationVersion::V3
        );
        Ok(())
    }
}
