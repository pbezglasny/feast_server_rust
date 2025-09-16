use saphyr::{Scalar, Yaml};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Provider {
    Local,
    AWS,
    GCP,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryType {
    #[default]
    File,
    SQL,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RegistryConfig {
    path: String,
    cache_ttl_seconds: Option<u64>,
    registry_type: RegistryType,
    account: Option<String>,
    user: Option<String>,
    password: Option<String>,
    role: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum OnlineStoreType {
    #[default]
    Sqlite,
    Redis,
    DynamoDB,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OnlineStoreConfig {
    Sqlite { path: String },
    // TODO add other redis configs: key_ttl_seconds, redis_type[cluster or not], sentinel_master
    Redis { connection_string: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepoConfig {
    pub project: String,
    pub project_description: Option<String>,
    pub provider: Provider,
    pub registry: RegistryConfig,
    pub online_store: OnlineStoreConfig,
}

impl TryFrom<&Yaml<'_>> for RegistryConfig {
    type Error = Box<dyn std::error::Error>;
    fn try_from(yaml: &Yaml) -> Result<RegistryConfig, Box<dyn std::error::Error>> {
        match yaml {
            Yaml::Value(val) => {
                let s = val.as_str().ok_or("Expected string for registry path")?;
                let mut config = RegistryConfig::default();
                config.path = s.to_string();
                Ok(config)
            }
            Yaml::Mapping(map) => {
                let mut config = RegistryConfig::default();
                for (key, value) in map {
                    if let Yaml::Value(Scalar::String(key_str)) = key {
                        match key_str.as_ref() {
                            "path" => {
                                if let Yaml::Value(Scalar::String(path_str)) = value {
                                    config.path = path_str.to_string();
                                } else {
                                    return Err("Expected string for registry path".into());
                                }
                            }
                            "cache_ttl_seconds" => {
                                if let Yaml::Value(Scalar::Integer(ttl)) = value {
                                    config.cache_ttl_seconds = Some(*ttl as u64);
                                } else {
                                    return Err("Expected integer for cache_ttl_seconds".into());
                                }
                            }
                            "registry_type" => {
                                if let Yaml::Value(Scalar::String(type_str)) = value {
                                    config.registry_type = match type_str.as_ref() {
                                        "file" => RegistryType::File,
                                        "sql" => RegistryType::SQL,
                                        _ => {
                                            return Err("Invalid registry_type value".into());
                                        }
                                    };
                                } else {
                                    return Err("Expected string for registry_type".into());
                                }
                            }
                            "account" => {
                                if let Yaml::Value(Scalar::String(account_str)) = value {
                                    config.account = Some(account_str.to_string());
                                } else {
                                    return Err("Expected string for account".into());
                                }
                            }
                            "user" => {
                                if let Yaml::Value(Scalar::String(user_str)) = value {
                                    config.user = Some(user_str.to_string());
                                } else {
                                    return Err("Expected string for user".into());
                                }
                            }
                            "password" => {
                                if let Yaml::Value(Scalar::String(password_str)) = value {
                                    config.password = Some(password_str.to_string());
                                } else {
                                    return Err("Expected string for password".into());
                                }
                            }
                            "role" => {
                                if let Yaml::Value(Scalar::String(role_str)) = value {
                                    config.role = Some(role_str.to_string());
                                } else {
                                    return Err("Expected string for role".into());
                                }
                            }
                            _ => {}
                        }
                    } else {
                        return Err("Invalid key type in registry mapping".into());
                    }
                }
                Ok(config)
            }
            _ => Err("Invalid YAML for RegistryConfig")?,
        }
    }
}

impl TryFrom<&Yaml<'_>> for OnlineStoreConfig {
    type Error = Box<dyn std::error::Error>;
    fn try_from(yaml: &Yaml) -> Result<OnlineStoreConfig, Box<dyn std::error::Error>> {
        match yaml {
            Yaml::Mapping(map) => {
                let store_type = map.get(&Yaml::Value(Scalar::String("type".into())));
                if store_type.is_none() {
                    return Err("Missing 'type' field for online store".into());
                }
                let store_type = store_type.unwrap();
                if let Yaml::Value(Scalar::String(type_str)) = store_type {
                    match type_str.as_ref() {
                        "sqlite" => {
                            let path = map.get(&Yaml::Value(Scalar::String("path".into())));
                            if let Some(Yaml::Value(Scalar::String(path_str))) = path {
                                Ok(OnlineStoreConfig::Sqlite {
                                    path: path_str.to_string(),
                                })
                            } else {
                                Err("Expected string for sqlite path".into())
                            }
                        }
                        "redis" => {
                            let connection_string_conf =
                                map.get(&Yaml::Value(Scalar::String("connection_string".into())));
                            if let Some(Yaml::Value(Scalar::String(connection_string))) =
                                connection_string_conf
                            {
                                Ok(OnlineStoreConfig::Redis {
                                    connection_string: connection_string.to_string(),
                                })
                            } else {
                                Err("Expected string for redis host and integer for port".into())
                            }
                        }
                        _ => Err("Unsupported online store type".into()),
                    }
                } else {
                    Err("Expected string for online store type".into())
                }
            }
            _ => Err("Invalid YAML for OnlineStoreConfig".into()),
        }
    }
}

impl TryFrom<&Yaml<'_>> for RepoConfig {
    type Error = Box<dyn std::error::Error>;
    fn try_from(yaml: &Yaml) -> Result<RepoConfig, Box<dyn std::error::Error>> {
        let mapping = yaml.as_mapping().ok_or("Expected mapping for RepoConfig")?;
        let project = mapping
            .get(&Yaml::Value(Scalar::String("project".into())))
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'project' field")?
            .to_string();
        let project_description = mapping
            .get(&Yaml::Value(Scalar::String("project_description".into())))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let provider_str = mapping
            .get(&Yaml::Value(Scalar::String("provider".into())))
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'provider' field")?;
        let provider = match provider_str {
            "local" => Provider::Local,
            "aws" => Provider::AWS,
            "gcp" => Provider::GCP,
            _ => return Err("Unsupported provider".into()),
        };
        let registry_yaml = mapping
            .get(&Yaml::Value(Scalar::String("registry".into())))
            .ok_or("Missing 'registry' field")?;
        let registry = RegistryConfig::try_from(registry_yaml)?;
        let online_store_yaml = mapping
            .get(&Yaml::Value(Scalar::String("online_store".into())))
            .ok_or("Missing 'online_store' field")?;
        let online_store = OnlineStoreConfig::try_from(online_store_yaml)?;
        Ok(RepoConfig {
            project,
            project_description,
            provider,
            registry,
            online_store,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use saphyr::{LoadableYamlNode, Yaml};
    use std::fs;

    #[test]
    fn parse_config_local_sqlite() -> Result<(), Box<dyn std::error::Error>> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/test_data/local_sqlite.yaml", project_dir);
        let yaml_str = fs::read_to_string(config_path).unwrap();
        let conf = Yaml::load_from_str(&yaml_str).unwrap();
        let repo_config = RepoConfig::try_from(&conf[0])?;
        println!("{:#?}", repo_config);
        assert_eq!(repo_config.project, "local_sqlite");
        let mut expected_registry = RegistryConfig::default();
        expected_registry.registry_type = RegistryType::File;
        expected_registry.path = "data/registry.db".to_string();
        assert_eq!(repo_config.registry, expected_registry);
        let expected_online_store = OnlineStoreConfig::Sqlite {
            path: "data/online_store.db".to_string(),
        };
        assert_eq!(repo_config.online_store, expected_online_store);
        Ok(())
    }

    #[test]
    fn parse_config_local_redis() -> Result<(), Box<dyn std::error::Error>> {
        let project_dir = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/test_data/local_redis.yaml", project_dir);
        let yaml_str = fs::read_to_string(config_path).unwrap();
        let conf = Yaml::load_from_str(&yaml_str).unwrap();
        let repo_config = RepoConfig::try_from(&conf[0])?;
        println!("{:#?}", repo_config);
        assert_eq!(repo_config.project, "local_redis");
        let mut expected_registry = RegistryConfig::default();
        expected_registry.registry_type = RegistryType::File;
        expected_registry.path = "data/redis_registry.db".to_string();
        assert_eq!(repo_config.registry, expected_registry);
        let expected_online_store = OnlineStoreConfig::Redis {
            connection_string: "localhost:6379".to_string(),
        };
        assert_eq!(repo_config.online_store, expected_online_store);
        Ok(())
    }
}
