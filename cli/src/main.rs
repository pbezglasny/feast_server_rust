use crate::cli_options::{CliCommand, CliOptions};
use anyhow::{Result, anyhow};
use clap::Parser;
use feast_server_core::config::RepoConfig;
use saphyr::{LoadableYamlNode, Yaml};
use std::fs;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod cli_options;

const FEATURE_REPO_DIR_ENV_VAR_ENV_VAR: &str = "FEATURE_REPO_DIR_ENV_VAR";
const FEAST_FS_YAML_FILE_PATH_ENV_VAR: &str = "FEAST_FS_YAML_FILE_PATH";
const DEFAULT_FEATURE_STORE_FILE_NAME: &str = "feature_store.yaml";

#[tokio::main]
async fn main() -> Result<()> {
    let cli_opts = CliOptions::parse();
    let CliOptions {
        chdir,
        log_level,
        feature_store_yaml,
        command,
    } = cli_opts;

    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(tracing::Level::from(log_level).into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cwd = chdir
        .or(std::env::var(FEATURE_REPO_DIR_ENV_VAR_ENV_VAR).ok())
        .unwrap_or(std::env::current_dir().map(|p| p.into_os_string().into_string().unwrap())?);

    let feature_store_yaml = feature_store_yaml
        .or(std::env::var(FEAST_FS_YAML_FILE_PATH_ENV_VAR).ok())
        .unwrap_or(DEFAULT_FEATURE_STORE_FILE_NAME.to_string());
    let config_path = std::path::Path::new(&cwd)
        .join(&feature_store_yaml)
        .into_os_string()
        .into_string()
        .unwrap();
    let yaml_str = fs::read_to_string(config_path)?;
    let conf = Yaml::load_from_str(&yaml_str)?;
    if conf.is_empty() {
        return Err(anyhow!("Empty configuration file"));
    }
    let repo_config = RepoConfig::try_from(&conf[0])?;

    match command {
        CliCommand::Serve {
            host,
            port,
            r#type,
            // registry_ttl_sec,
            key,
            cert,
            metrics_enabled,
        } => {
            if key.is_some() && cert.is_none() || key.is_none() && cert.is_some() {
                return Err(anyhow!(
                    "Both --key and --cert must be provided to enable TLS"
                ));
            }
            let tls_enabled = key.is_some() && cert.is_some();
            let registry = feast_server_core::registry::get_registry(
                &repo_config.registry,
                repo_config.provider.clone(),
                Some(&cwd),
            )
            .await?;
            let online_store = feast_server_core::onlinestore::get_online_store(
                &repo_config.online_store,
                &repo_config.project,
                Some(&cwd),
            )
            .await?;
            let feature_store =
                feast_server_core::feature_store::FeatureStore::new(registry, online_store);
            match r#type {
                cli_options::ServeType::Http => {
                    let server_config = rest_server::server::ServerConfig {
                        host,
                        port,
                        tls_enabled,
                        tls_cert_path: cert,
                        tls_key_path: key,
                    };
                    let handler = axum_server::Handle::new();
                    let mut sigterm =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                    tokio::select! {
                    res = rest_server::server::start_server(server_config, feature_store, metrics_enabled, handler.clone()) => {
                    res?
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("Received SIGTERM, shutting down...");
                        handler.graceful_shutdown(Some(Duration::from_secs(5)));
                        }
                    _ = tokio::signal::ctrl_c() => {
                            tracing::info!("Received Ctrl+C, shutting down...");
                            handler.graceful_shutdown(Some(Duration::from_secs(5)));
                        }

                    }
                }
                cli_options::ServeType::Grpc => {
                    return Err(anyhow!("Grpc server is not implemented yet"));
                }
            }
        }
    }
    Ok(())
}
