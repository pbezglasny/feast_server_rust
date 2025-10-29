use crate::cli_options::{CliCommand, CliOptions};
use anyhow::{Result, anyhow};
use clap::Parser;
use feast_server_core::config::{Provider, RepoConfig};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod cli_options;

const FEATURE_REPO_DIR_ENV_VAR_NAME: &str = "FEATURE_REPO_DIR_ENV_VAR";
const FEAST_FS_YAML_FILE_PATH_ENV_VAR: &str = "FEAST_FS_YAML_FILE_PATH";
const DEFAULT_FEATURE_STORE_FILE_NAME: &str = "feature_store.yaml";

#[tokio::main]
async fn main() -> Result<()> {
    let cli_opts = CliOptions::parse();
    let CliOptions {
        chdir,
        help: _,
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

    let cwd =
        if let Some(path) = chdir.or_else(|| std::env::var(FEATURE_REPO_DIR_ENV_VAR_NAME).ok()) {
            PathBuf::from(path)
        } else {
            std::env::current_dir()?
        };
    let cwd_str = cwd
        .to_str()
        .ok_or_else(|| anyhow!("Feature repository path contains invalid UTF-8"))?;

    let feature_store_yaml = feature_store_yaml
        .or(std::env::var(FEAST_FS_YAML_FILE_PATH_ENV_VAR).ok())
        .unwrap_or(DEFAULT_FEATURE_STORE_FILE_NAME.to_string());
    let config_path = cwd.join(&feature_store_yaml);
    let yaml_str = fs::read_to_string(&config_path)?;
    let repo_config = RepoConfig::from_yaml_str(&yaml_str)?;

    match command {
        CliCommand::Serve {
            host,
            port,
            r#type,
            key,
            cert,
            metrics_enabled,
        } => {
            if key.is_some() && cert.is_none() || key.is_none() && cert.is_some() {
                return Err(anyhow!(
                    "Both --key and --cert must be provided to enable TLS"
                ));
            }
            if let Some(Provider::Unknown(other)) = repo_config.provider {
                return Err(anyhow!(
                    "Unsupported provider: {}, available providers: [local, aws, gcp]",
                    other
                ));
            }
            tracing::info!("Start serving on {}:{} using {}", host, port, r#type);
            let tls_enabled = key.is_some() && cert.is_some();
            let registry = feast_server_core::registry::get_registry(
                repo_config.registry.clone(),
                repo_config.provider.clone(),
                repo_config.project.clone(),
                Some(cwd_str),
            )
            .await?;
            let online_store = feast_server_core::onlinestore::get_online_store(
                &repo_config.online_store,
                &repo_config.project,
                Some(cwd_str),
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
                    if metrics_enabled {
                        tracing::warn!(
                            "Metrics server is only available for HTTP; ignoring flag for gRPC"
                        );
                    }
                    let server_config = grpc_server::server::ServerConfig {
                        host,
                        port,
                        tls_enabled,
                        tls_cert_path: cert,
                        tls_key_path: key,
                    };
                    #[cfg(unix)]
                    {
                        let mut sigterm = tokio::signal::unix::signal(
                            tokio::signal::unix::SignalKind::terminate(),
                        )?;
                        tokio::select! {
                            res = grpc_server::server::start_server(server_config, feature_store) => {
                                res?
                            }
                            _ = sigterm.recv() => {
                                tracing::info!("Received SIGTERM, shutting down...");
                            }
                            _ = tokio::signal::ctrl_c() => {
                                tracing::info!("Received Ctrl+C, shutting down...");
                            }
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        tokio::select! {
                            res = grpc_server::server::start_server(server_config, feature_store) => {
                                res?
                            }
                            _ = tokio::signal::ctrl_c() => {
                                tracing::info!("Received Ctrl+C, shutting down...");
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
