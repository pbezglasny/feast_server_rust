use clap::ArgAction;
use clap::{Parser, Subcommand, ValueEnum};
use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ServeType {
    Http,
    Grpc,
}

impl Display for ServeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeType::Http => write!(f, "http"),
            ServeType::Grpc => write!(f, "grpc"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum LogLevel {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl From<LogLevel> for tracing::Level {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warning => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Critical => tracing::Level::ERROR,
        }
    }
}

#[derive(Subcommand, Debug)]
pub enum CliCommand {
    /// Start a feature server locally on a given port
    Serve {
        /// Specify a host for the server
        #[arg(short = 'h', long = "host", default_value = "127.0.0.1")]
        host: String,
        /// Specify a port for the server
        #[arg(short = 'p', long = "port", default_value_t = 6566)]
        port: u16,
        /// Specify a server type: 'http' or 'grpc'
        #[arg(value_enum, short = 't', long = "type", default_value = "http")]
        r#type: ServeType,
        // /// Number of seconds after which the registry is refreshed
        // #[arg(short = 'r', long = "registry_ttl_sec", default_value_t = 5)]
        // registry_ttl_sec: i32,
        /// path to TLS certificate private key. You need to pass --cert as well to start server in TLS mode
        #[arg(short='k', long="key", default_value = None)]
        key: Option<String>,
        /// path to TLS certificate public key. You need to pass --key as well to start server in TLS mode
        #[arg(short='c', long="cert", default_value = None)]
        cert: Option<String>,
        /// Enable the Metrics Server
        #[arg(short = 'm', long = "metrics", default_value_t = false)]
        metrics_enabled: bool,
    },
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, disable_help_flag = true, name = "feast-server-rust")]
pub struct CliOptions {
    #[arg(short = 'c', long = "chdir", default_value = None)]
    /// Switch to a different feature repository directory before executing the given subcommand.
    /// Can also be set via the FEATURE_REPO_DIR_ENV_VAR environment variable.
    pub chdir: Option<String>,
    /// Print help information
    #[arg(long = "help", action = ArgAction::Help, help = "Print help information")]
    pub help: Option<bool>,
    /// The logging level. Case-insensitive.
    #[arg(
        value_enum,
        long = "log-level",
        ignore_case = true,
        default_value = "info"
    )]
    pub log_level: LogLevel,
    /// Override the directory where the CLI should look for the feature_store.yaml file.
    /// Can also be set via the FEAST_FS_YAML_FILE_PATH environment variable
    #[arg(short='f', long="feature-store-yaml", default_value = None)]
    pub feature_store_yaml: Option<String>,
    #[command(subcommand)]
    pub command: CliCommand,
}
