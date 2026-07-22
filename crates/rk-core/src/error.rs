use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("invalid tuple: {0}")]
    InvalidTuple(String),

    #[error("daemon not running (socket: {0})")]
    DaemonNotRunning(String),

    #[error("protocol: {0}")]
    Protocol(String),

    #[error("{0}")]
    Other(String),
}

impl Error {
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}
