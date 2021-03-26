use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GeneralError {
    #[error("I/O error")]
    Io(#[from] ::std::io::Error),

    #[error("Error when creating timer: {}", _0)]
    Timer(#[from] ::tokio1::timer::Error),

    #[error("getting system time")]
    Time(#[from] ::std::time::SystemTimeError),

    #[error("Gave up connecting to {}", _0)]
    TcpOutOfTries(SocketAddr),

    #[error("Carbon backend errorure")]
    CarbonBackend,

    #[error("future send error")]
    FutureSend,

    #[error("unknown consensus state")]
    UnknownState,

    #[error("metric error")]
    Metric(#[from] bioyino_metric::MetricError),

    #[error("configuration error: {}", _0)]
    Configuration(#[from] crate::config::ConfigError),

    #[error("utility error")]
    Other(#[from] crate::util::OtherError),
}
