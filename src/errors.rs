use std::net::SocketAddr;

#[derive(Fail, Debug)]
pub enum GeneralError {
    #[fail(display = "I/O error")]
    Io(#[cause] ::std::io::Error),

    #[fail(display = "Error when creating timer: {}", _0)]
    Timer(#[cause] ::tokio::timer::Error),

    #[fail(display = "getting system time")]
    Time(#[cause] ::std::time::SystemTimeError),

    #[fail(display = "Gave up connecting to {}", _0)]
    TcpOutOfTries(SocketAddr),

    #[fail(display = "Carbon backend failure")]
    CarbonBackend,

    #[fail(display = "future send error")]
    FutureSend,

    #[fail(display = "unknown consensus state")]
    UnknownState,
}
