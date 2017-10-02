#[derive(Debug, error_chain)]
#[error_chain(backtrace = "false")]
pub enum ErrorKind {
    Msg(String),

    #[error_chain(custom)]
    Convert,

    #[error_chain(custom)]
    Sampling,

    #[error_chain(custom)]
    Parsing,

    #[error_chain(custom)]
    Aggregate,

    #[error_chain(custom)]
    Format,

    #[error_chain(custom)]
    Unreachable,

    #[error_chain(custom)]
    Future,

    #[error_chain(custom)]
    TcpTimeout,

    #[error_chain(foreign)]
    Io(::std::io::Error),

    #[error_chain(foreign)]
    Time(::std::time::SystemTimeError),
}
