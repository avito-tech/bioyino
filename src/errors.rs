#[derive(Fail, Debug)]
pub enum GeneralError {
    #[fail(display = "I/O error")] Io(#[cause] ::std::io::Error),

    #[fail(display = "getting system time")] Time(#[cause] ::std::time::SystemTimeError),
    #[fail(display = "graphite connection error")] GraphiteConn,

    #[fail(display = "future send error")] FutureSend,
}
