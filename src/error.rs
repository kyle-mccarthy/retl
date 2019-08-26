pub use failure::ResultExt;
use failure::{Backtrace, Fail};

// TODO convert to snafu

pub type Result<T> = std::result::Result<T, failure::Error>;

#[derive(Debug)]
pub struct Error {
    inner: failure::Context<ErrorKind>,
}

#[derive(Copy, Clone, Debug, Fail, Eq, PartialEq)]
pub enum ErrorKind {
    #[fail(display = "Error occured during i/o")]
    Io,

    #[fail(display = "UTF-8 decording error")]
    Utf8,

    #[fail(display = "Serialization error")]
    Serialize,

    #[fail(display = "Deserialization error")]
    Deserialize,

    #[fail(display = "Unkown error")]
    UnknownError,

    #[fail(display = "CsvError")]
    CsvError,

    #[fail(display = "Option is none, but value expected")]
    NoneError,

    #[fail(display = "Failed to run the python code")]
    PyO3Error,

    #[fail(display = "Invalid Operation")]
    InvalidOperation,

    #[fail(
        display = "The length of the data does not match the expected length (expected ({}) != received = ({}))",
        _0, _1
    )]
    InvalidDataLength(usize, usize),

    #[fail(display = "The index is out of bounds (idx {}, len {})", _0, _1)]
    IndexOutofBounds(usize, usize),

    #[fail(display = "Failed to convert the float into a decimal type")]
    FloatConversion(),
}

#[derive(Debug)]
pub struct ContextPair<T: std::fmt::Display>(&'static str, T);

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Error {
            inner: failure::Context::new(kind),
        }
    }
}

impl From<failure::Context<ErrorKind>> for Error {
    fn from(inner: failure::Context<ErrorKind>) -> Self {
        Error { inner }
    }
}
