use snafu::Snafu;

pub use snafu::ResultExt;

// TODO convert to snafu

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error occurred during io op"))]
    IoError { source: std::io::Error },

    #[snafu(display("Error occurred relating to CSV op"))]
    CsvError { source: csv::Error },

    #[snafu(display("The length of the data does not match the schema. Expected field length {}, actual field length: {}", expected, actual))]
    InvalidDataLength { expected: usize, actual: usize },

    #[snafu(display(
        "Attempted to perform on operation by index, but index out of bounds. Index = {}, Len = {}",
        index,
        length
    ))]
    IndexOutOfBounds { index: usize, length: usize },

    #[snafu(display("A column doesn't exist with the name {}", column))]
    InvalidColumnName { column: String },

    #[snafu(display("Failed cast op"))]
    CastError { source: crate::ops::cast::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
