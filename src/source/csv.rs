use crate::data_frame::DataFrame;
use crate::error::{Error, ErrorKind, Result, ResultExt};
use crate::value::Value;

pub trait CsvSource {
    fn from_path(path: &str) -> Result<DataFrame> {
        let reader: csv::Reader<std::fs::File> =
            csv::Reader::from_path(path).context(ErrorKind::CsvError)?;

        Self::read_csv(reader)
    }

    fn from_reader<R: std::io::Read>(reader: R) -> Result<DataFrame> {
        Self::read_csv(csv::Reader::from_reader(reader))
    }

    fn read_csv<R: std::io::Read>(mut reader: csv::Reader<R>) -> Result<DataFrame> {
        let mut data_frame = DataFrame::default();

        let mut size = 0usize;

        for record in reader.records() {
            let row = record.context(ErrorKind::CsvError)?;
            let len = row.len();

            if len > size {
                size = len;
            }

            // in the future it will likely be possible to use a csv that is missing columns, if
            // the use passes it in we just want to fill those rows in with null
            let mut data_row = vec![];
            for i in 0..size {
                data_row.push(match row.get(i) {
                    Some(v) => {
                        if v.len() == 0 {
                            Value::Null
                        } else {
                            Value::String(v.to_string())
                        }
                    }
                    _ => Value::Null,
                });
            }

            data_frame.push_data(data_row);
        }

        // if there are headers use them for the columns
        if reader.has_headers() {
            let headers = reader.headers().context(ErrorKind::CsvError)?;

            for i in 0..headers.len() {
                if let Some(h) = headers.get(i) {
                    data_frame.push_column(h.to_string());
                }
            }

            return Ok(data_frame);
        }

        // no headers avail so use index of header as name
        for i in 0..size {
            data_frame.push_column(format!("{}", i));
        }

        Ok(data_frame)
    }
}

impl CsvSource for DataFrame {}

/// Converts errors from the csv module into Error which can be generalized to fail
impl std::convert::From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        use csv::ErrorKind as CsvErrorKind;
        (match e.kind() {
            CsvErrorKind::Io(_) => ErrorKind::Io,
            CsvErrorKind::Utf8 { pos: _, err: _ } => ErrorKind::Utf8,
            CsvErrorKind::Deserialize { pos: _, err: _ } => ErrorKind::Deserialize,
            CsvErrorKind::Serialize(_) => ErrorKind::Serialize,
            CsvErrorKind::UnequalLengths {
                pos: _,
                expected_len: _,
                len: _,
            } => ErrorKind::CsvError,
            CsvErrorKind::Seek => ErrorKind::CsvError,
            _ => ErrorKind::UnknownError,
        })
        .into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn it_reads_csv() {
        let raw_data = r"a,b,c
        1,2,3
        4,5,6";

        let result: Result<DataFrame> = DataFrame::from_reader(raw_data.as_bytes());

        // let result: Result<DataFrame> =
        // CsvSource::from_reader::<&[u8]>(raw_data.as_bytes() as &[u8]);

        assert!(result.is_ok());
    }

    #[test]
    fn it_into_data_frame() {
        let raw_data = r"a,b,c
        1,2,3
        4,5,6";

        let df = DataFrame::from_reader(raw_data.as_bytes()).unwrap();

        let cols = df.get_columns();

        assert_eq!(*cols, vec!["a", "b", "c"]);
        assert_eq!(df.size(), 2);
    }
}
