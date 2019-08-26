use crate::error::{Error, ErrorKind, Result, ResultExt};
use crate::value::Value;
use crate::DataFrame;

pub trait CsvSource {
    fn from_path(path: &str) -> Result<DataFrame> {
        let reader: csv::Reader<std::fs::File> =
            csv::Reader::from_path(path).context(ErrorKind::CsvError)?;

        Self::read_csv(reader)
    }

    fn from_reader<'a, R: std::io::Read>(reader: R) -> Result<DataFrame<'a>> {
        Self::read_csv(csv::Reader::from_reader(reader))
    }

    fn read_csv<'a, R: std::io::Read>(mut reader: csv::Reader<R>) -> Result<DataFrame<'a>> {
        // convert all the records into vectors of values
        let data = reader
            .records()
            .filter_map(|record| record.ok())
            .map(|record| {
                record
                    .into_iter()
                    .map(|value| match value.len() {
                        0 => Value::Null,
                        _ => Value::String(value.to_string()),
                    })
                    .collect::<Vec<Value>>()
            })
            .collect::<Vec<Vec<Value>>>();

        // all the data should have the same number of rows which should equal the number of
        // headers assuming that the CSV has headers
        let expected_row_length = data.iter().map(|row| row.len()).max().unwrap_or(0);

        // ensure that each record has the expected number of columns, otherwise fill with null
        let data = data
            .into_iter()
            .map(|mut record| {
                if record.len() != expected_row_length {
                    record.resize(expected_row_length, Value::Null);
                }
                record
            })
            .collect::<Vec<Vec<Value>>>();

        // get the headers or create default ones
        let headers = match reader.headers() {
            Ok(headers) => headers
                .into_iter()
                .map(|h| h.to_string())
                .collect::<Vec<String>>(),
            _ => (0..expected_row_length)
                .map(|h| format!("{}", h))
                .collect::<Vec<String>>(),
        };

        // create  the dataframe with the headers
        let mut df = DataFrame::with_columns(headers);

        // push data
        df.extend(data)?;

        Ok(df)
    }
}

impl<'a> CsvSource for DataFrame<'a> {}

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
    fn it_reads_csv_to_data_frame() {
        let raw_data = "a,b,c\r\n1,2,3\r\n4,5,6\r";

        let df = DataFrame::from_reader(raw_data.as_bytes()).unwrap();

        let cols = df.columns();

        assert_eq!(*cols, ["a", "b", "c"]);
        assert_eq!(df.size(), 2);
        assert_eq!(df[0], ["1".into(), "2".into(), "3".into()]);
        assert_eq!(df[1], ["4".into(), "5".into(), "6".into()]);
    }
}
