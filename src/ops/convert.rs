use crate::{DataFrame, DataType, Value};

use snafu::Snafu;
use std::error::Error as ErrorTrait;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to convert value into date data type using the format. Cannot convert {} to date from format {}. Error = {}",
        value.to_string(), format, message
    ))]
    ParseDateError {
        value: String,
        format: String,
        message: String,
    },

    #[snafu(display(
        "Cannot convert values data type of {} into destination type of {}",
        value_type,
        dest_type
    ))]
    IllegalConversion {
        value_type: DataType,
        dest_type: DataType,
    },
}

pub enum Convert<'a> {
    ParseDateTime(&'a str),
}

pub fn convert<'b, 'a: 'b>(
    df: &mut DataFrame<'a>,
    column: &str,
    conversion: Convert<'b>,
) -> Result<DataType, crate::error::Error> {
    match conversion {
        Convert::ParseDateTime(fmt) => try_parse_datetime(df, column, fmt),
    }
}

fn try_parse_datetime(
    df: &mut DataFrame,
    column: &str,
    fmt: &str,
) -> Result<DataType, crate::error::Error> {
    use chrono::NaiveDateTime;

    let parse = |value: &mut Value| -> Result<Value, Error> {
        let str_val = value.to_string();
        NaiveDateTime::parse_from_str(&str_val, &fmt)
            .map_err(|err| Error::ParseDateError {
                value: value.to_string(),
                format: fmt.to_string().clone(),
                message: err.description().to_string(),
            })
            .and_then(|date| Ok(Value::Date(date)))
    };

    df.map_column(column, |value| {
        parse(value)
            .map(|converted| {
                *value = converted;
            })
            .map_err(|e| crate::error::Error::ConvertError { source: e })
    })
    .map(|_| DataType::Date)
}

#[cfg(test)]
mod test_convert {
    use super::*;

    #[test]
    fn it_converts_date() {
        let mut df = DataFrame::new(&["a"], vec![vec!["2019-09-05 18:14:04".into()]]);
        let conversion_result = df.convert_column("a", Convert::ParseDateTime("%Y-%m-%d %H:%M:%S"));

        assert!(conversion_result.is_ok());

        dbg!(df);
    }
}
