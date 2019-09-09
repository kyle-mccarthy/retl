use crate::DataFrame;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to write the row: {}", source))]
    WriteRecordError { source: csv::Error },

    #[snafu(display("Failed to write the field: {}", source))]
    WriteFieldError { source: csv::Error },

    #[snafu(display("Failed while writing buffer to writer: {}", source))]
    FlushError { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait CsvDestination {
    fn to_csv(&self) -> Result<()>;
}

impl<'a> CsvDestination for DataFrame<'a> {
    fn to_csv(&self) -> Result<()> {
        // TODO update to write somewhere other than stdout
        let mut writer = csv::Writer::from_writer(std::io::stdout());

        writer
            .write_record(self.columns())
            .context(WriteRecordError)?;

        for row in self.iter() {
            for val in row.iter() {
                let str_val: String = val.clone().into();
                writer.write_field(str_val).context(WriteFieldError)?;
            }
            writer
                .write_record(None::<&[u8]>)
                .context(WriteRecordError)?;
        }

        writer.flush().context(FlushError)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn it_df_to_csv() {
        let mut df = DataFrame::with_columns(&["a", "b", "c"]);

        df.push_row(vec!["x".into(), 1.into(), true.into()] as Vec<Value>)
            .unwrap();

        df.push_row(vec!["y".into(), 2.into(), true.into()] as Vec<Value>)
            .unwrap();

        df.push_row(vec!["z".into(), 3.into(), false.into()] as Vec<Value>)
            .unwrap();

        let res = df.to_csv();

        // TODO verify data written?
    }
}
