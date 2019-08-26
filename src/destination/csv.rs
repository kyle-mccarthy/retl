use crate::error::{ErrorKind, Result, ResultExt};
use crate::DataFrame;

pub trait CsvDestination {
    fn to_csv(&self) -> Result<()>;
}

impl<'a> CsvDestination for DataFrame<'a> {
    fn to_csv(&self) -> Result<()> {
        // TODO update to write somewhere other than stdout
        let mut writer = csv::Writer::from_writer(std::io::stdout());

        // let mut writer = csv::Writer::from_path("/Users/kylemccarthy/Downloads/out.csv").unwrap();

        writer
            .write_record(self.columns())
            .context(ErrorKind::CsvError)?;

        for row in self.iter() {
            for val in row.iter() {
                let str_val: String = val.clone().into();
                writer.write_field(str_val).context(ErrorKind::CsvError)?;
            }
            writer
                .write_record(None::<&[u8]>)
                .context(ErrorKind::CsvError)?;
        }

        writer.flush().context(ErrorKind::CsvError)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn it_df_to_csv() {
        let mut df = DataFrame::with_columns(vec!["a", "b", "c"]);

        df.push_row(vec!["x".into(), 1.into(), true.into()] as Vec<Value>)
            .unwrap();

        df.push_row(vec!["y".into(), 2.into(), true.into()] as Vec<Value>)
            .unwrap();

        df.push_row(vec!["z".into(), 3.into(), false.into()] as Vec<Value>)
            .unwrap();

        let res = df.to_csv();

        // TODO add some way to data written
        dbg!(res);
    }
}
