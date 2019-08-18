use crate::error::{ErrorKind, Result, ResultExt};
use crate::DataFrame;

pub trait CsvDestination {
    fn to_csv(&self) -> Result<()>;
}

impl CsvDestination for DataFrame {
    fn to_csv(&self) -> Result<()> {
        // TODO update to write somewhere other than stdout
        let mut writer = csv::Writer::from_writer(std::io::stdout());

        // let mut writer = csv::Writer::from_path("/Users/kylemccarthy/Downloads/out.csv").unwrap();

        writer
            .write_record(self.get_columns())
            .context(ErrorKind::CsvError)?;

        for row in self.iter() {
            for val in row.iter() {
                let str_val: String = val.into();
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
        let mut df = DataFrame::default();

        df.set_columns(vec!["a".into(), "b".into(), "c".into()]);

        df.push_data(vec!["x".into(), 1.into(), true.into()] as Vec<Value>);

        df.push_data(vec!["y".into(), 2.into(), true.into()] as Vec<Value>);

        df.push_data(vec!["z".into(), 3.into(), false.into()] as Vec<Value>);

        let res = df.to_csv();
        dbg!(res);
    }
}
