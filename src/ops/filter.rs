use crate::error;
use crate::{DataFrame, Record, Value};
use regex::Regex;

pub enum FilterOps {
    Eq(Value),
    NotEq(Value),
    Gt(Value),
    GtEq(Value),
    Lt(Value),
    LtEq(Value),
    RegExp(Regex),
}

pub trait Filter {
    fn filter(self: Self, df: DataFrame) -> error::Result<DataFrame>;
}

impl Filter for (&str, FilterOps) {
    fn filter(self, df: DataFrame) -> error::Result<DataFrame> {
        let col_idx = df
            .get_column_idx(self.0)
            .ok_or(error::ErrorKind::NoneError)?;

        let cmp: Box<dyn Fn(&Value) -> bool> = match self.1 {
            FilterOps::Eq(a) => Box::new(move |b: &Value| &a == b),
            FilterOps::NotEq(a) => Box::new(move |b: &Value| &a != b),
            FilterOps::Gt(a) => Box::new(move |b: &Value| &a > b),
            FilterOps::GtEq(a) => Box::new(move |b: &Value| &a >= b),
            FilterOps::Lt(a) => Box::new(move |b: &Value| &a < b),
            FilterOps::LtEq(a) => Box::new(move |b: &Value| &a <= b),
            _ => Box::new(|_: &Value| false),
        };

        let f = |record: &Record| {
            let opt = record.get(col_idx);

            match opt {
                Some(v) => cmp(&v),
                None => false,
            }
        };

        Ok(df.filter(f))
    }
}

impl Filter for Box<dyn FnMut(&Record) -> bool> {
    fn filter(self, df: DataFrame) -> error::Result<DataFrame> {
        Ok(df.filter(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_df() -> DataFrame {
        let mut df = DataFrame::default();
        df.set_columns(vec!["a".into(), "b".into(), "c".into()]);

        df.push_data(vec!["x".into(), 1.into(), true.into()] as Vec<Value>);

        df.push_data(vec!["y".into(), 2.into(), true.into()] as Vec<Value>);

        df.push_data(vec!["z".into(), 3.into(), false.into()] as Vec<Value>);

        df
    }

    #[test]
    fn it_filters_tuple() {
        let mut df = get_df();

        df = <(&str, FilterOps) as Filter>::filter(("a", FilterOps::Eq("x".into())), df).unwrap();

        assert_eq!(df.size(), 1);

        let row = df.get(0).unwrap();

        let row_b: Vec<Value> = vec!["x".into(), 1.into(), true.into()];

        assert_eq!(row, &Into::<Record>::into(row_b));
    }

    #[test]
    fn it_filters_closure() {
        let mut df = get_df();

        let idx = df.get_column_idx("a").unwrap();
        let value: Value = "x".into();

        let f = Box::new(move |record: &Record| {
            let opt = record.get(idx);

            match opt {
                Some(v) => v == &value,
                None => false,
            }
        });

        df = <Box<dyn FnMut(&Record) -> bool> as Filter>::filter(f, df).unwrap();

        assert_eq!(df.size(), 1);

        let row: &Record = df.get(0).unwrap();

        let row_b: Vec<Value> = vec!["x".into(), 1.into(), true.into()];

        assert_eq!(row, &Into::<Record>::into(row_b));
    }
}
