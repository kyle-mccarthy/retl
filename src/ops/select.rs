use crate::{
    dim::Dim,
    schema::{Field, Schema},
    DataFrame, Value,
};

use snafu::Snafu;
use std::borrow::Cow;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A column with the name {} does not exist", name))]
    InvalidColumnName { name: String },
}

pub enum Select<'a> {
    Name(&'a str),
    Alias(&'a str, &'a str),
}

impl<'a> Select<'a> {
    pub fn name(&self) -> &'a str {
        match self {
            Select::Name(name) => name,
            Select::Alias(name, _) => name,
        }
    }

    pub fn with_name(&self) -> &'a str {
        match self {
            Select::Name(name) => name,
            Select::Alias(_, name) => name,
        }
    }
}

impl<'a> From<&'a str> for Select<'a> {
    fn from(s: &'a str) -> Self {
        Select::Name(s)
    }
}

impl<'a> From<(&'a str, &'a str)> for Select<'a> {
    fn from(alias: (&'a str, &'a str)) -> Self {
        Select::Alias(alias.0, alias.1)
    }
}

pub fn select<'a>(df: &DataFrame<'a>, select: &[Select]) -> Result<DataFrame<'a>, Error> {
    let columns = select
        .iter()
        .map(|s| {
            df.schema
                .get_field_full(s.name())
                .ok_or(Error::InvalidColumnName {
                    name: s.name().to_string(),
                })
                .map(|(pos, field)| {
                    let mut field = field.clone();
                    field.name = s.with_name().to_string();
                    (pos, field)
                })
        })
        .collect::<Result<Vec<(&usize, Field)>, Error>>()?;

    let mut schema = Schema::with_size(columns.len());

    let fields = columns
        .into_iter()
        .map(|(pos, field)| {
            schema.push_field(field);
            pos
        })
        .collect::<Vec<&usize>>();

    let next_size = fields.len() * df.dim.1;
    let mut data: Vec<Value> = Vec::with_capacity(next_size);

    for i in 0..df.dim.1 {
        let offset = df.dim.0 * i;
        fields.iter().for_each(|col_index| {
            data.push(df.data[*col_index + offset].clone());
        });
    }

    let dim = Dim::new(schema.len(), df.dim.1);

    Ok(DataFrame {
        schema,
        dim,
        data: Cow::from(data),
    })
}

#[cfg(test)]
mod test_select {
    use super::*;
    #[macro_use]
    use crate::{Value, val, row};

    #[test]
    fn it_selects_columns_by_name() {
        let df = DataFrame::new(
            &["a", "b", "c"],
            vec![row![0, 1, 2], row![3, 4, 5], row![6, 7, 8], row![9, 10, 11]],
        );

        let cols: &[Select] = &["b".into(), "c".into()];
        let out_df = select(&df, cols);

        assert!(out_df.is_ok());
        let out_df = out_df.unwrap();

        assert_eq!(out_df.dim.shape(), (2, 4));

        assert_eq!(out_df[0].to_vec(), row![1, 2]);
        assert_eq!(out_df[1].to_vec(), row![4, 5]);
        assert_eq!(out_df[2].to_vec(), row![7, 8]);
        assert_eq!(out_df[3].to_vec(), row![10, 11]);

        assert_eq!(
            out_df.schema.field_names(),
            vec![&"b".to_string(), &"c".to_string()]
        );
    }

    #[test]
    fn it_selects_columns_by_name_reordered() {
        let df = DataFrame::new(
            &["a", "b", "c"],
            vec![row![0, 1, 2], row![3, 4, 5], row![6, 7, 8], row![9, 10, 11]],
        );

        let cols: &[Select] = &["c".into(), "a".into()];
        let out_df = select(&df, cols);

        assert!(out_df.is_ok());
        let out_df = out_df.unwrap();

        assert_eq!(out_df.dim.shape(), (2, 4));

        assert_eq!(out_df[0].to_vec(), vec![2.into(), 0.into()]);
        assert_eq!(out_df[1].to_vec(), vec![5.into(), 3.into()]);
        assert_eq!(out_df[2].to_vec(), vec![8.into(), 6.into()]);
        assert_eq!(out_df[3].to_vec(), vec![11.into(), 9.into()]);

        assert_eq!(
            out_df.schema.field_names(),
            vec![&"c".to_string(), &"a".to_string()]
        );
    }

    #[test]
    fn it_selects_columns_and_aliases() {
        let df = DataFrame::new(
            &["a", "b", "c"],
            vec![row![0, 1, 2], row![3, 4, 5], row![6, 7, 8], row![9, 10, 11]],
        );

        let cols: &[Select] = &[("a", "z").into()];
        let out_df = select(&df, cols);

        assert!(out_df.is_ok());
        let out_df = out_df.unwrap();

        assert_eq!(out_df.dim.shape(), (1, 4));

        assert_eq!(out_df[0].to_vec(), vec![0.into()]);
        assert_eq!(out_df[1].to_vec(), vec![3.into()]);
        assert_eq!(out_df[2].to_vec(), vec![6.into()]);
        assert_eq!(out_df[3].to_vec(), vec![9.into()]);

        assert_eq!(out_df.schema.field_names(), vec![&"z".to_string()]);
    }
}
