use crate::{
    dim::Dim,
    error::{Error, Result},
    ops::{
        cast,
        convert::{self as convert, Convert},
    },
    schema::Schema,
    traits::TypeOf,
    views::{SubView, View},
    DataType, Value,
};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::iter::{FromIterator, Iterator};
use std::ops::Index;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFrame<'a> {
    pub(crate) data: Cow<'a, [Value]>,
    pub(crate) dim: Dim,
    pub(crate) schema: Schema,
}

impl<'a> std::default::Default for DataFrame<'a> {
    fn default() -> Self {
        DataFrame {
            data: Cow::from(vec![]),
            dim: Dim::default(),
            schema: Schema::default(),
        }
    }
}

// TODO logically reorder the methods

impl<'a> DataFrame<'a> {
    pub fn new<S>(columns: &[S], data: Vec<Vec<Value>>) -> DataFrame<'a>
    where
        S: Into<String> + Clone,
        [S]: ToOwned,
        [S]: ToOwned<Owned = Vec<S>>,
    {
        let dim = Dim::new(columns.len(), data.len());

        // TODO ensure that the length of the flattened data is equal the length of the columns *
        // length of the data -- panic or return result
        let data = data.into_iter().flatten().collect::<Vec<Value>>();

        let columns = columns
            .iter()
            .cloned()
            .map(Into::<String>::into)
            .collect::<Vec<String>>();

        let mut schema = Schema::default();

        columns.iter().for_each(|col| {
            let _ = schema.add_field(col);
        });

        DataFrame {
            data: data.into(),
            dim,
            schema,
        }
    }

    pub fn empty() -> DataFrame<'a> {
        DataFrame::default()
    }

    pub fn with_schema<S: Into<Schema>>(s: S) -> DataFrame<'a> {
        DataFrame {
            schema: s.into(),
            ..Default::default()
        }
    }

    pub fn with_columns<S>(columns: &[S]) -> DataFrame<'a>
    where
        S: Into<String> + Clone,
        [S]: ToOwned,
        [S]: ToOwned<Owned = Vec<S>>,
    {
        DataFrame::new(columns, vec![])
    }

    // TODO should this be renamed to len?
    pub fn size(&self) -> usize {
        self.dim.1
    }

    pub fn shape(&self) -> (usize, usize) {
        self.dim.shape()
    }

    pub fn iter(&self) -> View<'_, 'a> {
        View::new(&self)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Derive the schema's types from the data. Automatically updates the schema.
    pub fn derive_schema(&mut self) {
        // get the keys of the columns and iterate over each column along with the values trying to
        // determine a more strict type and if the column contains null values
        let keys = self
            .schema
            .field_names()
            .iter()
            .cloned()
            .cloned()
            .collect::<Vec<String>>();

        for key in keys {
            let mut dtype: DataType = DataType::Any;
            let mut strict_dtype = true;
            let mut is_nullable = false;

            self.column_values(&key)
                .unwrap()
                .iter()
                .for_each(|v| match (&dtype, &v.type_of()) {
                    (DataType::Any, vtype) => {
                        dtype = vtype.clone();
                    }
                    (_, DataType::Null) => {
                        is_nullable = true;
                    }
                    (col_type, vtype) => {
                        if col_type != vtype {
                            strict_dtype = false;
                        }
                    }
                });

            if let Some(field) = self.schema.get_field_mut(&key) {
                field.dtype = dtype;
                field.nullable = is_nullable;
            }
        }
    }

    pub fn columns(&self) -> Vec<&String> {
        self.schema.field_names()
    }

    pub fn push_column<S: Into<String>>(&mut self, column: S) {
        let name = column.into();
        self.schema.add_field(&name);
        self.dim.0 += 1;

        let col_count = self.dim.0;
        let col_index = col_count - 1;

        for row_num in 0..self.dim.1 {
            let index = self.dim.get_value_index(row_num, col_index);

            // insert NULL into the new column for each row at the index. If the index exceeds the
            // current length, push the value.
            if index > self.data.len() {
                self.data.to_mut().push(Value::Null);
            } else {
                self.data.to_mut().insert(index, Value::Null);
            }
        }
    }

    pub fn remove_column(&mut self, column: usize) -> Result<()> {
        let field = self.schema.find_by_index(column);
        dbg!(&self.schema);
        let index_exists = field.is_some();

        if !index_exists {
            return Err(Error::IndexOutOfBounds {
                index: column,
                length: self.schema.len(),
            });
        }

        let field = field.unwrap();

        // starting from the end of our value buffer delete elements for associated with the index
        // being removed
        for row_num in (0..self.dim.1).rev() {
            let index = self.dim.get_value_index(row_num, column);
            self.data.to_mut().remove(index);
        }

        self.dim.0 -= 1;
        let name = field.name.clone();
        let _ = self.schema.remove(&name);

        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Option<&String> {
        self.schema.rename_field(old_name, new_name)
    }

    /// Map over each value of the column
    pub fn map_column<F>(&mut self, column: &str, func: F) -> Result<()>
    where
        F: FnMut(&mut Value) -> std::result::Result<(), Error>,
    {
        let index = self
            .schema
            .find_index(column)
            .ok_or(Error::InvalidColumnName {
                column: column.to_string(),
            })?;

        // skip data up until the index and then step by the number of columns
        // "a" | "b" | "c"
        //  0  |  1  |  2
        //  3  |  4  |  5
        //  6  |  7  |  8
        //  index(b) = 1, skip 1 then step by 3 (row length) -> [1, 4, 7]

        self.data
            .to_mut()
            .iter_mut()
            .skip(*index)
            .step_by(self.schema.len())
            .map(func)
            .collect()
    }

    /// Return a columns values
    pub fn column_values(&self, column: &str) -> Result<Vec<&Value>> {
        let index = self
            .schema
            .find_index(column)
            .ok_or(Error::InvalidColumnName {
                column: column.to_string(),
            })?;

        Ok(self
            .data
            .iter()
            .skip(*index)
            .step_by(self.schema.len())
            .collect())
    }

    /// try to cast the column and its values into a certain type
    pub fn cast_column(&mut self, column: &str, to_type: DataType) -> Result<()> {
        cast::cast(self, column, &to_type).map(|_| {
            let field = self.schema.get_field_mut(column).unwrap();
            field.dtype = to_type;
        })
    }

    /// try to convert the column and values into a type using the conversion. Differs from cast as
    /// conversion as options (e.x. parsing a date requires the format of the date).
    pub fn convert_column(&mut self, column: &str, conversion: Convert) -> Result<()> {
        convert::convert(self, column, conversion).map(|dtype| {
            if let Some(field) = self.schema.get_field_mut(column) {
                field.dtype = dtype;
            }
        })
    }

    /// Get a row by its id/row number
    pub fn row(&self, row: usize) -> Option<&[Value]> {
        let (start, end) = self.dim.get_row_range(row);

        if self.data.len() < end {
            return None;
        }

        Some(&self.data.as_ref()[start..end])
    }

    /// Pushes new row onto the data, performs a check to ensure the length equals the number of
    /// columns
    pub fn push_row(&mut self, data: Vec<Value>) -> Result<usize> {
        if data.len() != self.dim.0 {
            return Err(Error::InvalidDataLength {
                expected: self.dim.0,
                actual: data.len(),
            });
        }

        self.push_row_unchecked(data);

        // TODO should we try to cast the data into the columns type?

        Ok(self.dim.1)
    }

    pub fn push_row_unchecked(&mut self, data: Vec<Value>) {
        self.data.to_mut().extend(data);
        self.dim.1 += 1;
    }

    /// Extends the internal data with the vector of rows. Ensures that length of each row equals
    /// the number of columns.
    pub fn extend(&mut self, data: Vec<Vec<Value>>) -> Result<usize> {
        let len_check = data.iter().find(|r| r.len() != self.dim.0);

        match len_check {
            Some(invalid_row) => Err(Error::InvalidDataLength {
                expected: self.dim.0,
                actual: invalid_row.len(),
            }),
            _ => {
                self.extend_unchecked(data);
                Ok(self.dim.1)
            }
        }
    }

    pub fn extend_unchecked(&mut self, data: Vec<Vec<Value>>) {
        self.dim.1 += data.len();
        self.data
            .to_mut()
            .extend(data.into_iter().flatten().collect::<Vec<Value>>());
    }

    /// Print the data frame to std out for debugging
    /// You can limit the number of rows shown with  the num_rows parameter. Will print at most
    /// num_rows, 0 prints all rows.
    pub fn print(&self, num_rows: usize) {
        use prettytable::{Cell, Row, Table};

        let mut table = Table::new();

        let mut type_row = Row::empty();
        let mut row = Row::empty();
        for c in self.columns() {
            row.add_cell(Cell::new(&c));
            type_row.add_cell(Cell::new(
                self.schema.get_field(&c).map_or("?", |f| &f.dtype.as_str()),
            ));
        }
        table.add_row(row);
        table.add_row(type_row);

        for record in self.iter().take(num_rows) {
            let mut row = Row::empty();

            for c in record.iter() {
                row.add_cell(Cell::new(c.to_string().as_str()));
            }

            table.add_row(row);
        }

        let rows_displayed = std::cmp::min(num_rows, self.dim.1);

        table.add_row(Row::new(vec![Cell::new(&format!(
            "Displayed {} of {} rows",
            rows_displayed, self.dim.1
        ))
        .with_hspan(self.dim.0)]));

        table.printstd();
    }

    /// Clear the schema, and data, and reset the dimensions
    pub fn clear(&mut self) {
        self.schema.clear();
        self.data.to_mut().clear();
        self.dim.0 = 0;
        self.dim.1 = 0;
    }
}

/// TODO this currently loses the data type for the columns, has access to the schema, needs to be
/// updated to use it when re-creating the data frame
impl<'a> FromIterator<SubView<'a>> for DataFrame<'a> {
    fn from_iter<I: IntoIterator<Item = SubView<'a>>>(iter: I) -> Self {
        let mut view = iter.into_iter();
        let first = view.next();

        if first.is_none() {
            return DataFrame::empty();
        }

        let first = first.unwrap();

        let mut df = DataFrame::with_columns(&first.columns());

        df.push_row_unchecked(first.data().into_owned());

        let data = view
            .map(|row| row.data().into_owned())
            .collect::<Vec<Vec<Value>>>();

        df.extend_unchecked(data);

        df
    }
}

/// Get the row at the specific index
impl<'a> Index<usize> for DataFrame<'a> {
    type Output = [Value];

    fn index(&self, index: usize) -> &[Value] {
        let (start, end) = self.dim.get_row_range(index);

        &self.data[start..end]
    }
}

#[cfg(test)]
mod dataframe_tests {
    use super::*;

    #[test]
    fn it_iterates() {
        let df = DataFrame::new(
            &["a", "b"],
            vec![vec![1.into(), 10.into()], vec![2.into(), 20.into()]],
        );

        {
            let mut iter = df.iter();

            let row = iter.next();
            assert!(row.is_some());
            let row = row.unwrap();
            assert_eq!(row, &[1.into(), 10.into()] as &[Value]);

            let row = iter.next();

            assert!(row.is_some());
            let row = row.unwrap();
            assert_eq!(row, &[2.into(), 20.into()] as &[Value]);

            let row = iter.next();
            assert!(row.is_none());
        }

        assert_eq!(df.shape(), (2, 2));
    }

    #[test]
    fn it_pushes_column() {
        let mut df = DataFrame::empty();

        df.push_column("a");
        df.push_column("b");

        assert_eq!(df.shape(), (2, 0));
    }

    #[test]
    fn it_pushes_column_and_reshapes_data() {
        // shape (1,2) to (2,2)
        let mut df = DataFrame::new(&[String::from("a")], vec![vec![1.into()], vec![2.into()]]);

        assert_eq!(df.shape(), (1, 2));

        df.push_column("b");

        assert_eq!(df.shape(), (2, 2));
        assert_eq!(df[0], [1.into(), Value::Null]);
        assert_eq!(df[1], [2.into(), Value::Null]);

        // shape (2, 2) to (3, 2)
        df.push_column("c");

        assert_eq!(df.shape(), (3, 2));
        assert_eq!(df[0], [1.into(), Value::Null, Value::Null]);
        assert_eq!(df[1], [2.into(), Value::Null, Value::Null]);
    }

    #[test]
    fn it_removes_column_and_reshapes() {
        let mut df = DataFrame::with_columns(&["a", "b"]);

        assert!(df
            .extend(vec![vec![1.into(), 10.into()], vec![2.into(), 20.into()]])
            .is_ok());

        assert_eq!(df.shape(), (2, 2));
        assert!(df.remove_column(0).is_ok());
        assert_eq!(df.shape(), (1, 2));

        assert_eq!(df.columns(), ["b"]);

        assert_eq!(df[0], [10.into()]);
        assert_eq!(df[1], [20.into()]);
    }

    #[test]
    fn it_pushes_data() {
        let mut df = DataFrame::new(&["a"], vec![vec![1.into()]]);

        assert_eq!(df.shape(), (1, 1));

        let res = df.push_row(vec![2.into()]);
        assert!(res.is_ok());

        assert_eq!(df.shape(), (1, 2));
        assert_eq!(df[0], [1.into()]);
        assert_eq!(df[1], [2.into()]);

        // don't push row of incorrect length
        assert!(df.push_row(vec![1.into(), 2.into()]).is_err());
    }

    #[test]
    fn it_iterates_column_values() {
        let mut df = DataFrame::new(
            &["a", "b", "c"],
            vec![
                vec![0.into(), 1.into(), 2.into()],
                vec![3.into(), 4.into(), 5.into()],
                vec![6.into(), 7.into(), 8.into()],
                vec![9.into(), 10.into(), 11.into()],
            ],
        );

        let mut count = 0;
        let expected_values: Vec<Value> = vec![1.into(), 4.into(), 7.into(), 10.into()];

        let res = df.map_column("b", |v| {
            assert_eq!(v, &expected_values[count]);
            count += 1;
            Ok(())
        });

        assert!(res.is_ok());
        assert_eq!(count, 4);
    }

    #[test]
    fn it_casts_column() {
        let mut df = DataFrame::new(
            &["a", "b"],
            vec![
                vec![0.into(), 1.into()],
                vec![2.into(), 3.into()],
                vec![4.into(), 5.into()],
            ],
        );

        let cast_result = df.cast_column("a", DataType::Int64);

        assert!(cast_result.is_ok());
    }

    #[test]
    fn it_derives_schema_from_data() {
        let mut df = DataFrame::new(
            &["a", "b"],
            vec![
                vec![0.into(), 1.into()],
                vec![2.into(), 3.into()],
                vec![4.into(), 5.into()],
            ],
        );

        df.derive_schema();
    }
}
