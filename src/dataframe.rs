use crate::{
    dim::Dim,
    error::{ErrorKind, Result},
    views::{SubView, View},
    Value,
};

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::iter::{FromIterator, Iterator};
use std::ops::Index;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DataFrame<'a> {
    pub(crate) columns: Cow<'a, [String]>,
    pub(crate) data: Cow<'a, [Value]>,
    pub(crate) dim: Dim,
}

impl<'a> std::default::Default for DataFrame<'a> {
    fn default() -> Self {
        DataFrame {
            columns: Cow::from(vec![]),
            data: Cow::from(vec![]),
            dim: Dim::default(),
        }
    }
}

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

        // let columns = columns
        //     .to_owned()
        //     .into_iter()
        //     .map(Into::<String>::into)
        //     .collect::<Vec<String>>();

        DataFrame {
            columns: Cow::Owned(columns),
            data: data.into(),
            dim,
            ..Default::default()
        }
    }

    pub fn empty() -> DataFrame<'a> {
        DataFrame::default()
    }

    pub fn with_columns<S>(columns: &[S]) -> DataFrame<'a>
    where
        S: Into<String> + Clone,
        [S]: ToOwned,
        [S]: ToOwned<Owned = Vec<S>>,
    {
        DataFrame::new(columns, vec![])
    }

    pub fn iter(&self) -> View<'_, 'a> {
        View::new(&self)
    }

    pub fn push_column<S: Into<String>>(&mut self, column: S) {
        self.columns.to_mut().push(column.into());
        self.dim.0 += 1;

        let col_count = self.dim.0;
        let col_index = col_count - 1;

        for row_num in 0..self.dim.1 {
            let index = self.dim.get_value_index(row_num, col_index);

            // insert NULL into the new column for each row at the index. If the index exceeds the
            // current length, push the value;
            if index > self.data.len() {
                self.data.to_mut().push(Value::Null);
            } else {
                self.data.to_mut().insert(index, Value::Null);
            }
        }
    }

    pub fn remove_column(&mut self, column: usize) -> Result<()> {
        if column >= self.columns.len() {
            return Err(ErrorKind::IndexOutofBounds(column, self.columns.len()).into());
        }

        // starting from the end of our value buffer delete elements for associated with the index
        // being removed
        for row_num in (0..self.dim.1).rev() {
            let index = self.dim.get_value_index(row_num, column);
            self.data.to_mut().remove(index);
        }

        self.dim.0 -= 1;
        self.columns.to_mut().remove(column);

        Ok(())
    }

    /// Pushes new row onto the data, performs a check to ensure the length equals the number of
    /// columns
    pub fn push_row(&mut self, data: Vec<Value>) -> Result<usize> {
        if data.len() != self.dim.0 {
            return Err(ErrorKind::InvalidDataLength(self.dim.0, data.len()).into());
        }

        self.push_row_unchecked(data);

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
            Some(invalid_row) => {
                Err(ErrorKind::InvalidDataLength(self.dim.0, invalid_row.len()).into())
            }
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

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn size(&self) -> usize {
        self.dim.1
    }

    pub fn shape(&self) -> (usize, usize) {
        self.dim.shape()
    }

    pub fn row(&self, row: usize) -> Option<&[Value]> {
        let (start, end) = self.dim.get_row_range(row);

        if self.data.len() < end {
            return None;
        }

        Some(&self.data.as_ref()[start..end])
    }

    pub fn rows(&self) -> usize {
        self.dim.1
    }

    /// Print the data frame to std out for debugging
    /// You can limit the number of rows shown with  the num_rows parameter. Will print at most
    /// num_rows, 0 prints all rows.
    pub fn debug(&self, num_rows: usize) {
        use prettytable::{format::Alignment, Cell, Row, Table};

        let mut table = Table::new();

        let mut row = Row::empty();
        for c in self.columns() {
            row.add_cell(Cell::new(&c));
        }
        table.add_row(row);

        for (n, record) in self.iter().enumerate() {
            let mut row = Row::empty();

            for c in record.iter() {
                row.add_cell(Cell::new(c.to_string().as_str()));
            }

            table.add_row(row);

            if n + 1 == num_rows {
                let (_, width) = self.shape();
                table.add_row(Row::new(vec![
                    Cell::new_align("...", Alignment::CENTER).with_hspan(width)
                ]));
                break;
            }
        }

        table.printstd();
    }

    pub fn clear(&mut self) {
        self.columns.to_mut().clear();
        self.data.to_mut().clear();
        self.dim.0 = 0;
        self.dim.1 = 0;
    }
}

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

/// Get the row at the specific index, returns an empty slice if index out of bounds
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

        assert_eq!(df.columns(), [String::from("b")]);

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
    fn it_df_from_iterator() {
        // let mut df = DataFrame::new(vec!, data: Vec<Vec<Value>>)
    }

}
