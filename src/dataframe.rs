use crate::error::{ErrorKind, Result};
use crate::Value;
use std::iter::FromIterator;
use std::iter::Iterator;
use std::ops::{Index, IndexMut};

type OwnedValue = Value;
type BorrowedValue<'a> = &'a Value;
type BorrowedMutValue<'a> = &'a mut Value;

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum Layout {
    Row,
    Column,
}

// TODO adopt approach similar to ndarray where value has special repr / owned vs borrowed etc...
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct DataFrame {
    columns: Vec<String>,
    data: Vec<Value>,
    dim: Dim,
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub struct Dim(pub(crate) usize, usize);

impl Default for Dim {
    fn default() -> Self {
        Dim(0, 0)
    }
}

impl Dim {
    pub(crate) fn new(x: usize, y: usize) -> Self {
        Dim(x, y)
    }

    pub fn expected_len(&self) -> usize {
        self.0 * self.1
    }

    /// Get the start and end index for a row
    pub fn get_row_range(&self, row: usize) -> (usize, usize) {
        let i = self.0 * row;
        (i, i + self.0)
    }

    /// Calculate the position of a value in the buffer from the row number and the index of the
    /// column
    pub fn get_value_index(&self, row_number: usize, column_index: usize) -> usize {
        (row_number * self.0) + column_index
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.0, self.1)
    }
}

impl std::default::Default for DataFrame {
    fn default() -> Self {
        DataFrame {
            columns: vec![],
            data: vec![],
            dim: Dim::default(),
        }
    }
}

impl DataFrame {
    pub fn empty() -> DataFrame {
        DataFrame::default()
    }

    pub fn new<S: Into<String>>(columns: Vec<S>, data: Vec<Vec<Value>>) -> DataFrame {
        let dim = Dim::new(columns.len(), data.len());

        // TODO ensure that the length of the flattened data is equal the length of the columns *
        // length of the data -- panic or return result
        let data = data.into_iter().flatten().collect::<Vec<Value>>();

        let columns = columns
            .into_iter()
            .map(Into::<String>::into)
            .collect::<Vec<String>>();

        DataFrame {
            columns,
            data,
            dim,
            ..Default::default()
        }
    }

    pub fn with_columns<S: Into<String>>(columns: Vec<S>) -> DataFrame {
        let columns = columns
            .into_iter()
            .map(Into::<String>::into)
            .collect::<Vec<String>>();

        let dim = Dim::new(columns.len(), 0);

        DataFrame {
            columns,
            dim,
            ..Default::default()
        }
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn push_column<S: Into<String>>(&mut self, column: S) {
        self.columns.push(column.into());
        self.dim.0 += 1;

        let col_count = self.dim.0;
        let col_index = col_count - 1;

        for row_num in 0..self.dim.1 {
            let index = self.dim.get_value_index(row_num, col_index);

            // insert NULL into the new column for each row at the index. If the index exceeds the
            // current length, push the value;
            if index > self.data.len() {
                self.data.push(Value::Null);
            } else {
                self.data.insert(index, Value::Null);
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
            dbg!(index);
            self.data.remove(index);
        }

        self.dim.0 -= 1;
        self.columns.remove(column);

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
        self.data.extend(data);
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
            .extend(data.into_iter().flatten().collect::<Vec<Value>>());
    }

    pub fn clear(&mut self) {
        self.columns.clear();
        self.data.clear();
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

        Some(&self.data[start..end])
    }

    pub fn row_mut(&mut self, row: usize) -> Option<&mut [Value]> {
        let (start, end) = self.dim.get_row_range(row);

        if self.data.len() < end {
            return None;
        }

        Some(&mut self.data[start..end])
    }

    pub fn rows(&self) -> usize {
        self.dim.1
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self)
    }

    // pub fn iter_mut(&mut self) -> IterMut {
    //     IterMut::new(self)
    // }

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
}

pub trait Get<T> {
    type Output;

    fn get(&self, index: T) -> Option<&Self::Output>;
}

impl Get<(usize, usize)> for DataFrame {
    type Output = Value;

    fn get(&self, index: (usize, usize)) -> Option<&Self::Output> {
        let index = self.dim.get_value_index(index.0, index.1);

        self.data.get(index)
    }
}

impl Get<usize> for DataFrame {
    type Output = Value;

    fn get(&self, index: usize) -> Option<&Self::Output> {
        self.data.get(index)
    }
}

// TODO determine if a view/window should be returned vs slice
// pub struct View<'a> {
//     data: &'a [Value],
//     columns: &'a [String],
// }

/// Get the row at the specific index, returns an empty slice if index out of bounds
impl Index<usize> for DataFrame {
    type Output = [Value];

    fn index(&self, index: usize) -> &[Value] {
        let (start, end) = self.dim.get_row_range(index);

        // return an empty slice if we are trying to access a bucket that is out of bounds
        if end > self.data.len() {
            return &[];
        }

        &self.data[start..end]
    }
}

impl Index<(usize, usize)> for DataFrame {
    type Output = Value;

    fn index(&self, (row, col): (usize, usize)) -> &Value {
        &self.data[self.dim.get_value_index(row, col)]
    }
}

impl IndexMut<usize> for DataFrame {
    fn index_mut(&mut self, index: usize) -> &mut [Value] {
        let (start, end) = self.dim.get_row_range(index);

        if end > self.data.len() {
            return &mut [];
        }

        &mut self.data[start..end]
    }
}

#[derive(Debug, Clone)]
pub struct Iter<'a> {
    ptr: usize,
    df: &'a DataFrame,
}

impl<'a> Iter<'a> {
    pub fn new(df: &'a DataFrame) -> Iter {
        Iter { ptr: 0, df }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a [Value];

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.df.row(self.ptr);
        self.ptr += 1;
        next
    }
}

// #[derive(Debug)]
// pub struct IterMut<'a> {
//     ptr: usize,
//     df: &'a mut DataFrame,
// }

// impl<'a> IterMut<'a> {
//     pub fn new(df: &'a mut DataFrame) -> IterMut {
//         IterMut { ptr: 0, df }
//     }
// }

// impl<'a> Iterator for IterMut<'a> {
//     type Item = &'a mut [Value];

//     fn next(&mut self) -> Option<Self::Item> {
//         let next = self.df.row_mut(self.ptr);

//         self.ptr += 1;
//         next
//     }
// }
// impl IntoIterator for DataFrame {
//     type Item = Vec<Value>;
//     type IntoIterator = Iter<'a>
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_iterates() {
        let df = DataFrame::new(
            vec!["a", "b"],
            vec![vec![1.into(), 10.into()], vec![2.into(), 20.into()]],
        );

        {
            let df = df.clone();
            let mut iter = df.iter();

            let row = iter.next();
            assert!(row.is_some());
            let row = row.unwrap();
            assert_eq!(row, [1.into(), 10.into()]);

            let row = iter.next();
            assert!(row.is_some());
            let row = row.unwrap();
            assert_eq!(row, [2.into(), 20.into()]);

            let row = iter.next();
            assert!(row.is_none());
        }

        assert_eq!(df.shape(), (2, 2));

        {
            //let df2 = df.iter_mut().map(|record| {
            //    //record[0] *= 2;
            //    record
            //});
        }
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
        let mut df = DataFrame::new(
            vec![String::from("a")],
            vec![vec![1.into()], vec![2.into()]],
        );

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
        let mut df = DataFrame::with_columns(vec!["a", "b"]);

        assert!(df
            .extend(vec![vec![1.into(), 10.into()], vec![2.into(), 20.into()]])
            .is_ok());

        assert_eq!(df.shape(), (2, 2));
        assert!(df.remove_column(0).is_ok());
        assert_eq!(df.shape(), (1, 2));

        assert_eq!(df.columns(), &vec![String::from("b")]);

        assert_eq!(df[0], [10.into()]);
        assert_eq!(df[1], [20.into()]);
    }

    #[test]
    fn it_pushes_data() {
        let mut df = DataFrame::new(vec!["a"], vec![vec![1.into()]]);

        assert_eq!(df.shape(), (1, 1));

        let res = df.push_row(vec![2.into()]);
        assert!(res.is_ok());

        assert_eq!(df.shape(), (1, 2));
        assert_eq!(df[0], [1.into()]);
        assert_eq!(df[1], [2.into()]);

        // don't push row of incorrect length
        assert!(df.push_row(vec![1.into(), 2.into()]).is_err());
    }

}
