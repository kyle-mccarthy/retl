use crate::{Record, Value};
use std::iter::Iterator;

#[derive(Debug)]
pub struct DataFrame {
    columns: Vec<String>,
    data: Vec<Record>,
}

impl std::default::Default for DataFrame {
    fn default() -> Self {
        DataFrame {
            columns: vec![],
            data: vec![],
        }
    }
}

impl DataFrame {
    pub fn new(columns: Vec<String>, data: Vec<Record>) -> DataFrame {
        DataFrame { columns, data }
    }

    pub fn get_column_idx(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|col| &name == col)
    }

    pub fn get_columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn set_columns(&mut self, columns: Vec<String>) {
        self.columns = columns
    }

    pub fn push_column(&mut self, column: String) {
        self.columns.push(column)
    }

    pub fn get_data(&self) -> &Vec<Record> {
        &self.data
    }

    pub fn set_data<T: Into<Record>>(&mut self, data: Vec<T>) {
        self.data = data
            .into_iter()
            .map(Into::<Record>::into)
            .collect::<Vec<Record>>()
    }

    pub fn push_data<T: Into<Record>>(&mut self, row: T) {
        self.data.push(Into::<Record>::into(row))
    }

    pub fn get(&self, i: usize) -> Option<&Record> {
        self.data.get(i)
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.data.len(), self.columns.len())
    }

    pub fn iter(&self) -> DataFrameIterator<'_> {
        DataFrameIterator(self.data.iter())
    }

    pub fn into_iter(self) -> std::vec::IntoIter<Record> {
        self.data.into_iter()
    }

    /// Consumes iterator of self.data, filtering out data based on some predicate and reassigning
    /// the filtered data back to itself
    pub fn filter<F>(mut self, pred: F) -> Self
    where
        F: FnMut(&Record) -> bool,
    {
        self.data = self.data.into_iter().filter(pred).collect::<Vec<Record>>();
        self
    }

    /// Print the data frame to std out for debugging
    /// You can limit the number of rows shown with  the num_rows parameter. Will print at most
    /// num_rows, 0 prints all rows.
    pub fn debug(&self, num_rows: usize) {
        use prettytable::{format::Alignment, Cell, Row, Table};

        let mut table = Table::new();

        let mut row = Row::empty();
        for c in self.get_columns() {
            row.add_cell(Cell::new(&c));
        }
        table.add_row(row);

        for (n, record) in self.get_data().iter().enumerate() {
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

pub struct DataFrameIterator<'a>(std::slice::Iter<'a, Record>);

impl<'a> Iterator for DataFrameIterator<'a> {
    type Item = &'a Record;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_iterates() {
        let mut df = DataFrame::default();
        df.push_column(String::from("a"));
        df.push_column(String::from("b"));

        let data: Vec<Value> = vec!["Col(A) -- Row(1)".into(), 1u32.into()];
        df.push_data(data);

        let data: Vec<Value> = vec!["Col(A) -- Row(2)".into(), 2u32.into()];
        df.push_data(data);

        assert_eq!(df.shape(), (2, 2));

        let mut df_iter = df.iter();

        let item = df_iter.next();
        assert!(item.is_some());
        let item = item.unwrap();

        assert_eq!(
            *item.get(0).unwrap(),
            Value::String("Col(A) -- Row(1)".into())
        );
        assert_eq!(*item.get(1).unwrap(), Value::Number(1u32.into()));

        let item = df_iter.next();
        assert!(item.is_some());
        let item = item.unwrap();

        assert_eq!(
            *item.get(0).unwrap(),
            Value::String("Col(A) -- Row(2)".into())
        );
        assert_eq!(*item.get(1).unwrap(), Value::Number(2u32.into()));

        assert!(df_iter.next().is_none());

        assert_eq!(df.size(), 2);
    }
}
