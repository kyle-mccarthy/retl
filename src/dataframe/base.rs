use crate::dataframe::{Container, DataFrame, DataSlice, Dim, View};
use crate::Value;

use std::ops::Index;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct BaseDataFrame<D: Container> {
    pub(crate) columns: Vec<String>,
    pub(crate) data: D,
    pub(crate) dim: Dim,
    pub(crate) ptr: usize,
}

impl<D: Container> BaseDataFrame<D> {
    pub fn into_view<'a>(dim: Dim, columns: Vec<String>, data: DataSlice<'a>) -> View<'a> {
        BaseDataFrame {
            columns,
            data,
            dim,
            ptr: 0,
        }
    }

    pub fn empty() -> DataFrame {
        BaseDataFrame::default()
    }

    pub fn with_columns<S: Into<String>>(columns: Vec<S>) -> DataFrame {
        let columns = columns
            .into_iter()
            .map(Into::<String>::into)
            .collect::<Vec<String>>();

        let dim = Dim::new(columns.len(), 0);

        BaseDataFrame {
            columns,
            dim,
            ..Default::default()
        }
    }

    pub fn columns(&self) -> &Vec<String> {
        &self.columns
    }

    pub fn size(&self) -> usize {
        self.dim.1
    }

    pub fn shape(&self) -> (usize, usize) {
        self.dim.shape()
    }

    pub fn row<'a>(&self, row: usize) -> Option<&[Value]> {
        let (start, end) = self.dim.get_row_range(row);

        if self.data.len() < end {
            return None;
        }

        Some(&self.data.as_ref()[start..end])
    }

    pub fn rows(&self) -> usize {
        self.dim.1
    }

    pub fn iter<'a>(&'a self) -> View<'a> {
        View {
            ptr: 0,
            data: &self.data.as_ref(),
            columns: self.columns.clone(),
            dim: self.dim.clone(),
        }
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
}

impl<D: Container> Index<(usize, usize)> for BaseDataFrame<D> {
    type Output = Value;

    fn index(&self, (row, col): (usize, usize)) -> &Value {
        &self.data.as_ref()[self.dim.get_value_index(row, col)]
    }
}
