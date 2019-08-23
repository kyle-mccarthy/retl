use crate::dataframe::{BaseDataFrame, DataFrame, DataSlice};
use crate::Value;

use std::ops::Index;

pub type View<'a> = BaseDataFrame<DataSlice<'a>>;

impl<'a> Iterator for View<'a> {
    type Item = &'a [Value];

    fn next(&mut self) -> Option<Self::Item> {
        let (start, end) = self.dim.get_row_range(self.ptr);

        if end > self.data.len() {
            return None;
        }

        Some(&self.data[start..end])
    }
}

impl<'a> Index<usize> for View<'a> {
    type Output = [Value];

    fn index(&self, index: usize) -> &[Value] {
        let (start, end) = self.dim.get_row_range(index);

        &self.data[start..end]
    }
}

impl<'a> View<'a> {
    pub fn to_df(self) -> DataFrame {
        DataFrame::from_view(self)
    }
}
