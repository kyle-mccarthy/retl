use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Dim(pub(crate) usize, pub(crate) usize);

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
