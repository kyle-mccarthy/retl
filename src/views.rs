use crate::{DataFrame, Get, Value};

use std::borrow::Cow;
use std::iter::Iterator;
use std::ops::Index;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct View<'a, 'b: 'a> {
    ptr: usize,
    df: &'a DataFrame<'b>,
}

impl<'a, 'b> View<'a, 'b> {
    pub fn new(df: &'a DataFrame<'b>) -> View<'a, 'b> {
        View { ptr: 0, df }
    }
}

impl<'a, 'b> Iterator for View<'a, 'b> {
    type Item = SubView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (start, end) = self.df.dim.get_row_range(self.ptr);

        if end > self.df.data.len() {
            return None;
        }

        self.ptr += 1;

        Some(SubView::new(
            Cow::Borrowed(&self.df.columns),
            Cow::Borrowed(&self.df.data[start..end]),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct SubView<'a> {
    columns: Cow<'a, [String]>,
    data: Cow<'a, [Value]>,
}

impl<'a> SubView<'a> {
    pub fn new(columns: Cow<'a, [String]>, data: Cow<'a, [Value]>) -> SubView<'a> {
        SubView { columns, data }
    }

    pub fn data(&self) -> Cow<'a, [Value]> {
        self.data.clone()
    }

    pub fn columns(&self) -> Cow<'a, [String]> {
        self.columns.clone()
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|item| item == name)
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.column_index(name).is_some()
    }

    pub fn iter(&self) -> std::slice::Iter<Value> {
        self.data.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<Value> {
        self.data.to_mut().iter_mut()
    }
}

/// Get the value by position in the inner data slice. This will panic if the index is out of
/// bounds. Prefer using get.
impl<'a> Index<usize> for SubView<'a> {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

/// Get the value by the name of the column, this will panic if the column doesn't exist. Prefer
/// using get.
impl<'a> Index<&str> for SubView<'a> {
    type Output = Value;

    fn index(&self, index: &str) -> &Self::Output {
        &self.data[self.column_index(index).unwrap()]
    }
}

/// Get the value by the position in the inner slice.
impl<'a> Get<usize> for SubView<'a> {
    type Output = Value;

    fn get(&self, index: usize) -> Option<&Self::Output> {
        self.data.get(index)
    }

    fn get_mut(&mut self, index: usize) -> Option<&mut Self::Output> {
        self.data.to_mut().get_mut(index)
    }
}

/// Get the value by a columns name.
impl<'a> Get<&str> for SubView<'a> {
    type Output = Value;

    fn get(&self, index: &str) -> Option<&Self::Output> {
        match self.column_index(index) {
            Some(index) => self.data.get(index),
            None => None,
        }
    }

    fn get_mut(&mut self, index: &str) -> Option<&mut Self::Output> {
        match self.column_index(index) {
            Some(index) => self.data.to_mut().get_mut(index),
            None => None,
        }
    }
}

impl<'a> PartialEq<&[Value]> for SubView<'a> {
    fn eq(&self, rhs: &&[Value]) -> bool {
        &self.data == rhs
    }
}

impl<'a> PartialEq<[Value]> for SubView<'a> {
    fn eq(&self, rhs: &[Value]) -> bool {
        self.data == rhs
    }
}
