use crate::Value;

pub trait Container: AsRef<[Value]> + Sized {
    fn len(&self) -> usize;
}

pub type Data = Vec<Value>;
pub type DataSlice<'a> = &'a [Value];

impl Container for Data {
    fn len(&self) -> usize {
        self.len()
    }
}

impl<'a> Container for DataSlice<'a> {
    fn len(&self) -> usize {
        (*self).len()
    }
}
