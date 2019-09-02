use crate::schema::DataType;

pub trait Get<T> {
    type Output;

    fn get(&self, index: T) -> Option<&Self::Output>;
    fn get_mut(&mut self, index: T) -> Option<&mut Self::Output>;
}

pub trait TypeOf {
    fn type_of(&self) -> DataType;
}
