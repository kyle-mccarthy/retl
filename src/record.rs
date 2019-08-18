use crate::error;
use crate::error::ResultExt;
use crate::Value;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::iter::{FromIterator, IntoIterator};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record(pub(crate) Vec<Value>);

impl Record {
    pub fn iter(&self) -> std::slice::Iter<Value> {
        self.0.iter()
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.0.get(index)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Record {
    fn from(v: Vec<T>) -> Record {
        Record(
            v.into_iter()
                .map(Into::<Value>::into)
                .collect::<Vec<Value>>(),
        )
    }
}

impl Into<Vec<String>> for Record {
    fn into(self) -> Vec<String> {
        self.into_iter()
            .map(Into::<String>::into)
            .collect::<Vec<String>>()
    }
}

impl Into<Vec<String>> for &Record {
    fn into(self) -> Vec<String> {
        self.iter()
            .map(Into::<String>::into)
            .collect::<Vec<String>>()
    }
}

impl TryInto<Vec<u8>> for &Record {
    type Error = error::Error;

    fn try_into(self) -> Result<Vec<u8>, error::Error> {
        bincode::serialize(self)
            .context(error::ErrorKind::Serialize)
            .map_err(Into::<error::Error>::into)
    }
}

impl FromIterator<Value> for Record {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Value>,
    {
        Record(FromIterator::from_iter(iter))
    }
}

impl IntoIterator for Record {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
