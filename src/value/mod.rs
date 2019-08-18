pub mod map;
pub mod number;

use crate::error::{self as error, ResultExt};
use chrono::NaiveDateTime;
use map::Map;
use number::Number;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::{From, TryInto};
use std::ops::{Index, Deref};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    String(String),
    Array(Vec<Value>),
    Map(Map),
    Number(Number),
    Date(NaiveDateTime),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (&self, &other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Number(a), Value::Number(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (Value::Array(a), Value::Array(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl<T: Into<Number>> From<T> for Value {
    fn from(n: T) -> Self {
        Value::Number(n.into())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Bool(b)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::String(s)
    }
}

impl<'a> From<&'a str> for Value {
    fn from(s: &'a str) -> Value {
        Value::String(s.into())
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Value {
        Value::Array(v)
    }
}

impl Into<String> for Value {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl Into<String> for &Value {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl TryInto<Vec<u8>> for &Value {
    type Error = error::Error;

    fn try_into(self) -> std::result::Result<Vec<u8>, Self::Error> {
        bincode::serialize(self)
            .context(error::ErrorKind::Serialize)
            .map_err(Into::<error::Error>::into)
    }
}

impl<'a, T: Clone + Into<Value>> From<&'a [T]> for Value
where
    number::Integer: std::convert::From<&'a T>,
    number::Number: std::convert::From<&'a T>,
{
    fn from(s: &'a [T]) -> Self {
        Value::Array(s.iter().clone().map(Into::into).collect())
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        match (&self, &other) {
            (&Value::Null, &Value::Null) => true,
            (&Value::Bool(a), &Value::Bool(b)) => a == b,
            (&Value::String(a), &Value::String(b)) => a == b,
            (&Value::Number(a), &Value::Number(b)) => a == b,
            (&Value::Array(a), &Value::Array(b)) => a == b,
            (&Value::Map(a), &Value::Map(b)) => a == b,
            (&Value::Date(a), &Value::Date(b)) => a == b,
            _ => false, // values of other types can't be compared to one another
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            Value::Null => write!(f, "null"),
            Value::String(s) => write!(f, "{}", s),
            Value::Number(n) => write!(f, "{}", n),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Date(d) => write!(f, "{}", d),
            Value::Map(_m) => write!(f, "display not implemented for map"),
            Value::Array(_a) => write!(f, "display not implemented for array"),
        }
    }
}

impl<'a> Index<&'a str> for Value {
    type Output = Value;

    fn index(&self, index: &'a str) -> &Self::Output {
        match self {
            Value::Map(map) => map.index(index),
            _ => &Value::Null
        }
    }
}

impl Index<String> for Value {
    type Output = Value;

    fn index(&self, index: String) -> &Self::Output {
        self.index(index.deref())
    }
}

impl<'a> Index<&'a String> for Value {
    type Output = Value;

    fn index(&self, index: &'a String) -> &Self::Output {
        self.index(index.deref())
    }
}

#[cfg(test)]
mod tests {
    // @todo test for partial cmp
}
