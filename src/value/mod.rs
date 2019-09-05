pub mod map;
pub mod number;

use crate::{schema::DataType, traits::TypeOf};
use map::Map;
use number::Number;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::convert::From;
use std::ops::{Deref, Index};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Bool(bool),
    String(String),
    Array(Vec<Value>),
    Map(Map),
    Number(Number),
    Date(NaiveDateTime),
    Binary(Vec<u8>),
}

impl Value {
    pub fn is_numeric(&self) -> bool {
        match self {
            Value::Number(_) => true,
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Value::Null => true,
            _ => false,
        }
    }
}

impl TypeOf for Value {
    fn type_of(&self) -> DataType {
        match self {
            Value::Bool(_) => DataType::Bool,
            Value::String(_) => DataType::String,
            Value::Array(_) => DataType::Array,
            Value::Map(_) => DataType::Map,
            Value::Number(n) => n.type_of(),
            Value::Date(_) => DataType::Date,
            Value::Binary(_) => DataType::Binary,
            _ => DataType::Any,
        }
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

// impl From<Vec<u8>> for Value {
//     fn from(v: Vec<u8>) -> Value {
//         Value::Binary(v)
//     }
// }

impl Into<String> for Value {
    fn into(self) -> String {
        format!("{}", self)
    }
}

impl<N: Into<Number>> From<N> for Value {
    fn from(n: N) -> Value {
        Value::Number(n.into())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(s: Vec<T>) -> Self {
        Value::Array(s.into_iter().map(Into::into).collect())
    }
}

impl<'a, T: Clone + Into<Value>> From<&'a [T]> for Value {
    fn from(s: &'a [T]) -> Self {
        Value::Array(s.iter().cloned().map(Into::into).collect())
    }
}

impl<T: Into<Value>> std::iter::FromIterator<T> for Value {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Value::Array(iter.into_iter().map(Into::into).collect())
    }
}

// impl PartialEq for Value {
//     fn eq(&self, other: &Value) -> bool {
//         match (&self, &other) {
//             (&Value::Null, &Value::Null) => true,
//             (&Value::Bool(a), &Value::Bool(b)) => a == b,
//             (&Value::String(a), &Value::String(b)) => a == b,
//             (&Value::Number(a), &Value::Number(b)) => a == b,
//             (&Value::Array(a), &Value::Array(b)) => a == b,
//             (&Value::Map(a), &Value::Map(b)) => a == b,
//             (&Value::Date(a), &Value::Date(b)) => a == b,
//             (&Value::Binary(a), &Value::Binary(b)) => a == b,
//             _ => false,
//         }
//     }
// }

// TODO impl Display for <Vec> Value

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
            Value::Binary(_) => write!(f, "[bin data]"),
        }
    }
}

impl<'a> Index<&'a str> for Value {
    type Output = Value;

    fn index(&self, index: &'a str) -> &Self::Output {
        match self {
            Value::Map(map) => map.index(index),
            _ => &Value::Null,
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
