use crate::error;
use crate::{value::map, value::number, Value};
use serde_json::{value::Number as JsonNumber, Value as JsonValue};
use std::iter::FromIterator;

pub trait Json {
    fn to_json(&self) -> Result<String, error::Error>;
}

impl From<JsonNumber> for number::Num {
    fn from(num: JsonNumber) -> number::Num {
        if num.is_i64() {
            let int: i64 = num
                .as_i64()
                .expect("is_i64 indicated true - should be able to represent as i64");
            return number::Num::from(int);
        }

        if num.is_u64() {
            let int: u64 = num
                .as_u64()
                .expect("is_u64 indicated true - should be able to represent as u64");
            return number::Num::from(int);
        }

        if num.is_f64() {
            let float: f64 = num
                .as_f64()
                .expect("is_f64 indicated true - should be able to represent as f64");
            return number::Num::from(float);
        }

        panic!(
            "JSON number was not i64, u64, of f64, but those are the only JSON numbers available"
        )
    }
}

impl From<JsonValue> for Value {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => Value::Null,
            JsonValue::String(s) => Value::String(s),
            JsonValue::Bool(b) => Value::Bool(b),
            JsonValue::Number(n) => Value::Number(n.into()),
            JsonValue::Array(a) => Value::Array(
                a.into_iter()
                    .map(Into::<Value>::into)
                    .collect::<Vec<Value>>(),
            ),
            JsonValue::Object(obj) => {
                let iter = obj
                    .into_iter()
                    .map(|(key, value)| (key, Into::<Value>::into(value)));
                let map_conv = map::Map::from_iter(iter);
                Value::Map(map_conv)
            }
        }
    }
}
