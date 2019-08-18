
use crate::error;
use serde_json::Value as JsonValue;
use std::iter::FromIterator;
use crate::{Value, value::number, value::map};

pub trait Json {
    fn to_json(&self) -> Result<String, error::Error>;
}

impl From<JsonValue> for Value {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => Value::Null,
            JsonValue::String(s) => Value::String(s),
            JsonValue::Bool(b) => Value::Bool(b),
            JsonValue::Number(n) => {
                let num = if n.is_f64() {
                    number::Number::Decimal(n.as_f64().expect("n.is_f64() but returned None?")) 
                } else if n.is_u64() {
                    number::Number::Integer(number::Integer::Positive(n.as_u64().expect("n.is_u64() but returned None?")))
                } else {
                    number::Number::Integer(number::Integer::Negative(n.as_i64().expect("n.is_i64() but returned None?")))
                };
                Value::Number(num)
            },
            JsonValue::Array(a) => Value::Array(a.into_iter().map(Into::<Value>::into).collect::<Vec<Value>>()),
            JsonValue::Object(obj) => {
                let iter = obj.into_iter().map(
                    |(key, value)| (key, Into::<Value>::into(value)));
                let map_conv = map::Map::from_iter(iter);
                Value::Map(map_conv)
            },
        }
    }
}

