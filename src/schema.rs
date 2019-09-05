use crate::Value;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::{Entry, HashMap};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
pub enum DataType {
    Bool,
    String,
    Array,
    Map,
    Date,
    Binary,

    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,

    Float,
    Double,
    Decimal,

    /// A field can be weakly typed with "any"
    Any,
    /// A field should never be of type "null", this provides mapping between values and data types
    Null,
}

impl DataType {
    pub fn as_str(&self) -> &str {
        match self {
            DataType::Bool => "boolean",
            DataType::String => "string",
            DataType::Array => "array",
            DataType::Map => "object",
            DataType::Date => "date",
            DataType::Binary => "binary",
            DataType::Uint8 => "uint8",
            DataType::Uint16 => "uint16",
            DataType::Uint32 => "uint32",
            DataType::Uint64 => "uint64",
            DataType::Int8 => "int8",
            DataType::Int16 => "int16",
            DataType::Int32 => "int32",
            DataType::Int64 => "int64",
            DataType::Float => "float",
            DataType::Double => "double",
            DataType::Decimal => "decimal",
            DataType::Any => "any",
            DataType::Null => "null",
        }
    }

    pub fn has_default(&self) -> bool {
        match self {
            DataType::Bool => true,
            DataType::String => true,
            DataType::Uint8 => true,
            DataType::Uint16 => true,
            DataType::Uint32 => true,
            DataType::Uint64 => true,
            DataType::Int8 => true,
            DataType::Int16 => true,
            DataType::Int32 => true,
            DataType::Int64 => true,
            DataType::Float => true,
            DataType::Double => true,
            DataType::Decimal => true,
            _ => false,
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Uint8
            | DataType::Uint16
            | DataType::Uint32
            | DataType::Uint64
            | DataType::Float
            | DataType::Decimal
            | DataType::Double => true,
            _ => false,
        }
    }

    /// Get the default value for the data type or return null
    pub fn default_value(&self) -> Value {
        Value::Null
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(fmt, "{}", self.as_str())
    }
}

impl From<&str> for DataType {
    fn from(name: &str) -> DataType {
        match name {
            "boolean" => DataType::Bool,
            "string" => DataType::String,
            "array" => DataType::Array,
            "object" => DataType::Map,
            "date" => DataType::Date,
            "binary" => DataType::Binary,
            "uint8" => DataType::Uint8,
            "uint16" => DataType::Uint16,
            "uint32" => DataType::Uint32,
            "uint64" => DataType::Uint64,
            "int8" => DataType::Int8,
            "int16" => DataType::Int16,
            "int32" => DataType::Int32,
            "int64" => DataType::Int64,
            "float" => DataType::Float,
            "double" => DataType::Double,
            "decimal" => DataType::Decimal,
            "any" => DataType::Any,
            "null" => DataType::Null,
            _ => panic!("{} is not a valid type", name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Field {
    pub(crate) name: String,
    pub(crate) nullable: bool,
    pub(crate) default: Option<Value>,
    pub(crate) doc: Option<String>,
    pub(crate) dtype: DataType,
    pub(crate) index: usize,
}

impl Field {
    pub fn new<S: Into<String>>(name: S) -> Field {
        Field {
            name: name.into(),
            nullable: true,
            default: None,
            doc: None,
            dtype: DataType::Any,
            index: 0,
        }
    }

    pub fn with_index(name: String, index: usize) -> Field {
        Field {
            index,
            ..Field::new(name)
        }
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Schema {
    name: Option<String>,
    doc: Option<String>,
    fields: HashMap<String, Field>,
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            name: None,
            fields: HashMap::new(),
            doc: None,
        }
    }
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn with_fields(fields: Vec<Field>) -> Schema {
        let fields: HashMap<String, Field> = fields
            .into_iter()
            .enumerate()
            .map(|(i, mut f)| {
                f.index = i;
                (f.name.to_string(), f)
            })
            .collect::<HashMap<String, Field>>();

        Schema {
            fields,
            ..Default::default()
        }
    }

    pub fn add_field<S: Into<String> + Clone>(&mut self, name: S) -> usize {
        self.push_field(Field::new(name.into()))
    }

    pub fn push_field(&mut self, mut field: Field) -> usize {
        let index = self.fields.len();
        field.index = index;
        let _ = self.fields.insert(field.name.clone(), field);
        index
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut Field> {
        self.fields.get_mut(name)
    }

    pub fn entry(&mut self, key: String) -> Entry<String, Field> {
        self.fields.entry(key)
    }

    pub fn find_field<S: Into<String>>(&self, name: S) -> Option<&Field> {
        let name = name.into();
        self.get_field(&name)
    }

    pub fn find_index(&self, name: &str) -> Option<usize> {
        self.fields.get(name).and_then(|field| Some(field.index))
    }

    pub fn find_by_index(&self, index: usize) -> Option<&Field> {
        self.fields.values().find(|f| f.index == index)
    }

    pub fn field_names(&self) -> Vec<&String> {
        let mut ordered_fields = self
            .fields
            .iter()
            .map(|(_, f)| (f.index, &f.name))
            .collect::<Vec<(usize, &String)>>();

        ordered_fields.sort_by(|a, b| a.0.cmp(&b.0));

        ordered_fields
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<&String>>()
    }

    pub fn field_exists(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<Field> {
        self.fields.remove(name)
    }

    pub fn rename_field(&mut self, old_name: &str, new_name: &str) -> Option<String> {
        self.fields.remove(old_name).and_then(|mut field| {
            let new_name = new_name.to_string();
            field.name = new_name.clone();
            self.fields.insert(new_name.clone(), field);
            Some(new_name)
        })
    }

    pub fn is_weak(&self) -> bool {
        self.fields
            .iter()
            .any(|(_, field)| field.dtype == DataType::Any)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn clear(&mut self) {
        self.fields.clear();
        self.name = None;
        self.doc = None;
    }
}

impl From<&[&str]> for Schema {
    fn from(columns: &[&str]) -> Schema {
        let fields = columns
            .into_iter()
            .enumerate()
            .map(|(i, name)| Field::with_index(name.to_string(), i))
            .collect::<Vec<Field>>();
        Schema::with_fields(fields)
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn it_identifies_weak_vs_strong() {
        {
            let mut schema = Schema::new();
            schema.add_field("a");
            assert!(schema.is_weak());
        }
        {
            let mut schema = Schema::new();
            let mut field = Field::new("a");
            field.dtype = DataType::String;
            schema.push_field(field);
            assert!(!schema.is_weak());
        }
    }
}
