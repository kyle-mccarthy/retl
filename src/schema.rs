use crate::Value;

use serde::{Deserialize, Serialize};

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

    /// Get the default value for the data type or return null
    pub fn default_value(&self) -> Value {
        Value::Null
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
}

impl Field {
    pub fn new<S: Into<String>>(name: S) -> Field {
        Field {
            name: name.into(),
            nullable: true,
            default: None,
            doc: None,
            dtype: DataType::Any,
        }
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Schema {
    pub(crate) name: Option<String>,
    pub(crate) fields: Vec<Field>,
    pub(crate) doc: Option<String>,
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            name: None,
            fields: vec![],
            doc: None,
        }
    }
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn add_field<S: Into<String>>(&mut self, name: S) {
        self.fields.push(Field::new(name))
    }

    pub fn push_field(&mut self, field: Field) {
        self.fields.push(field)
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut Field> {
        self.fields.iter_mut().find(|field| field.name == name)
    }

    pub fn find_field<S: Into<String>>(&self, name: S) -> Option<&Field> {
        let name = name.into();
        self.get_field(&name)
    }

    pub fn is_weak(&self) -> bool {
        self.fields.iter().any(|field| field.dtype == DataType::Any)
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
