use crate::Value;

use serde::{Deserialize, Serialize};
use std::collections::hash_map::HashMap;
use std::ops::Index;

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

    pub fn is_any(&self) -> bool {
        self == &DataType::Any
    }

    pub fn is_null(&self) -> bool {
        self == &DataType::Null
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

// TODO fields with aliases

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

    pub fn with_type(name: &str, dt: DataType) -> Field {
        Field {
            name: name.to_string(),
            dtype: dt,
            nullable: true,
            default: None,
            doc: None,
        }
    }

    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}

impl From<String> for Field {
    fn from(s: String) -> Field {
        Field::new(s)
    }
}

impl From<&String> for Field {
    fn from(s: &String) -> Field {
        Field::new(s)
    }
}

impl From<&str> for Field {
    fn from(s: &str) -> Field {
        Field::new(s)
    }
}

impl From<(&str, DataType)> for Field {
    fn from(t: (&str, DataType)) -> Field {
        Field::with_type(t.0, t.1)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Schema {
    name: Option<String>,
    doc: Option<String>,
    fields: Vec<Field>,
    index: HashMap<String, usize>,
}

// TODO evaluate possibility of aliases for the fields
// TODO evaluate a way to index the fields by order too - as of right now getting a field by order
// requires iterating over all the values. It could be better to store the fields in a vec and have
// a map that points the name/string index position
impl Default for Schema {
    fn default() -> Self {
        Schema {
            name: None,
            fields: Vec::new(),
            index: HashMap::new(),
            doc: None,
        }
    }
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn with_size(size: usize) -> Schema {
        Schema {
            name: None,
            doc: None,
            fields: Vec::with_capacity(size),
            index: HashMap::with_capacity(size),
        }
    }

    pub fn with_fields(fields: Vec<Field>) -> Schema {
        let mut schema = Schema::with_size(fields.len());

        for field in fields {
            schema.index.insert(field.name.clone(), schema.fields.len());
            schema.fields.push(field);
        }

        schema
    }

    pub fn add_field<F: Into<Field> + Clone>(&mut self, field: F) -> usize {
        self.push_field(field.into())
    }

    pub fn push_field(&mut self, field: Field) -> usize {
        let index = self.fields.len();
        self.index.insert(field.name.clone(), index);
        self.fields.push(field);

        index
    }

    pub fn get_field_full(&self, name: &str) -> Option<(&usize, &Field)> {
        self.index.get(name).and_then(|index| {
            if let Some(field) = self.fields.get(*index) {
                return Some((index, field));
            }
            None
        })
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.index
            .get(name)
            .and_then(|index| self.fields.get(*index))
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.index.contains_key(name)
    }

    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut Field> {
        let index = *self.index.get(name)?;
        self.fields.get_mut(index)
    }

    pub fn find_field<S: Into<String>>(&self, name: S) -> Option<&Field> {
        let name = name.into();
        self.get_field(&name)
    }

    pub fn find_index(&self, name: &str) -> Option<&usize> {
        self.index.get(name)
    }

    pub fn find_by_index(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    pub fn field_names(&self) -> Vec<&String> {
        self.fields
            .iter()
            .map(|f| &f.name)
            .collect::<Vec<&String>>()
    }

    pub fn field_exists(&self, name: &str) -> bool {
        self.index.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<Field> {
        self.index
            .remove(name)
            .and_then(|index| Some(self.fields.remove(index)))
    }

    pub fn rename_field(&mut self, old_name: &str, new_name: &str) -> Option<&String> {
        let index = *self.index.get(old_name)?;

        self.index.remove(old_name);
        self.index.insert(new_name.to_string(), index);

        let mut field = &mut self.fields[index];
        field.name = new_name.to_string();
        Some(&field.name)
    }

    pub fn is_weak(&self) -> bool {
        self.fields.iter().any(|field| field.dtype == DataType::Any)
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
            .iter()
            .map(|name| Field::new(name.to_string()))
            .collect::<Vec<Field>>();
        Schema::with_fields(fields)
    }
}

impl Index<usize> for Schema {
    type Output = Field;
    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn it_schema_from_macro() {
        use crate::schema;

        let s = schema!(("a", DataType::Int8));

        dbg!(s);
    }

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
