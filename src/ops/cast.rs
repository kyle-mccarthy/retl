use crate::{
    schema::{DataType, Schema},
    traits::TypeOf,
    DataFrame, Number, Value,
};
use snafu::{IntoError, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The datatype of the value cannot be cast into the desired type. Cannot cast type {} into type {}.", source_type.as_str(), dest_type.as_str() ))]
    IllegalCast {
        source_type: DataType,
        dest_type: DataType,
    },

    FailedNumericCast {
        source: crate::value::number::Error,
    },

    #[snafu(display("Called convert into_number with non numeric destination type"))]
    InvalidNumericCast,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! castable {
    ($to:ident, $from:ident, [$($x:path; [$($y:path),*]);+]) => {{
        match ($to, $from) {
            $(
                $(
                    pcast!($x, $y) => true,
                )*
            )*
            _ => false
        }
    }};
}

macro_rules! pcast {
    ($x:path, $y:path) => {($x, $y)};
    ($x:path; [$(y:path),*]) => { $(pcast!(x, y))* };
    ($x:path; $y:tt) => { pcast!($x, $y) };
    ($([$x:path; $y:tt])*) => { $(pcast!($x; $y))*}
}

pub fn can_cast(to: &DataType, from: &DataType) -> bool {
    castable!(
        to,
        from,
        [
            DataType::Int64; [DataType::Bool, DataType::Uint8, DataType::Uint16, DataType::Uint32, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Float, DataType::Decimal];
            DataType::Int32; [DataType::Bool, DataType::Uint8, DataType::Uint16, DataType::Int8, DataType::Int16];
            DataType::Int16; [DataType::Bool, DataType::Uint8, DataType::Int8];
            DataType::Int8; [DataType::Bool];

            DataType::Uint64; [DataType::Bool, DataType::Uint8, DataType::Uint16, DataType::Uint32];
            DataType::Uint32; [DataType::Bool, DataType::Uint8, DataType::Uint16];
            DataType::Uint16; [DataType::Bool, DataType::Uint8];
            DataType::Uint8; [DataType::Bool];

            DataType::Float; [DataType::Uint8, DataType::Uint16, DataType::Int8, DataType::Uint16];
            DataType::Double; [DataType::Uint8, DataType::Uint16, DataType::Uint32, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Float];

            DataType::String; [DataType::Bool, DataType::Uint8, DataType::Uint16, DataType::Uint32, DataType::Uint64, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64]

        ]
    )
}

pub fn can_try_cast(to: &DataType, from: &DataType) -> bool {
    castable!(
        to,
        from,
        [
            DataType::Uint64; [DataType::String, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64];
            DataType::Uint32; [DataType::String, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint64];
            DataType::Uint16; [DataType::String, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint32, DataType::Uint64];
            DataType::Uint8; [DataType::String, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint16, DataType::Uint32, DataType::Uint64];

            DataType::Int64; [DataType::String, DataType::Uint64];
            DataType::Int32; [DataType::String, DataType::Uint64, DataType::Uint32, DataType::Int64];
            DataType::Int16; [DataType::String, DataType::Uint64, DataType::Uint32, DataType::Uint16, DataType::Int64, DataType::Int32];
            DataType::Int8; [DataType::String, DataType::Uint64, DataType::Uint32, DataType::Uint16, DataType::Uint8, DataType::Int64, DataType::Int32, DataType::Int16]
        ]
    )
}

/// Try to cast the value into some DataType or return error
pub fn try_cast(value: Value, dtype: DataType) -> Result<Value> {
    if can_cast(&dtype, &value.type_of()) {
        if value.is_numeric() && dtype == DataType::String {
            return Ok(value.to_string().into());
        }
    }

    if can_try_cast(&dtype, &value.type_of()) {}
    Ok(value)
}

/// Wraps cast_or_default while using Null as the default value on failure
pub fn safe_cast(value: Value, dtype: DataType) -> Value {
    cast_or_default(value, dtype, Value::Null)
}

/// Attempt to cast the value into some DataType or return the default value
pub fn cast_or_default(value: Value, dtype: DataType, default: Value) -> Value {
    match try_cast(value, dtype) {
        Ok(value) => value,
        _ => default,
    }
}

pub fn into_number(value: Value, into_type: DataType) -> Result<Value> {
    if !into_type.is_numeric() {
        return Err(Error::InvalidNumericCast);
    }

    match value {
        Value::Number(num) => match match into_type {
            DataType::Uint8 => num.into_uint8(),
            DataType::Uint16 => num.into_uint16(),
            DataType::Uint32 => num.into_uint32(),
            DataType::Uint64 => num.into_uint64(),
            DataType::Int8 => num.into_int8(),
            DataType::Int16 => num.into_int16(),
            DataType::Int32 => num.into_int32(),
            DataType::Int64 => num.into_int64(),
            _ => panic!("into_type should be a number when calling into_number"),
        } {
            Ok(num) => Ok(Value::Number(num)),
            Err(err) => Err(FailedNumericCast.into_error(err)),
        },
        Value::String(s) => Number::from_string(&s, into_type)
            .map(Value::Number)
            .map_err(|e| FailedNumericCast.into_error(e)),
        // convert the bool to an int and then into the right data type
        Value::Bool(b) => into_number(
            Value::Number(Number::from(if b { 1u8 } else { 0u8 })),
            into_type,
        ),
        _ => Err(Error::IllegalCast {
            source_type: value.type_of(),
            dest_type: into_type,
        }),
    }
}

#[cfg(test)]
mod test_casting {
    // use super::*;

    #[test]
    fn it_can_cast() {}
}
