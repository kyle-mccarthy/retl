use crate::{
    schema::{DataType, Schema},
    traits::TypeOf,
    DataFrame, Number, Value,
};
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The datatype of the value cannot be cast into the desired type. Cannot cast type {} into type {}.", source_type.as_str(), dest_type.as_str() ))]
    IllegalCast {
        source_type: DataType,
        dest_type: DataType,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

macro_rules! castable {
    ($to:ident, $from:ident, [$($x:path; [$($y:path),*]);+]) => {{

        // return match ($from, $to) {
        //     $($(($x, $y))|*)* => true,
        //     _ => false,
        // }

        // match ($from, $to) => {
        //     castable!($opts) => true,
        //     _ => false
        // }
        //
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
            DataType::Double; [DataType::Uint8, DataType::Uint16, DataType::Uint32, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Float]

        ]
    )
}

pub fn can_try_cast(to: &DataType, from: &DataType) -> bool {
    castable!(
        to,
        from,
        [
            DataType::Uint64; [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64];
            DataType::Uint32; [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint64];
            DataType::Uint16; [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint32, DataType::Uint64];
            DataType::Uint8; [DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Uint16, DataType::Uint32, DataType::Uint64];

            DataType::Int64; [DataType::Uint64];
            DataType::Int32; [DataType::Uint64, DataType::Uint32, DataType::Int64];
            DataType::Int16; [DataType::Uint64, DataType::Uint32, DataType::Uint16, DataType::Int64, DataType::Int32];
            DataType::Int8; [DataType::Uint64, DataType::Uint32, DataType::Uint16, DataType::Uint8, DataType::Int64, DataType::Int32, DataType::Int16]
        ]
    )
}

/// Try to cast the value into some DataType or return error
pub fn try_cast(value: Value, dtype: DataType) -> Result<Value> {
    if can_cast(&dtype, &value.type_of()) {}

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

#[cfg(test)]
mod test_casting {
    use super::*;

    #[test]
    fn it_can_cast() {}
}
