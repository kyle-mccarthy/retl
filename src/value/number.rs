use crate::{schema::DataType, traits::TypeOf};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::convert::{From, Into, TryInto};
use std::error::Error as ErrorTrait;

use std::ops::Add as AddTrait;

#[derive(Debug, Snafu)]
pub enum Error {
    Infallible,
    CastError {
        description: String,
    },
    IllegalConversion,

    #[snafu(display("Cannot perform this operation on numbers of two different types"))]
    IllegalOperation,

    #[snafu(display("Failed to perform the operation, result may have under or overflowed"))]
    OpFailed,
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Error {
        Error::Infallible
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Error {
        Error::CastError {
            description: err.description().to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Num {
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    Float(f32),
    Double(f64),
    Decimal(Decimal),
}

// fn checked_add<T>(n: Num, lhs: T, rhs: T) -> Result<Num, Error>
// where
//     T: std::ops::Add<Output = T>,
// {
//     Ok(n(lhs.add(rhs) as T))
// }

macro_rules! impl_op {
    ($op:ident, $checked_op:ident) => {{
        |lhs: Number, rhs: Number| -> Result<Number, Error> {
            let lhs = lhs.0;
            let rhs = rhs.0;
            perform_op!($op, $checked_op, rhs, lhs).and_then(|num| Ok(Number(num)))
        }
    }};
}

macro_rules! perform_op {
    ($op: ident, $checked_op:ident, $lhs:ident, $rhs:ident) => {{
        match ($lhs, $rhs) {
            (Num::Uint8(l), Num::Uint8(r)) => perform_op!($checked_op, Num::Uint8, l, r),
            (Num::Uint16(l), Num::Uint16(r)) => perform_op!($checked_op, Num::Uint16, l, r),

            (Num::Uint32(l), Num::Uint32(r)) => perform_op!($checked_op, Num::Uint32, l, r),
            (Num::Uint64(l), Num::Uint64(r)) => perform_op!($checked_op, Num::Uint64, l, r),
            (Num::Int8(l), Num::Int8(r)) => perform_op!($checked_op, Num::Int8, l, r),
            (Num::Int16(l), Num::Int16(r)) => perform_op!($checked_op, Num::Int16, l, r),
            (Num::Int32(l), Num::Int32(r)) => perform_op!($checked_op, Num::Int32, l, r),
            (Num::Int64(l), Num::Int64(r)) => perform_op!($checked_op, Num::Int64, l, r),

            // floats don't have checked operations
            (Num::Float(l), Num::Float(r)) => Ok(Num::Float(l.$op(r))),
            // (Num::Double(l), Num::Double(r)) => Ok(Num::Double(l.$op(r))),

            // (Num::Decimal(l), Num::Decimal(r)) => Ok(Num::Decimal(l.$op(r))),
            _ => Err(Error::IllegalOperation),
        }
    }};
    ($op:ident, $num:path, $lhs:ident, $rhs:ident) => {{
        match $lhs.$op($rhs) {
            Some(value) => Ok($num(value + 1)),
            _ => Err(Error::OpFailed),
        }
    }};
}

macro_rules! cast_num {
    ($val:ident, $to:ty) => {{
        match match $val {
            Num::Uint8(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Uint16(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Uint32(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Uint64(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Int8(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Int16(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Int32(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            Num::Int64(int) => TryInto::<$to>::try_into(int).map_err(Into::<Error>::into),
            _ => Err(Error::IllegalConversion),
        } {
            Ok(num) => Ok(Number::from(num)),
            Err(e) => Err(e),
        }
    }};
}

macro_rules! impl_cast_num {
    ($func:ident, $into:ty) => {
        pub fn $func(self) -> Result<Number, Error> {
            let num = self.0;
            cast_num!(num, $into)
        }
    }
}

macro_rules! impl_from_primative {
    ($prim:ty, $num:path) => {
        impl From<$prim> for Num {
            fn from(val: $prim) -> Num {
                $num(val)
            }
        }
    };
}

impl TypeOf for Num {
    fn type_of(&self) -> DataType {
        match self {
            Num::Uint8(_) => DataType::Uint8,
            Num::Uint16(_) => DataType::Uint16,
            Num::Uint32(_) => DataType::Uint32,
            Num::Uint64(_) => DataType::Uint64,
            Num::Int8(_) => DataType::Int8,
            Num::Int16(_) => DataType::Int16,
            Num::Int32(_) => DataType::Int32,
            Num::Int64(_) => DataType::Int64,
            Num::Float(_) => DataType::Float,
            Num::Double(_) => DataType::Double,
            Num::Decimal(_) => DataType::Decimal,
        }
    }
}

impl_from_primative!(u8, Num::Uint8);
impl_from_primative!(u16, Num::Uint16);
impl_from_primative!(u32, Num::Uint32);
impl_from_primative!(u64, Num::Uint64);

impl_from_primative!(i8, Num::Int8);
impl_from_primative!(i16, Num::Int16);
impl_from_primative!(i32, Num::Int32);
impl_from_primative!(i64, Num::Int64);

impl_from_primative!(f32, Num::Float);
impl_from_primative!(f64, Num::Double);

impl std::fmt::Display for Num {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}", self))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Number(pub(crate) Num);

impl TypeOf for Number {
    fn type_of(&self) -> DataType {
        self.0.type_of()
    }
}

impl<N: Into<Num>> From<N> for Number {
    fn from(n: N) -> Number {
        Number(n.into())
    }
}

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}", self.0))
    }
}

impl Number {
    impl_cast_num!(into_uint8, u8);
    impl_cast_num!(into_uint16, u16);
    impl_cast_num!(into_uint32, u32);
    impl_cast_num!(into_uint64, u64);

    impl_cast_num!(into_int8, i8);
    impl_cast_num!(into_int16, i16);
    impl_cast_num!(into_int32, i32);
    impl_cast_num!(into_int64, i64);

    pub fn checked_add(self, lhs: Number) -> Result<Number, Error> {
        impl_op!(add, checked_add)(self, lhs)
    }
}

#[cfg(test)]
mod number_test {
    use super::*;

    #[test]
    fn test_cast() {
        let from_num: Number = 16i64.into();
        let converted = from_num.into_uint8();
    }
}
