use crate::{schema::DataType, traits::TypeOf};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use snafu::{IntoError, Snafu};
use std::convert::{From, Into, TryInto};
use std::error::Error as ErrorTrait;
use std::str::FromStr;

use std::ops::Add as AddTrait;

#[derive(Debug, Snafu)]
pub enum Error {
    Infallible,

    CastError {
        description: String,
    },

    #[snafu(display("Cannot convert this datatype into desired type"))]
    IllegalConversion,

    #[snafu(display("Cannot perform this operation on numbers of two different types"))]
    IllegalOperation,

    #[snafu(display("Failed to perform the operation, result may have under or overflowed"))]
    OpFailed,

    #[snafu(display("Failed to parse the string to a numeric type"))]
    ParseStringError {
        string_source: String,
        destination_type: DataType,
    },

    #[snafu(display("Failed to parse string to integer value"))]
    ParseIntError {
        source: std::num::ParseIntError,
        from_str: String,
    },

    #[snafu(display("Failed to parse string to floating point value"))]
    ParseFloatError {
        source: std::num::ParseFloatError,
        from_str: String,
    },

    #[snafu(display("Failed to parse string to decimal value"))]
    ParseDecimalError {
        from_str: String,
        description: String,
    },

    #[snafu(display("Attempted to use non numeric data type as numeric"))]
    InvalidDataType {
        datatype: DataType,
    },
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

macro_rules! impl_is_type {
    ($func:ident, $func2:ident, $is:path) => {
        impl_is_type!($func, $is);

        /// Is an alias for self.$func()
        pub fn $func2(&self) -> bool {
            self.$func()
        }
    };
    ($func:ident, $is:path) => {
        pub fn $func(&self) -> bool {
            match self.0 {
                $is(_) => true,
                _ => false
            }
        }
    }
}

macro_rules! impl_as_primative {
    ($func:ident, $type:ty) => {
        pub fn $func(&self) -> $type {
            match self.0 {
                Num::Uint8(n) => n as $type,
                Num::Uint16(n) => n as $type,
                Num::Uint32(n) => n as $type,
                Num::Uint64(n) => n as $type,
                Num::Int8(n) => n as $type,
                Num::Int16(n) => n as $type,
                Num::Int32(n) => n as $type,
                Num::Int64(n) => n as $type,
                Num::Float(n) => n as $type,
                Num::Double(n) => n as $type,
                Num::Decimal(_) => 0 as $type,
            }
        }
    }
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

macro_rules! impl_primative_partial_eq {
    ($prim:ty, $num:path) => {
        impl PartialEq<$prim> for Num {
            fn eq(&self, lhs: &$prim) -> bool {
                match self {
                    $num(n) => lhs == n,
                    _ => false,
                }
            }
        }

        impl PartialEq<$prim> for &Num {
            fn eq(&self, lhs: &$prim) -> bool {
                match self {
                    $num(n) => lhs == n,
                    _ => false,
                }
            }
        }
    };
}

macro_rules! impl_traits {
    ($prim:ty, $num:path) => {
        impl_from_primative!($prim, $num);
        impl_primative_partial_eq!($prim, $num);
    };
}

impl TypeOf for Num {
    fn type_of(&self) -> &DataType {
        match self {
            Num::Uint8(_) => &DataType::Uint8,
            Num::Uint16(_) => &DataType::Uint16,
            Num::Uint32(_) => &DataType::Uint32,
            Num::Uint64(_) => &DataType::Uint64,
            Num::Int8(_) => &DataType::Int8,
            Num::Int16(_) => &DataType::Int16,
            Num::Int32(_) => &DataType::Int32,
            Num::Int64(_) => &DataType::Int64,
            Num::Float(_) => &DataType::Float,
            Num::Double(_) => &DataType::Double,
            Num::Decimal(_) => &DataType::Decimal,
        }
    }
}

impl Num {
    pub fn to_string(&self) -> String {
        match self {
            Num::Uint8(n) => n.to_string(),
            Num::Uint16(n) => n.to_string(),
            Num::Uint32(n) => n.to_string(),
            Num::Uint64(n) => n.to_string(),
            Num::Int8(n) => n.to_string(),
            Num::Int16(n) => n.to_string(),
            Num::Int32(n) => n.to_string(),
            Num::Int64(n) => n.to_string(),
            Num::Float(n) => n.to_string(),
            Num::Double(n) => n.to_string(),
            Num::Decimal(n) => n.to_string(),
        }
    }
}

impl_traits!(u8, Num::Uint8);
impl_traits!(u16, Num::Uint16);
impl_traits!(u32, Num::Uint32);
impl_traits!(u64, Num::Uint64);

impl_traits!(i8, Num::Int8);
impl_traits!(i16, Num::Int16);
impl_traits!(i32, Num::Int32);
impl_traits!(i64, Num::Int64);

impl_traits!(f32, Num::Float);
impl_traits!(f64, Num::Double);

macro_rules! try_from_str {
    ($prim:ty, $num:path, $var:ident, $err_type:ident) => {{
        match <$prim>::from_str($var) {
            Ok(int) => Ok(Number($num(int))),
            Err(source) => Err($err_type {
                from_str: $var.to_string(),
            }
            .into_error(source)),
        }
    }};
}

impl std::fmt::Display for Num {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}", self))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Number(pub(crate) Num);

impl TypeOf for Number {
    fn type_of(&self) -> &DataType {
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
        f.write_str(&self.to_string())
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

    pub fn into_float(self) -> Result<Number, Error> {
        use rust_decimal::prelude::ToPrimitive;

        match self.0 {
            Num::Uint8(n) => Ok(f32::from(n)),
            Num::Uint16(n) => Ok(f32::from(n)),
            Num::Int8(n) => Ok(f32::from(n)),
            Num::Int16(n) => Ok(f32::from(n)),
            Num::Float(n) => Ok(n),

            Num::Decimal(n) => n.to_f32().ok_or(Error::CastError {
                description: "Failed to cast f32 into decimal datatype".to_string(),
            }),

            _ => Err(Error::IllegalConversion),
        }
        .map(|n| Number(Num::Float(n)))
    }

    pub fn into_double(self) -> Result<Number, Error> {
        use rust_decimal::prelude::ToPrimitive;

        match self.0 {
            Num::Uint8(n) => Ok(f64::from(n)),
            Num::Uint16(n) => Ok(f64::from(n)),
            Num::Int8(n) => Ok(f64::from(n)),
            Num::Int16(n) => Ok(f64::from(n)),
            Num::Float(n) => Ok(f64::from(n)),
            Num::Double(n) => Ok(n),
            Num::Uint32(n) => Ok(f64::from(n)),
            Num::Int32(n) => Ok(f64::from(n)),
            Num::Decimal(n) => n.to_f64().ok_or(Error::CastError {
                description: "Failed to cast f32 into decimal datatype".to_string(),
            }),

            _ => Err(Error::IllegalConversion),
        }
        .map(|n| Number(Num::Double(n)))
    }

    pub fn into_decimal(self) -> Result<Number, Error> {
        use rust_decimal::prelude::FromPrimitive;

        match self.0 {
            Num::Uint8(n) => Ok(Decimal::from(n)),
            Num::Uint16(n) => Ok(Decimal::from(n)),
            Num::Uint32(n) => Ok(Decimal::from(n)),
            Num::Uint64(n) => Ok(Decimal::from(n)),
            Num::Int8(n) => Ok(Decimal::from(n)),
            Num::Int16(n) => Ok(Decimal::from(n)),
            Num::Int32(n) => Ok(Decimal::from(n)),
            Num::Int64(n) => Ok(Decimal::from(n)),
            Num::Float(n) => Decimal::from_f32(n).ok_or(Error::CastError {
                description: "Failed to convert f32 into decimal".to_string(),
            }),
            Num::Double(n) => Decimal::from_f64(n).ok_or(Error::CastError {
                description: "Failed to convert f64 into decimal".to_string(),
            }),
            Num::Decimal(n) => Ok(n),
        }
        .map(|n| Number(Num::Decimal(n)))
    }

    impl_is_type!(is_u8, is_uint8, Num::Uint8);
    impl_is_type!(is_u16, is_uint16, Num::Uint16);
    impl_is_type!(is_u32, is_uint32, Num::Uint32);
    impl_is_type!(is_u64, is_uint64, Num::Uint64);

    impl_is_type!(is_i8, is_int8, Num::Int8);
    impl_is_type!(is_i16, is_int16, Num::Int16);
    impl_is_type!(is_i32, is_int32, Num::Int32);
    impl_is_type!(is_i64, is_int64, Num::Int64);

    impl_is_type!(is_f32, is_float, Num::Float);
    impl_is_type!(is_f64, is_double, Num::Double);

    impl_is_type!(is_decimal, Num::Decimal);

    impl_as_primative!(as_u8, u8);
    impl_as_primative!(as_u16, u16);
    impl_as_primative!(as_u32, u32);
    impl_as_primative!(as_u64, u64);

    impl_as_primative!(as_i8, i8);
    impl_as_primative!(as_i16, i16);
    impl_as_primative!(as_i32, i32);
    impl_as_primative!(as_i64, i64);

    impl_as_primative!(as_f32, f32);
    impl_as_primative!(as_f64, f64);

    pub fn checked_add(self, lhs: Number) -> Result<Number, Error> {
        impl_op!(add, checked_add)(self, lhs)
    }

    pub fn to_string(&self) -> String {
        self.0.to_string()
    }

    pub fn inner(&self) -> &Num {
        &self.0
    }

    pub fn from_str(s: &str, dtype: &DataType) -> Result<Number, Error> {
        match dtype {
            DataType::Uint8 => try_from_str!(u8, Num::Uint8, s, ParseIntError),
            DataType::Uint16 => try_from_str!(u16, Num::Uint16, s, ParseIntError),
            DataType::Uint32 => try_from_str!(u32, Num::Uint32, s, ParseIntError),
            DataType::Uint64 => try_from_str!(u64, Num::Uint64, s, ParseIntError),

            DataType::Int8 => try_from_str!(i8, Num::Int8, s, ParseIntError),
            DataType::Int16 => try_from_str!(i16, Num::Int16, s, ParseIntError),
            DataType::Int32 => try_from_str!(i32, Num::Int32, s, ParseIntError),
            DataType::Int64 => try_from_str!(i64, Num::Int64, s, ParseIntError),

            DataType::Float => try_from_str!(f32, Num::Float, s, ParseFloatError),
            DataType::Double => try_from_str!(f64, Num::Double, s, ParseFloatError),
            DataType::Decimal => Decimal::from_str(s)
                .map_err(|e| Error::ParseDecimalError {
                    from_str: s.into(),
                    description: e.description().into(),
                })
                .map(|d| Number(Num::Decimal(d))),
            _ => Err(Error::InvalidDataType {
                datatype: dtype.clone(),
            }),
        }
    }
}

macro_rules! impl_primative_partial_eq_number {
    ($prim:ty, $num:path) => {
        impl PartialEq<$prim> for Number {
            fn eq(&self, lhs: &$prim) -> bool {
                self.inner().eq(lhs)
            }
        }

        impl PartialEq<$prim> for &Number {
            fn eq(&self, lhs: &$prim) -> bool {
                self.inner().eq(lhs)
            }
        }
    };
}

impl_primative_partial_eq_number!(u8, Num::Uint8);
impl_primative_partial_eq_number!(u16, Num::Uint16);
impl_primative_partial_eq_number!(u32, Num::Uint32);
impl_primative_partial_eq_number!(u64, Num::Uint64);
impl_primative_partial_eq_number!(i8, Num::Int8);
impl_primative_partial_eq_number!(i16, Num::Int16);
impl_primative_partial_eq_number!(i32, Num::Int32);
impl_primative_partial_eq_number!(i64, Num::Int64);

#[cfg(test)]
mod number_test {
    use super::*;

    #[test]
    fn test_cast() {
        let from_num: Number = 16i64.into();
        let converted = from_num.into_uint8();
        assert!(converted.is_ok());
        assert_eq!(converted.unwrap(), 16u8);
    }
}
