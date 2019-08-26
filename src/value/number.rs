use crate::error::{self as error, ErrorKind};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::From;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Num {
    Uint8(u8),
    Int8(i8),

    Uint16(u16),
    Int16(i16),

    Uint32(u32),
    Int32(i32),

    Uint64(u64),
    Int64(i64),

    HalfFloat(f32),
    Float(f64),
    Double(Decimal),
}

impl From<u8> for Num {
    fn from(int: u8) -> Num {
        Num::Uint8(int)
    }
}

impl From<i8> for Num {
    fn from(int: i8) -> Num {
        Num::Int8(int)
    }
}

impl From<u16> for Num {
    fn from(int: u16) -> Num {
        Num::Uint16(int)
    }
}

impl From<i16> for Num {
    fn from(int: i16) -> Num {
        Num::Int16(int)
    }
}

impl From<u32> for Num {
    fn from(int: u32) -> Num {
        Num::Uint32(int)
    }
}

impl From<i32> for Num {
    fn from(int: i32) -> Num {
        Num::Int32(int)
    }
}

impl From<u64> for Num {
    fn from(int: u64) -> Num {
        Num::Uint64(int)
    }
}

impl From<i64> for Num {
    fn from(int: i64) -> Num {
        Num::Int64(int)
    }
}

impl From<f32> for Num {
    fn from(f: f32) -> Num {
        Num::HalfFloat(f)
    }
}

impl From<f64> for Num {
    fn from(f: f64) -> Num {
        Num::Float(f)
    }
}

impl std::fmt::Display for Num {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{}", self))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Number(pub(crate) Num);

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
