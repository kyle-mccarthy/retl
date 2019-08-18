use crate::error::{self as error, ResultExt};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::{From, TryInto};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Integer {
    Negative(i64),
    Positive(u64),
}

impl PartialEq for Integer {
    fn eq(&self, other: &Integer) -> bool {
        match (&self, &other) {
            (Integer::Positive(a), Integer::Positive(b)) => a == b,
            (Integer::Negative(a), Integer::Negative(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Integer {
    fn partial_cmp(&self, rhs: &Integer) -> Option<std::cmp::Ordering> {
        Some(match (&self, &rhs) {
            (Integer::Positive(a), Integer::Positive(b)) => a.cmp(b),
            (Integer::Negative(a), Integer::Negative(b)) => a.cmp(b),
            (Integer::Positive(_), _) => Ordering::Greater,
            (Integer::Negative(_), _) => Ordering::Less,
        })
    }
}

impl std::fmt::Display for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self {
            Integer::Positive(n) => write!(f, "{}", n),
            Integer::Negative(n) => write!(f, "{}", n),
        }
    }
}

macro_rules! integer_from_unsigned {
    (
        $($ty:ty), *
    ) => {
        $(
            impl From<$ty> for Integer {
                fn from(i: $ty) -> Self {
                    Integer::Positive(i as u64)
                }
            }

            // impl<'a> From<&'a $ty> for Integer {
            //     fn from(i: &'a $ty) -> Self {
            //         Integer::Positive(i.clone() as u64)
            //     }
            // }

        )*
    }
}

macro_rules! integer_from_signed {
    (
        $($ty:ty), *
     ) => {
        $(
            impl From<$ty> for Integer {
                fn from(i: $ty) -> Self {
                    if i > 0 {
                        Integer::Positive(i as u64)
                    } else {
                        Integer::Negative(i as i64)
                    }
                }
            }

         // impl<'a> From<&'a $ty> for Integer {
         //        fn from(i: &'a $ty) -> Self {
         //            if *i > 0 {
         //                Integer::Positive(i.clone() as u64)
         //            } else {
         //                Integer::Negative(i.clone() as i64)
         //            }
         //        }
         //    }

        )*
    }
}

integer_from_unsigned!(u8, u16, u32, u64, usize);
integer_from_signed!(i8, i16, i32, i64, isize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Number {
    Integer(Integer),
    Decimal(f64),
}

impl PartialEq for Number {
    fn eq(&self, other: &Number) -> bool {
        match (&self, &other) {
            (Number::Integer(a), Number::Integer(b)) => a == b,
            (Number::Decimal(a), Number::Decimal(b)) => a == b,
            _ => false, // can't really compare floats and ints -- should never actually happen though since each column should have same data type
        }
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Number) -> Option<Ordering> {
        match (&self, &other) {
            (Number::Integer(a), Number::Integer(b)) => a.partial_cmp(&b),
            (Number::Decimal(a), Number::Decimal(b)) => a.partial_cmp(&b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Number::Integer(i) => write!(f, "{}", i),
            Number::Decimal(d) => write!(f, "{}", d),
        }
    }
}
impl<T: Into<Integer>> From<T> for Number {
    fn from(i: T) -> Self {
        Number::Integer(i.into())
    }
}

// impl<'a, T: Into<Integer>> From<&'a T> for &'a Number
// where
//     Integer: std::convert::From<&'a T>,
// {
//     fn from(i: &'a T) -> Self {
//         Number::Integer(i.into())
//     }
// }

// impl<'a, T: Into<Number>> From<&T> for Number {
//     fn from(i: &T) -> Self {
//         i.into()
//     }
// }

// impl<T> From<T> for Number
// where
//     Number: From<T>,
// {
//     fn from(i: T) -> Self {
//         i.into()
//     }
// }
