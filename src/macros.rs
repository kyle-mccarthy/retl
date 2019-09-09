#[macro_export]
macro_rules! val {
    ($x:expr) => {{
        $crate::Value::from($x)
    }};
}

#[macro_export]
macro_rules! row {
    ($($x:expr),* $(,)?) => (vec![$($crate::val!($x)),*])
}
