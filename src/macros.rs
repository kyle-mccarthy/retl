#[macro_export]
macro_rules! val {
    ($x:expr) => {{
        $crate::Value::from($x)
    }};
    ($x:expr, $dt:path) => {{
        $crate::ops::cast::safe_cast($crate::val!($x), &$dt)
    }};
}

#[macro_export]
macro_rules! row {
    // ([$($x:expr),* $(,)?]) => {$crate::row!(..$x)};
    ($($x:expr),* $(,)?) => (vec![$($crate::val!($x)),*])
}

#[macro_export]
macro_rules! schema {
    ($($x:expr),* $(,)?) => ($crate::schema::Schema::with_fields(
            vec![
                $($crate::field!($x)),*
            ]
        )
    )
}

#[macro_export]
macro_rules! field {
    ($e:expr) => {
        Into::<$crate::schema::Field>::into($e)
    };
}

#[macro_export]
macro_rules! df {
    ([$($f:expr),* $(,)?], [$($x:expr),*]) => {
        $crate::DataFrame::with_data(crate::schema::Schema::with_fields(vec![
             $($crate::field!($f)),*
        ]), vec![
            $($x),*
        ])
    };
}
