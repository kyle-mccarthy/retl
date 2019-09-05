use retl::source::csv::CsvSource;
use retl::{ops::convert::Convert, DataFrame, DataType};

// Wed May 21 00:00:00 EDT 2008

fn main() {
    let mut df =
        <DataFrame as CsvSource>::from_path("./sample-csv.csv").expect("failed to load csv");

    df.derive_schema();

    df.rename_column("sq__ft", "sq_ft");

    df.cast_column("beds", DataType::Uint32)
        .expect("failed to cast beds to u32");
    df.cast_column("baths", DataType::Uint32)
        .expect("failed to cast beds to u32");
    df.cast_column("sq_ft", DataType::Uint32)
        .expect("failed to cast sq_ft to u32");

    df.convert_column(
        "sale_date",
        Convert::ParseDateTime("%a %B %e %H:%M:%S EDT %Y"),
    )
    .expect("failed to convert sale_date to date");

    df.cast_column("price", DataType::Uint32)
        .expect("failed to cast price into u32");

    df.debug(10);
}
