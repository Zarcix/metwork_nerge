mod csv_interface;
mod layout_branch;
mod datamod;

use csv_interface::*;
use layout_branch::*;
use polars::series::Series;

fn main() {
    init_logger();
    process_data();
}

fn init_logger() {
    env_logger::init();
}

fn process_data() {
    let csv_reader = CsvConverter::default();
    let mut base_dataframe = csv_reader.process();
    // Old Layout
    base_dataframe = base_dataframe.drop_many(&["column_4", "column_7", "column_9", "column_10", "column_11"]);
    let header = base_dataframe.slice(0, 8);
    let mut body = base_dataframe.slice(7, usize::MAX);
    route_layout(&base_dataframe);

    let a = datamod::DataMod::default();
    a.expose_fn(body);
}