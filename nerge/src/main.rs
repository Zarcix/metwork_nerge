mod csv_interface;
mod layout_branch;
mod datamod;

use csv_interface::*;
use layout_branch::*;
use simple_logger::*;

fn main() {
    init_logger();
    process_data();
}

fn init_logger() {
    SimpleLogger::new().init().unwrap();
}

fn process_data() {
    let csv_reader = CsvConverter::default();
    let base_dataframe = csv_reader.process();
    route_layout(&base_dataframe);

    let a = datamod::DataMod::default();
}