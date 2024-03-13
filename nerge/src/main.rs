mod csv_interface;
mod layout_branch;
mod datamod;

use csv_interface::*;
use layout_branch::*;

use std::fs::File;

fn main() {
    init_logger();
    process_data();
}

fn init_logger() {
    env_logger::init();
}

fn process_data() {
    let csv_reader = CsvConverter::default();

    let base_dataframe = csv_reader.process();
    let mut processed_dataframe = route_layout(&base_dataframe);
    
    let csv_writer = CsvWriter::default();
    csv_writer.write_df(&mut processed_dataframe);
}