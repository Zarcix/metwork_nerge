mod csv_interface;
mod layout_branch;
mod datamod;

use csv_interface::*;
use layout_branch::*;
use datamod::*;

fn main() {
    let csv_reader = CsvConverter::default();
    csv_reader.process();
}
