mod csv_interface;
mod layout_branch;
mod datamod;

fn main() {
    let csv_reader = csv_interface::CsvConverter::default();
    csv_reader.process();
}
