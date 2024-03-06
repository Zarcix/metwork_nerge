mod csv_processor;
mod layout_branch;
mod datamod;

fn main() {
    let csv_reader = csv_processor::CsvProcessor::default();
    csv_reader.process();
}
