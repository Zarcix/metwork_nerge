use crate::datamod::*;

use polars::frame::DataFrame;

pub fn route_layout(input_dataframe: &DataFrame) {
    if input_dataframe.get_columns().len() == 11 {
        // Old Layout
        return;
    }
    // New Layout
}