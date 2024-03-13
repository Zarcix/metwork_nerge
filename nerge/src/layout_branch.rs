use crate::datamod::*;

use polars::{frame::DataFrame, series::Series};

pub fn route_layout(input_dataframe: &DataFrame) -> DataFrame {
    let data_processor = DataMod::default();
    let mut initial_dataframe = input_dataframe.clone();
    if input_dataframe.get_columns().len() == 11 {
        old_layout_drop(&mut initial_dataframe);
    }
    let (mut header, body) = get_parts(&initial_dataframe);
    let mut processed_dataframe = data_processor.process_data(body);

    return processed_dataframe
}

fn old_layout_drop(old_dataframe: &mut DataFrame) -> DataFrame {
    old_dataframe.drop_many(&["column_4", "column_7", "column_9", "column_10", "column_11"])
}

fn get_parts(old_dataframe: &DataFrame) -> (DataFrame, DataFrame) {
    let header = old_dataframe.slice(0, 8);
    let body = old_dataframe.slice(7, usize::MAX);
    (header, body)
}