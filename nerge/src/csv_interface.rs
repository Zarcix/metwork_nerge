use std::path::PathBuf;
use std::fs;

use polars::prelude::*;

pub struct CsvConverter {
    input_path: &'static str,
}

impl CsvConverter {
    pub fn process(&self) -> DataFrame {
        let files = self.get_files();
        let dataframes = self.csv_to_dataframe(&files).unwrap();
        let combined_dataframe = self.combine_csv(dataframes);
        
        combined_dataframe
    }

    fn get_files(&self) -> Vec<PathBuf> {
        let mut path_vec = Vec::new();
        fs::create_dir_all(self.input_path).expect("Input folder expected");
        let csv_files = fs::read_dir(self.input_path).expect("Expected readable directory");

        for file in csv_files {
            path_vec.push(file.unwrap().path());
        }

        return path_vec;
    }

    fn csv_to_dataframe(&self, paths: &Vec<PathBuf>) -> Result<Vec<DataFrame>, PolarsError> {
        let mut dataframe_vec = Vec::new();
        for file in paths {
            let dataframe = CsvReader::from_path(file)?
                .infer_schema(Some(123000))
                .has_header(false)
                .with_separator(b',')
                .finish()?;
            dataframe_vec.push(dataframe);
        }

        Ok(dataframe_vec)
    }

    fn combine_csv(&self, csv_list: Vec<DataFrame>) -> DataFrame {
        // TODO
        return csv_list.first().unwrap_or(&DataFrame::default()).clone();
    }
}

impl Default for CsvConverter {
    fn default() -> Self {
        CsvConverter {
            input_path: "./input"
        }
    }
}

pub struct CsvWriter {
    output_path: &'static str
}

impl CsvWriter {
    pub fn write_df(&self, output_dataframe: &mut DataFrame) {
        let mut file = std::fs::File::create(format!("{}/output.xlsx", self.output_path)).expect("could not create file");

        polars::io::prelude::CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(b',')
            .finish(output_dataframe).ok();
    }
}

impl Default for CsvWriter {
    fn default() -> Self {
        CsvWriter {
            output_path: "./"
        }
    }
}