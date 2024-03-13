use std::borrow::BorrowMut;
use std::error::Error;
use configparser::ini::Ini;
use polars::{frame::DataFrame, series::Series};
use polars::prelude::*;
pub struct DataMod {
    ip_ranges: Vec<(String, String, String)>, // Vector<(ip start, ip end, campus)>
}

impl DataMod {
    pub fn process_data(&self, dataframe: DataFrame) -> DataFrame {
        let working_df = self.drop_duplicates(self.sort(dataframe));
        let indices = self.get_data_indices(&working_df);
        let new_df = self.drop_indices(&working_df, indices.clone());
        log::debug!("{:?}", new_df);
        new_df
    }

    fn sort(&self, dataframe: DataFrame) -> DataFrame {
        log::debug!("Sorting data by IP");
        return dataframe.sort(["column_5"], false, false).expect("Expect sorted dataframe");
    }

    fn drop_duplicates(&self, dataframe: DataFrame) -> DataFrame {
        log::debug!("Removing duplicates");
        return dataframe.unique_stable(Some(&["column_2".into()]), polars::frame::UniqueKeepStrategy::First, None).expect("Expected dataframe without duplicates");
    }

    fn get_data_indices(&self, dataframe: &DataFrame) -> Vec<(u32, u32, String)> {
        let mut data_indices = Vec::new();
        let dataframe = dataframe.column("column_5").unwrap();

        for (start_ip, end_ip, campus) in &self.ip_ranges {
            let mut ip_start_idx = 0;
            { // Get Start Index
                // TODO multithread this
                for data in dataframe.as_list().into_iter() {
                    ip_start_idx += 1;
                    if data.unwrap().get(0).unwrap().get_str().unwrap().contains(start_ip) {
                        break;
                    }
                }
                log::debug!("Got Starting Index: {:?}", ip_start_idx);
            }
            
            let mut ip_end_idx = ip_start_idx;
            { // Get Ending Index
                // TODO multithread this
                // Get start of ending index area after start index
                let mut ip_end_idx_start = ip_end_idx;
                for data in dataframe.as_list().into_iter().skip(ip_start_idx) {
                    if data.unwrap().get(0).unwrap().get_str().unwrap().contains(end_ip) {
                        break;
                    }
                    ip_end_idx_start += 1;
                }

                // Go to the end of the index to cover *.*.*.254
                ip_end_idx = ip_end_idx_start;
                for data in dataframe.as_list().into_iter().skip(ip_end_idx_start) {
                    if !data.unwrap().get(0).unwrap().get_str().unwrap().contains(end_ip) {
                        break;
                    }
                    ip_end_idx += 1;
                }

                log::debug!("Got Ending Index: {:?}", ip_end_idx);
            }

            data_indices.push((ip_start_idx as u32, ip_end_idx as u32, campus.clone()));
        }


        return data_indices;
    }

    fn update_campus(&self, campus_series: &mut Series, indices: (u32, u32, String)) {
        let start = indices.0;
        let end = indices.1;
        let campus = indices.2;
        
        let campus_list = (start .. end).map(|_| campus.clone()).collect::<Vec<String>>();
        let new_range = Series::new("Campus", campus_list);
        campus_series.append(&new_range).ok();
    }

    fn drop_indices(&self, dataframe: &DataFrame, indices: Vec<(u32, u32, String)>) -> DataFrame {
        let mut idx_vec = Vec::new();
        let mut campus_series = Series::new_empty("Campus", &DataType::String);
        for (start_idx, end_idx, campus) in indices {
            self.update_campus(&mut campus_series, (start_idx, end_idx, campus));
            idx_vec.append(&mut (start_idx .. end_idx).collect())
        }
        let kept_idxs = IdxCa::from_vec("keepIndex", idx_vec);
        let mut dropped_df = dataframe.take(&kept_idxs).unwrap();

        dropped_df.insert_column(0, campus_series).ok();
        return dropped_df;
    }
}

impl Default for DataMod {
    fn default() -> Self {
        let ip_range = parse_config().ok().expect("Expected valid ip range from config.txt");
        log::info!("Loaded ip range: {ip_range:?}");

        Self {
            ip_ranges: ip_range,
        }
    }
}

/// Loads a config list from config.txt. If there is no file, a new file will be created
fn parse_config() -> Result<Vec<(String, String, String)>, Box<dyn Error>> {
    let mut ip_vec: Vec<(String, String, String)> = Vec::new();
    let mut config = Ini::new();
    config.load_and_append("config.txt").ok();

    { // Add headers if they don't exist
        let expected_config_headers = vec!["STK", "SAC", "SF"];
        // Check if the cofnig file contains the headers
        if config.sections().iter().all(|section| expected_config_headers.contains(&section.as_str())) {
            log::debug!("Not all headers found. Headers found: {:?}. Adding...", config.sections());
            // Add in headers and write it back out to save
            for header in expected_config_headers {
                config.set(header, "Range", Some("[]".into()));
            }
            config.write("config.txt")?;
        }
    }

    { // Push all values into the main ip list
        for ip_range in serde_json::from_str::<Vec<(String, String)>>(&config.get("STK", "Range").unwrap())? {
            ip_vec.push((ip_range.0, ip_range.1, "Stockton".into()))
        }
    
        for ip_range in serde_json::from_str::<Vec<(String, String)>>(&config.get("SAC", "Range").unwrap())? {
            ip_vec.push((ip_range.0, ip_range.1, "Sacramento".into()))
        }
    
        for ip_range in serde_json::from_str::<Vec<(String, String)>>(&config.get("SF", "Range").unwrap())? {
            ip_vec.push((ip_range.0, ip_range.1, "San Francisco".into()))
        }
    }

    Ok(ip_vec)
}