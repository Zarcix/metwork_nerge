use std::error::Error;
use configparser::ini::Ini;
use polars::{frame::DataFrame, series::Series};
pub struct DataMod {
    ip_ranges: Vec<(String, String, String)>, // Vector<(ip start, ip end, campus)>
}

impl DataMod {
    pub fn expose_fn(&self, dataframe: DataFrame) {
        println!("{}", &self.drop_duplicates(self.sort(dataframe)));
    }

    fn sort(&self, dataframe: DataFrame) -> DataFrame {
        log::debug!("Sorting data by IP");
        return dataframe.sort(["column_5"], false, false).expect("Expect sorted dataframe");
    }

    fn drop_duplicates(&self, dataframe: DataFrame) -> DataFrame {
        log::debug!("Removing duplicates");
        return dataframe.unique_stable(Some(&["column_2".into()]), polars::frame::UniqueKeepStrategy::First, None).expect("Expected dataframe without duplicates");
    }

    fn get_data_indices(&self, dataframe: DataFrame) -> Vec<(String, String, String)> {
        let data_indices = Vec::new();

        for ip_range in &self.ip_ranges {

        }

        return data_indices;
    }

    fn parse_data(&self, dataframe: Series, start_ip: String, end_ip: String) {
        let starting_index = 0;
        for data in dataframe.iter() {
            println!("{data}")
        }
    }
}

impl Default for DataMod {
    fn default() -> Self {
        let ip_range = parse_config().ok().expect("Expected valid ip range from config.txt");
        log::debug!("Loaded ip range: {ip_range:?}");

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