use std::error::Error;

use configparser::ini::Ini;
pub struct DataMod {
    ip_ranges: Vec<(String, String, String)>, // Vector<(ip start, ip end, campus)>
}

impl Default for DataMod {
    fn default() -> Self {
        let mut ip_range_vec = Vec::new();

        parse_config(&mut ip_range_vec).ok();

        log::debug!("Loaded ip range: {ip_range_vec:?}");

        Self {
            ip_ranges: ip_range_vec,
        }
    }
}

/// Loads a config list from config.txt. If there is no file, a new file will be created
fn parse_config(ip_vec: &mut Vec<(String, String, String)>) -> Result<(), Box<dyn Error>> {
    let mut config = Ini::new();
    config.load_and_append("config.txt").ok();

    { // Add headers if they don't exist
        let expected_config_headers = vec!["STK", "SAC", "SF"];
        if config.sections().iter().all(|section| expected_config_headers.contains(&section.as_str())) {
            log::debug!("Not all headers found. Headers found: {:?}. Adding...", config.sections());
            for header in expected_config_headers {
                config.set(header, "Range", Some("[]".into()));
            }
        }
        config.write("config.txt")?;
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
    

    Ok(())
}