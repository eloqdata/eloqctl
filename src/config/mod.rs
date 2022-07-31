use crate::config::common::Common;
use std::path::Path;

pub mod common;

pub fn load_common_config(config_path: &str) -> anyhow::Result<Common> {
    let common_binary = std::fs::read(Path::new(config_path))
        .unwrap_or_else(|_| panic!("can't read data from {}", config_path));
    let common: Common = toml::from_slice(&common_binary)?;
    Ok(common)
}

#[cfg(test)]
mod tests {
    use crate::config::load_common_config;

    #[test]
    pub fn test_load_config() {
        let mut base_path = env!("CARGO_MANIFEST_DIR").to_owned();
        base_path.push_str("/config/common.toml");
        let common_rs = load_common_config(base_path.as_str());
        assert!(common_rs.is_ok());
        println!("{:?}", common_rs.unwrap());
    }
}
