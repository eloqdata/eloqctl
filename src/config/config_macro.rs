#[macro_export]
macro_rules! extract_config_value {
    ($config_obj_key:expr, $config_obj:ident, $input_config_path:expr) => {{
        use $crate::config::load_config;
        use $crate::config::ConfigObject;
        use $crate::config::MONOGRAPH_WATER_CONFIG_DIR;
        let mut input_path = match $input_config_path {
            Some(val) => val,
            _ => "".to_string(),
        };
        if input_path.is_empty() {
            input_path = std::env::var(MONOGRAPH_WATER_CONFIG_DIR).unwrap_or_else(|_| {
                panic!("Maybe it's a bug.The path to the configuration file must exist")
            });
        }
        let config_mapping = load_config(input_path.as_str());
        let config_obj = config_mapping.get($config_obj_key).unwrap();
        let rtn_value = match config_obj {
            ConfigObject::$config_obj(data) => data,
            _ => unreachable!(),
        };
        rtn_value
    }};
}

#[macro_export]
macro_rules! build_script {
    ($compile_obj:ident, $config_path:expr $(,$repo:ident)*) => {{
        use $crate::cmd::base::{CmdDef, PipeDef};
        let mut cmd_vec: Vec<CmdDef> = Vec::new();
        let common = extract_config_value!("common", Common, $config_path).clone();
        $(
           let build_script = common.compile.$compile_obj.$repo.build.unwrap_or_else(|| "echo 'no build script'".to_string());
           let cmd = CmdDef {
             name: "bash".to_string(),
             args: Some(vec!["-c".to_string(), format!(r"{}", build_script)]),
             show_progress_type: None,
             payload: None
           };
           cmd_vec.push(cmd);
        )*
        PipeDef { cmd_vec }
    }};
}

#[macro_export]
macro_rules! load_toml_config {
    ($config_path:expr, $config_type:ty) => {{
        use std::path::Path;
        let config_binary = std::fs::read(Path::new($config_path))
            .unwrap_or_else(|_| panic!("can't read common.toml from path = {}", $config_path));
        let obj: $config_type = toml::from_slice(&config_binary).unwrap();
        obj
    }};
}
