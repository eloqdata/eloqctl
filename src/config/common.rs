use once_cell::sync::Lazy;
use serde_derive::Deserialize;

pub static MONOGRAPH_GIT_REPOS: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "log_service".to_string(),
        "tx_service".to_string(),
        "monograph".to_string(),
        "cass".to_string(),
        "mariadb".to_string(),
    ]
});

#[macro_export]
macro_rules! git_clone {
    ($git_obj:expr $(,$git_attr:ident)*) => {{
        use $crate::cmd::base::CmdDef;
        use $crate::config::workspace_sub_dir;
        use $crate::config::common::MONOGRAPH_GIT_REPOS;
        let mut cmd_desc_vec: Vec<CmdDef> = vec![];
        let workspace_sub_dirs = workspace_sub_dir();
        $(
           let mut cmd_desc = CmdDef::default();
           cmd_desc.name = "git".to_string();
           let mut git_clone_args = vec!["clone".to_string()];
           if let Some(git_option) = $git_obj.$git_attr.options {
               git_clone_args.extend(git_option);
           }
           let git_repo = std::stringify!($git_attr).to_string();

           let dest_dir = if MONOGRAPH_GIT_REPOS.contains(&git_repo) {
               workspace_sub_dirs.get("source").unwrap()
           } else {
               workspace_sub_dirs.get("third_party").unwrap()
           };
           let dest_name = format!("{}/{}", dest_dir, std::stringify!($git_attr).to_string());
           if let Some(branch_name) = $git_obj.$git_attr.branch {
              git_clone_args.extend(vec!["-b".to_string(), branch_name, $git_obj.$git_attr.git, dest_name]);
           } else {
              git_clone_args.extend(vec![$git_obj.$git_attr.git, dest_name]);
           }
           cmd_desc.args = Some(git_clone_args);
           cmd_desc_vec.push(cmd_desc);
        )*
        cmd_desc_vec
    }};
}
#[derive(Clone, Debug, Deserialize)]
pub struct Common {
    pub workspace: String,
    pub compile: Compile,
    pub monograph: Monograph,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Compile {
    pub download: Download,
    pub git: Git,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Monograph {
    pub storage: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Git {
    pub brpc: GitArgs,
    pub braft: GitArgs,
    pub catch2: GitArgs,
    pub aws: GitArgs,
    pub log_service: GitArgs,
    pub tx_service: GitArgs,
    pub monograph: GitArgs,
    pub cass: GitArgs,
    pub mariadb: GitArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GitArgs {
    pub git: String,
    pub branch: Option<String>,
    pub build: Option<String>,
    pub options: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Download {
    pub protobuf: DownloadArgs,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DownloadArgs {
    pub url: String,
    pub build: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Cassandra {
    pub download: CassandraDownload,
    pub command: CassandraCommand,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CassandraDownload {
    pub url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CassandraCommand {
    pub start_script: String,
}

#[cfg(test)]
mod tests {
    use crate::config::common::{Git, GitArgs};
    use crate::config::MONOGRAPH_WATER_CONFIG_DIR;
    use crate::extract_config_value;
    use std::env;

    #[test]
    pub fn test_git_clone_macro() {
        let test_git_attr = GitArgs {
            git: "https://github.com/apache/incubator-brpc.git".to_string(),
            branch: Some("v2.x".to_string()),
            build: None,
            options: None,
        };
        let git = Git {
            brpc: test_git_attr.clone(),
            braft: test_git_attr.clone(),
            catch2: test_git_attr.clone(),
            aws: test_git_attr.clone(),
            log_service: test_git_attr.clone(),
            tx_service: test_git_attr.clone(),
            monograph: test_git_attr.clone(),
            cass: test_git_attr.clone(),
            mariadb: test_git_attr,
        };
        let git_string = stringify!(Git);
        let root = env!("CARGO_MANIFEST_DIR");
        let config_path = format!("{}/{}", root, "config");
        env::set_var(MONOGRAPH_WATER_CONFIG_DIR, config_path);
        let common = extract_config_value!("common", Common, None);
        env::set_var(MONOGRAPH_WATER_CONFIG_DIR, common.clone().workspace);
        println!("git_string {}", git_string.to_string().to_lowercase());
        let git_cmd = git_clone!(git, brpc, braft);
        println!("Cmd {:?}", git_cmd);
        assert_eq!(2, git_cmd.len())
    }
}
