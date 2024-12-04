use crate::cli::task::download_task::DownloadTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{Config, ProxyTaskGroup, TaskGroup};
use crate::cli::task::proxy_ctl_task::ProxyCtlTask;
use crate::cli::task::task_base::{TaskExecutionContext, TaskId, TaskInstance};
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::ProxyCommand;
use crate::cli::SubCommand;
use crate::config::proxy_config_base::ProxyConfig;
use anyhow::Result;
use indexmap::IndexMap;
use std::collections::HashMap;

#[async_trait::async_trait]
impl TaskGroup for ProxyTaskGroup {
    async fn tasks(&self, cmd: SubCommand, config: &Config) -> Result<TaskExecutionContext> {
        let proxy_config = match config {
            Config::Proxy(cfg) => cfg,
            _ => return Err(anyhow::anyhow!("Expected ProxyConfig for ProxyTaskGroup")),
        };

        let mut executable = IndexMap::new();
        let mut barrier = vec![];

        match &cmd {
            SubCommand::Proxy { command } => {
                let ssh_key = proxy_config.connection.ssh_auth_key();
                let user = proxy_config.connection.username.clone();
                let ssh_port = proxy_config.connection.ssh_port() as usize;

                match command {
                    ProxyCommand::Start {
                        config: config_path,
                    } => {
                        // download binary
                        let download_task = DownloadTask::from_proxy_config(&proxy_config)?;
                        barrier.push(download_task.len());
                        executable.extend(download_task);

                        // mkdir
                        let mkdir_remote_task = ExecCustomCommand::from_config(
                            &cmd,
                            "mkdir",
                            format!("mkdir -p {}", proxy_config.proxy_service.install_dir()),
                            config,
                        );
                        barrier.push(mkdir_remote_task.len());
                        executable.extend(mkdir_remote_task);

                        // upload ini config file and binary
                        let proxy_upload_task = upload_tasks(UploadTaskBuilderType::Proxy, &config);
                        barrier.push(proxy_upload_task.len());
                        executable.extend(proxy_upload_task);

                        // start proxy

                        // Extract and concatenate hosts with error handling
                        let hosts = proxy_config
                            .proxy_service
                            .proxy_addrs
                            .iter()
                            .filter_map(|addr| addr.split(':').next().map(|s| s.to_string()))
                            .collect::<Vec<String>>();

                        let mut args = HashMap::new();
                        args.insert(
                            "proxy_bin".to_string(),
                            proxy_config.proxy_service.proxy_bin(),
                        );
                        let cluster_ip_port_list = proxy_config
                            .proxy_service
                            .eloqkv_cluster_addr
                            .iter()
                            .flat_map(|ip_port| {
                                ip_port
                                    .split(',') // Split on ','
                                    .map(str::trim) // Trim any whitespace
                            })
                            .collect::<Vec<&str>>()
                            .join(",");
                        args.insert("cluster_addr".to_string(), cluster_ip_port_list);
                        let cluster_token = proxy_config
                            .proxy_service
                            .eloqkv_cluster_token
                            .iter()
                            .flat_map(|ip_port| {
                                ip_port
                                    .split(',') // Split on ','
                                    .map(str::trim) // Trim any whitespace
                            })
                            .collect::<Vec<&str>>()
                            .join(",");
                        args.insert("cluster_token".to_string(), cluster_token);
                        let cluster_password = proxy_config
                            .proxy_service
                            .eloqkv_cluster_password
                            .iter()
                            .flat_map(|ip_port| {
                                ip_port
                                    .split(',') // Split on ','
                                    .map(str::trim) // Trim any whitespace
                            })
                            .collect::<Vec<&str>>()
                            .join(",");
                        args.insert("cluster_password".to_string(), cluster_password);

                        let start_proxy_task = ProxyCtlTask::from_config(
                            ProxyCommand::Start {
                                config: config_path.clone(),
                            },
                            ssh_key.unwrap(),
                            user,
                            ssh_port,
                            hosts,
                            &args,
                            proxy_config,
                        );

                        barrier.push(start_proxy_task.len());
                        executable.extend(start_proxy_task);
                    }
                    ProxyCommand::Stop { .. } => {
                        // kill proxy process
                        let hosts = proxy_config
                            .proxy_service
                            .proxy_addrs
                            .iter()
                            .filter_map(|addr| addr.split(':').next().map(|s| s.to_string()))
                            .collect::<Vec<String>>();
                        let proxy_name = proxy_config.proxy_service.proxy_name.clone();

                        let mut args = HashMap::new();
                        args.insert("proxy_bin".to_string(), "/path/to/install".to_string());
                        // Add other necessary args as needed

                        let stop_proxy_task = ProxyCtlTask::from_config(
                            ProxyCommand::Stop { proxy_name },
                            ssh_key.unwrap(),
                            user,
                            ssh_port,
                            hosts,
                            &args,
                            proxy_config,
                        );

                        barrier.push(stop_proxy_task.len());
                        executable.extend(stop_proxy_task);
                    }
                    ProxyCommand::List { proxy_name } => {
                        //
                    }
                    ProxyCommand::Add {
                        cluster_name,
                        proxy_name,
                    } => {
                        todo!()

                        // // kill proxy process
                        // let host = proxy_config.proxy_service.proxy_hosts.clone();
                        // let proxy_name = proxy_config.proxy_service.proxy_name.clone();

                        // let mut args = HashMap::new();
                        // args.insert("proxy_bin".to_string(), "/path/to/install".to_string());
                        // // Add other necessary args as needed

                        // let stop_proxy_task = ProxyCtlTask::from_config(
                        //     ProxyCommand::Stop { proxy_name },
                        //     ssh_key.unwrap(),
                        //     user,
                        //     ssh_port,
                        //     host,
                        //     &args,
                        //     proxy_config,
                        // );

                        // barrier.push(stop_proxy_task.len());
                        // executable.extend(stop_proxy_task);

                        // // upload ini config file and binary
                        // let proxy_upload_task = upload_tasks(UploadTaskBuilderType::Proxy, &config);
                        // barrier.push(proxy_upload_task.len());
                        // executable.extend(proxy_upload_task);
                    }
                    ProxyCommand::Remove { cluster_name, .. } => {
                        todo!()
                    }
                }
            }
            _ => unreachable!(),
        }

        let cmd_str = cmd.as_ref().to_owned();
        Ok(TaskExecutionContext {
            task_group: format!("proxy-control-{cmd_str}"),
            barrier: Some(barrier), // Wrap in Some() if needed
            executable,
        })
    }
}
