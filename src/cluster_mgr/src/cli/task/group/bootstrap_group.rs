use crate::cli::task::cassandra_ctl_task::CassandraCtlTask;
use crate::cli::task::exec_custom_cmd::ExecCustomCommand;
use crate::cli::task::group::{InstallDBTaskGroup, TaskGroup};
use crate::cli::task::monograph_bootstrap_task::MonographInstall;
use crate::cli::task::task_base::{TaskExecutionContext, TaskHost};
use crate::cli::task::upload::upload_task_builder::{upload_tasks, UploadTaskBuilderType};
use crate::cli::CommandArgs;
use crate::config::config_base::DeploymentConfig;
use crate::config::deployment::Product;
use crate::config::{DeploymentPackage, StorageProvider};
use indexmap::IndexMap;
use tracing::info;

#[async_trait::async_trait]
impl TaskGroup for InstallDBTaskGroup {
    async fn tasks(
        &self,
        cmd_args: CommandArgs,
        config: DeploymentConfig,
    ) -> anyhow::Result<TaskExecutionContext> {
        let conn_user = &config.connection.username;
        let ssh_port = config.connection.ssh_port();
        let install_db_host_string = config.deployment.bootstrap_host();
        let install_db_host = TaskHost::Remote {
            user: conn_user.clone(),
            port: ssh_port as usize,
            hosts: install_db_host_string,
        };
        info!(
            "InstallDBTaskGroup The bootstrap node is ={:?}",
            install_db_host
        );
        let install_cmd = CommandArgs::Install {
            cluster: config.clone().deployment.cluster_name,
        };
        let storage_provider = config.get_monograph_storage()?;

        let mut execution_context_tuple = match storage_provider {
            StorageProvider::Cassandra => {
                let upload_cass_config_task =
                    upload_tasks(UploadTaskBuilderType::CassConf, &config);
                let mut barrier = vec![upload_cass_config_task.len()];
                let mut executable = IndexMap::new();
                executable.extend(upload_cass_config_task);
                if let Some(monitor) = config.deployment.monitor.as_ref() {
                    if let Some(mcac_collector) = &monitor.cassandra_collector {
                        let install_dir = config.install_dir();
                        let update_http_port_cmd = mcac_collector.update_http_port_cmd(install_dir);
                        let cassandra_hosts = config.get_host_list(DeploymentPackage::Storage);
                        let update_http_port_task = ExecCustomCommand::build_task_by_host(
                            update_http_port_cmd,
                            &config,
                            cassandra_hosts,
                            None,
                        );
                        barrier.push(update_http_port_task.len());
                        executable.extend(update_http_port_task);
                    }
                }
                let cassandra_start = CassandraCtlTask::from_config(install_cmd, &config);
                barrier.push(cassandra_start.len());
                executable.extend(cassandra_start);

                TaskExecutionContext {
                    task_group: cmd_args.as_ref().to_string(),
                    barrier: Some(barrier),
                    executable,
                }
            }
            _ => TaskExecutionContext {
                task_group: cmd_args.as_ref().to_string(),
                barrier: None,
                executable: IndexMap::new(),
            },
        };
        let mut barrier = execution_context_tuple.clone().barrier.unwrap();
        let mut executable = execution_context_tuple.executable;

        if config.product() == Product::Monograph {
            // Bootstrap
            let monograph_install = MonographInstall::from_config(&config, install_db_host);
            barrier.push(monograph_install.len());
            executable.extend(monograph_install);
        }

        let upload_data_dir_task = upload_tasks(UploadTaskBuilderType::DataDir, &config);
        if !upload_data_dir_task.is_empty() {
            barrier.push(upload_data_dir_task.len());
            executable.extend(upload_data_dir_task);

            execution_context_tuple.barrier = Some(barrier.clone());
            execution_context_tuple.executable = executable.clone();
        }
        // rm -rf cc_ng/ tx_log/
        let remote_install_dir = config.install_dir();
        let rm_log_data_cmd = format!(
            "rm -rf {remote_install_dir}/datafarm/cc_ng {remote_install_dir}/datafarm/tx_log",
        );

        let rm_log_data_task_instance = ExecCustomCommand::from_config(rm_log_data_cmd, &config);
        barrier.push(rm_log_data_task_instance.len());
        executable.extend(rm_log_data_task_instance);
        execution_context_tuple.barrier = Some(barrier);
        execution_context_tuple.executable = executable;
        Ok(execution_context_tuple)
    }
}
