use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeploymentConfig;
use crate::task_return_value;
use futures::future;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

// The timeout time of the probe, in seconds.
const TIMEOUT: u64 = 60 * 5;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogGroupState {
    pub log_group: String,
    pub state: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckHealthResponse {
    pub raft_stat: Vec<LogGroupState>,
}

#[derive(Clone, Debug)]
pub struct MonographLogProbeTask {
    task_id: TaskId,
    check_health_url: HashMap<usize, Vec<String>>,
}

impl MonographLogProbeTask {
    pub fn from_config(config: &DeploymentConfig) -> IndexMap<TaskId, TaskInstance> {
        if let Some(log_srv) = &config.deployment.log_service {
            let log_members = log_srv.group_members();
            // key is group id, value is member check health
            let check_health_url = log_members
                .iter()
                .map(|(usize, members)| {
                    let check_health_url_vec = members
                        .iter()
                        .map(|member| member.check_health_url.clone())
                        .collect_vec();
                    (*usize, check_health_url_vec)
                })
                .collect::<HashMap<usize, Vec<String>>>();

            let task_id = TaskId {
                cmd: "monograph_log_service_probe".to_string(),
                task: "readiness".to_string(),
                host: "_NONE".to_string(),
            };
            let probe_task = MonographLogProbeTask::new(task_id.clone(), check_health_url);
            IndexMap::from([(
                task_id,
                TaskInstance {
                    task_input: HashMap::default(),
                    task: Box::new(probe_task),
                    task_host: TaskHost::Local,
                },
            )])
        } else {
            IndexMap::new()
        }
    }

    fn build_command_result(&self, status: usize, output: String) -> ExecutionValue {
        HashMap::from([
            (
                CMD.to_string(),
                TaskArgValue::Str(self.check_health_string()),
            ),
            (CMD_STATUS.to_string(), TaskArgValue::Number(status)),
            (CMD_OUTPUT.to_string(), TaskArgValue::Str(output)),
        ])
    }

    pub fn new(task_id: TaskId, check_health_url: HashMap<usize, Vec<String>>) -> Self {
        Self {
            task_id,
            check_health_url,
        }
    }

    fn check_health_string(&self) -> String {
        self.check_health_url
            .iter()
            .map(|(group_id, urls)| {
                let multi_url_as_string = urls.join(";");
                format!("group_id={group_id},check_health_url={multi_url_as_string}")
            })
            .join("\t")
    }

    async fn action(&self) -> ExecutionValue {
        let client_rs = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .build();
        if let Err(client_build_err) = client_rs {
            let err_msg = client_build_err.to_string();
            let http_client_init_err = self.build_command_result(usize::MAX, err_msg);
            return http_client_init_err;
        }
        let expect_leader_count = self.check_health_url.len();
        println!(
            "MonographLogProbeTask current check_heal_url={:#?}",
            self.check_health_url
        );
        let http_client = client_rs.unwrap();
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        loop {
            let probe_result_fut = self
                .check_health_url
                .iter()
                .flat_map(|(group_id, urls)| {
                    urls.iter()
                        .map(|url| {
                            let send_fut = http_client.clone().get(url).send();
                            async move { (*group_id, send_fut.await) }
                        })
                        .collect_vec()
                })
                .collect_vec();

            let probe_rs = future::join_all(probe_result_fut).await;

            let leader_count_fut = probe_rs
                .into_iter()
                .map(|(group_id, response_rs)| async move {
                    println!("MonographLogProbeTask retrieve response={response_rs:#?}");
                    let my_leader = if let Ok(response) = response_rs {
                        let status_code = response.status();
                        if !status_code.is_success() {
                            let request_url = response.url();
                            warn!("MonographLogProbeTask get response not_ok {status_code:?}, url = {request_url:#?}");
                            (group_id, 0_usize)
                        } else {
                            let check_health_rs = response.json::<CheckHealthResponse>().await;
                            let check_health_rsp = check_health_rs.unwrap();
                            let raft_stat = check_health_rsp.raft_stat;
                            assert!(!raft_stat.is_empty());
                            let group_state = raft_stat.first().unwrap();
                            println!("MonographLogProbeTask retrieve group_id={group_id:?},member_role={group_state:#?}");
                            if group_state.state.to_uppercase().eq("LEADER") {
                                (group_id, 1_usize)
                            } else {
                                (group_id, 0_usize)
                            }
                        }
                    } else {
                        (group_id, 0_usize)
                    };
                    my_leader
                }).collect_vec();

            let all_group_leaders = future::join_all(leader_count_fut).await;
            let total_leader = all_group_leaders
                .iter()
                .map(|(_group, leader_count)| *leader_count)
                .sum::<usize>();

            if total_leader == expect_leader_count {
                let execution_success = self.build_command_result(
                    0,
                    all_group_leaders
                        .iter()
                        .map(|(group, leader_count)| {
                            format!("group={group},leader_count={leader_count}")
                        })
                        .join(";"),
                );
                return execution_success;
            }
            interval.tick().await;
        }
    }
}

#[async_trait::async_trait]
impl TaskExecutor for MonographLogProbeTask {
    fn identifier(&self) -> TaskId {
        self.task_id.clone()
    }

    async fn execute(
        &self,
        _task_host: TaskHost,
        _task_input: HashMap<String, TaskArgValue>,
    ) -> anyhow::Result<Option<ExecutionValue>> {
        println!("{} execute.\n", self.task_id.pretty_string());
        let timeout_duration = Duration::from_secs(TIMEOUT);
        let action_result = timeout(timeout_duration, self.action()).await;
        let probe_result = if let Ok(exec_rs) = action_result {
            exec_rs
        } else {
            let timeout_elapsed = action_result.err().unwrap();
            self.build_command_result(usize::MAX,
                                      format!("MonographLogProbeTask \
                                      timeout elapsed={timeout_elapsed:?} secs,timeout={TIMEOUT} secs"))
        };
        let probe_cmd = self.check_health_string();
        task_return_value!(
            probe_result,
            |status_code: usize| -> CmdErr {
                CmdErr::ExecUserCmdErr(probe_cmd, status_code.to_string())
            },
            "MonographLogProbeTask"
        )
    }
}
