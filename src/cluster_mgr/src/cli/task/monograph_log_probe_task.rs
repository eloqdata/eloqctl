use crate::cli::cmd_base::HTTP_INTERNAL;
use crate::cli::task::task_base::CmdErr;
use crate::cli::task::task_base::{
    ExecutionValue, TaskArgValue, TaskExecutor, TaskHost, TaskId, TaskInstance,
};
use crate::cli::{CMD, CMD_OUTPUT, CMD_STATUS};
use crate::config::config_base::DeployConfig;
use crate::config::log_service::LogReadiness;
use crate::task_return_value;
use futures::future;
use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info, warn};

// The timeout time of the probe, in seconds.
const TIMEOUT: u64 = 60 * 5;

#[derive(Clone, Debug)]
struct MemberLeaderInfo {
    group_id: usize,
    leader_count: usize,
    check_health: String,
}

impl std::fmt::Display for MemberLeaderInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(group:{}, leaders:{}, addr:{})",
            self.group_id, self.leader_count, self.check_health
        )
    }
}

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
    readiness: LogReadiness,
}

impl MonographLogProbeTask {
    pub fn from_config(config: &DeployConfig) -> IndexMap<TaskId, TaskInstance> {
        let deployment_ref = &config.deployment;
        let has_log_srv = deployment_ref.log_service.is_some();
        if !has_log_srv {
            return IndexMap::new();
        }
        let log_srv = deployment_ref.log_service.as_ref().unwrap();
        let log_readiness = log_srv.readiness_opts();
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
            let probe_task =
                MonographLogProbeTask::new(task_id.clone(), log_readiness, check_health_url);
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

    fn build_command_result(&self, status: i32, output: String) -> ExecutionValue {
        HashMap::from([
            (
                CMD.to_string(),
                TaskArgValue::Str(self.check_health_string()),
            ),
            (CMD_STATUS.to_string(), TaskArgValue::Number(status)),
            (CMD_OUTPUT.to_string(), TaskArgValue::Str(output)),
        ])
    }

    pub fn new(
        task_id: TaskId,
        log_readiness: LogReadiness,
        check_health_url: HashMap<usize, Vec<String>>,
    ) -> Self {
        Self {
            task_id,
            readiness: log_readiness,
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
        let expect_leader_count = self.check_health_url.len();
        let mut interval = tokio::time::interval(Duration::from_millis(300));
        let success = self.readiness.success_threshold.unwrap_or(3);
        let mut success_counter = 0_usize;
        loop {
            let probe_result_fut = self
                .check_health_url
                .iter()
                .flat_map(|(group_id, urls)| {
                    urls.iter()
                        .map(|url| {
                            let send_fut = HTTP_INTERNAL.get(url).send();
                            async move { (*group_id, send_fut.await) }
                        })
                        .collect_vec()
                })
                .collect_vec();
            let probe_rs = future::join_all(probe_result_fut).await;
            let leader_count_fut = probe_rs
                .into_iter()
                .map(|(group_id, response_rs)| async move {
                    let my_leader = if let Ok(response) = response_rs {
                        let status_code = response.status();
                        let request_url = response.remote_addr().unwrap().to_string();
                        if !status_code.is_success() {
                            warn!("MonographLogProbeTask get response not_ok {status_code:?},url={request_url:#?}");
                            (group_id, Ok(MemberLeaderInfo {
                                group_id,
                                leader_count: 0,
                                check_health: request_url,
                            }))
                        } else {
                            let check_health_rs = response.json::<CheckHealthResponse>().await;
                            let check_health_rsp = check_health_rs.unwrap();
                            let raft_stat = check_health_rsp.raft_stat;
                            assert!(!raft_stat.is_empty());
                            let group_state = raft_stat.first().unwrap();
                            info!("MonographLogProbeTask retrieve from {request_url:#?} group_id={group_id:?},member_role={group_state:#?}");
                            if group_state.state.to_uppercase().eq("LEADER") {
                                (group_id, Ok(MemberLeaderInfo {
                                    group_id,
                                    leader_count: 1,
                                    check_health: request_url,
                                }))
                            } else {
                                (group_id, Ok(MemberLeaderInfo {
                                    group_id,
                                    leader_count: 0,
                                    check_health: request_url,
                                }))
                            }
                        }
                    } else {
                        let rsp_err = response_rs.err().unwrap();
                        debug!("{rsp_err:#?}");
                        info!("MonographLogProbeTask Failed to request group_id={group_id}, error={rsp_err:#?}");
                        (group_id, Err(rsp_err))
                    };
                    my_leader
                }).collect_vec();

            let all_group_leaders = future::join_all(leader_count_fut).await;

            let mut total_leader = 0;
            for (_group, leader_count_rs) in &all_group_leaders {
                if let Ok(member_leader_info) = leader_count_rs.as_ref() {
                    total_leader += member_leader_info.clone().leader_count;
                } else {
                    total_leader += 0;
                }
            }
            if total_leader == expect_leader_count {
                if success_counter < success {
                    info!("MonographLogProbeTask success_counter={success_counter} < success_threshold={success}");
                    interval.tick().await;
                    success_counter += 1;
                    continue;
                }
                let execution_success = self.build_command_result(
                    0,
                    all_group_leaders
                        .iter()
                        .map(|(_group, member_leader_info)| {
                            let member = member_leader_info.as_ref().unwrap();
                            member.to_string()
                        })
                        .unique()
                        .join("\n"),
                );
                return execution_success;
            }
            info!("MonographLogProbeTask found current leader count={total_leader:#?} != {expect_leader_count}.\
             next round 300ms after");
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
        info!("execute {}", self.task_id.format_string());
        let time_out = self.readiness.timeout_sec;
        let timeout_duration = Duration::from_secs(time_out);
        let action_result = timeout(timeout_duration, self.action()).await;
        let probe_result = if let Ok(exec_rs) = action_result {
            exec_rs
        } else {
            let timeout_elapsed = action_result.err().unwrap();
            self.build_command_result(-1,
                                      format!("MonographLogProbeTask \
                                      timeout elapsed={timeout_elapsed:?} secs,timeout={TIMEOUT} secs"))
        };
        let probe_cmd = self.check_health_string();
        task_return_value!(
            probe_result,
            |status_code: i32| -> CmdErr {
                CmdErr::ExecUserCmdErr(probe_cmd, status_code.to_string())
            },
            "MonographLogProbeTask"
        )
    }
}
