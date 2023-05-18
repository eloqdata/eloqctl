use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const LOG_SRV_REPLICA_NUM: usize = 3;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct LogProcessKey {
    pub host: String,
    pub port: u16,
}

impl ToString for LogProcessKey {
    fn to_string(&self) -> String {
        let port = self.port;
        let host = &self.host;
        format!("{host}:{port}")
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct LogCmdItems {
    pub group_members_config: String,
    pub log_member: LogGroupMember,
}

#[derive(PartialEq, Debug, Clone)]
pub struct LogGroupMember {
    pub node_id: usize,
    pub group_id: usize,
    pub member_host: String,
    pub port: u16,
    pub storage_path: String,
    pub check_health_url: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct LogServiceNode {
    pub host: String,
    pub data_dir: Vec<String>,
    pub port: u16,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct LogService {
    pub nodes: Vec<LogServiceNode>,
    pub replica: Option<u32>,
}

impl LogService {
    pub fn log_host_unique(&self) -> Vec<String> {
        self.nodes
            .iter()
            .map(|log_node| log_node.host.clone())
            .unique()
            .collect_vec()
    }

    fn group(&self) -> usize {
        let host_num = self.log_host_unique().len();
        let group_member = self.log_replica();
        if host_num < group_member {
            1_usize
        } else {
            let group = host_num % group_member;
            if group == 0 {
                host_num / group_member
            } else {
                (host_num / group_member) + 1
            }
        }
    }

    fn gen_log_node_ids(&self, start: usize) -> Vec<usize> {
        let node_len = self.nodes.len();
        let from = if start > node_len { 0 } else { start };
        (0..self.log_replica())
            .into_iter()
            .map(|idx| {
                if from + idx > node_len - 1 {
                    node_len - 1
                } else {
                    from + idx
                }
            })
            .collect_vec()
    }

    pub fn group_members(&self) -> HashMap<usize, Vec<LogGroupMember>> {
        let group = self.group();
        let replica = self.log_replica();
        let mut start = 0;
        let mut port_usage = HashMap::new();
        // println!("The logService contains [{group:?}] group");
        (0..group)
            .into_iter()
            .map(|group_id| {
                let node_ids = self.gen_log_node_ids(start);
                let members = node_ids
                    .iter()
                    .enumerate()
                    .map(|(idx, id)| {
                        let node = self.nodes.get(*id).unwrap();
                        let port = if !port_usage.contains_key(&node.host) {
                            port_usage.insert(node.host.clone(), node.port);
                            node.port
                        } else {
                            *port_usage.get(&node.host).unwrap() + (group_id + idx) as u16
                        };

                        let data_dir_len = node.data_dir.len();
                        let data_dir = if *id >= data_dir_len {
                            node.data_dir.last().unwrap()
                        } else {
                            node.data_dir.get(*id).unwrap()
                        };
                        let node_host = node.host.clone();
                        LogGroupMember {
                            node_id: idx,
                            group_id,
                            member_host: node_host.clone(),
                            port,
                            storage_path: format!("{data_dir}/group_{group_id}_node_{idx}"),
                            check_health_url: format!("{node_host}:{port}/healthz"),
                        }
                    })
                    .collect_vec();
                start += replica;
                (group_id, members)
            })
            .collect::<HashMap<usize, Vec<LogGroupMember>>>()
    }

    pub fn log_replica(&self) -> usize {
        if let Some(replica_num) = self.replica {
            replica_num as usize
        } else {
            LOG_SRV_REPLICA_NUM
        }
    }

    pub fn group_member_config(&self, members: &[LogGroupMember]) -> IndexMap<usize, String> {
        let group_members = members
            .iter()
            .into_group_map_by(|log_member| log_member.group_id);

        group_members
            .into_iter()
            .map(|(group_id, inner_group_member)| {
                let member_config = inner_group_member
                    .iter()
                    .map(|inner_member| {
                        format!("{}:{}", inner_member.member_host, inner_member.port)
                    })
                    .collect_vec()
                    .join(",");

                (group_id, member_config)
            })
            .collect::<IndexMap<usize, String>>()
    }

    /// Grouping by host means categorizing log member processes on that node.
    fn host_members(&self, all_members: &[LogGroupMember]) -> HashMap<String, Vec<LogGroupMember>> {
        all_members
            .iter()
            .into_group_map_by(|log_member| log_member.member_host.clone())
            .into_iter()
            .map(|(host, members)| (host, members.into_iter().cloned().collect()))
            .collect()
    }

    pub fn group_member_as_vec(&self) -> Vec<LogGroupMember> {
        self.group_members()
            .values()
            .flat_map(|val| val.iter().cloned().collect_vec())
            .collect_vec()
    }

    /// log startup command, with host as granularity, key is hostname value is start command.
    pub fn log_start_cmd(&self) -> HashMap<String, Vec<LogCmdItems>> {
        let all_member_vec = self.group_member_as_vec(); //self.group_members();
        let all_member_as_slice = all_member_vec.as_slice();
        let group_member_config = self.group_member_config(all_member_as_slice);
        let host_members_lookup = self.host_members(all_member_as_slice);

        host_members_lookup
            .iter()
            .map(|(host, members)| {
                let cmds = members
                    .iter()
                    .map(|log_member| {
                        let group_id = log_member.group_id;
                        let member_config = group_member_config.get(&group_id).unwrap().clone();
                        LogCmdItems {
                            group_members_config: member_config,
                            log_member: log_member.clone(),
                        }
                    })
                    .collect_vec();
                (host.to_string(), cmds)
            })
            .collect::<HashMap<String, Vec<LogCmdItems>>>()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::log_service::{LogService, LogServiceNode};
    use itertools::Itertools;

    fn mock_log_service(host_num: usize, replica: usize) -> LogService {
        let nodes = (0..host_num)
            .into_iter()
            .map(|idx| LogServiceNode {
                host: format!("127.0.0.{idx}"),
                data_dir: vec!["/data/opt/log_srv".to_string()],
                port: 9800,
            })
            .collect_vec();
        LogService {
            nodes,
            replica: Some(replica as u32),
        }
    }

    #[test]
    pub fn test_gen_nodes() {
        let one_host_log_srv = &mock_log_service(1, 3);
        let nodes = one_host_log_srv.gen_log_node_ids(0);
        println!("{nodes:?}");
        let expected_total_nodes = nodes.iter().sum::<usize>();
        assert_eq!(0, expected_total_nodes);
    }

    #[test]
    pub fn test_log_service_groups() {
        let one_host_log_srv = &mock_log_service(1, 3);
        let group = one_host_log_srv.group();
        println!("host=1,group_size={group}");
        assert_eq!(1, group);

        let multi_host_log_srv = &mock_log_service(4, 3);
        let group = multi_host_log_srv.group();
        println!("host=4,group_size={group}");
        assert_eq!(2, group);
    }

    #[test]
    pub fn test_log_group_members() {
        let log_srv = &mock_log_service(4, 3);
        let expect_group = 2;
        let expect_members = 2 * log_srv.log_replica();
        let members = log_srv.group_members();
        println!("log_members={members:#?}");
        let groups = members
            .iter()
            .flat_map(|(_, member)| member.iter().map(|inner_member| inner_member.group_id))
            .unique()
            .count();
        println!("groups={groups}");
        assert_eq!(expect_members, members.len());
        assert_eq!(expect_group, groups);
    }

    #[test]
    pub fn test_log_start_cmd() {
        let log_srv = &mock_log_service(5, 3);
        let log_srv_cmd = log_srv.log_start_cmd();
        println!("log_srv_cmd={log_srv_cmd:#?}");
        let hosts = log_srv_cmd.keys();
        assert_eq!(5, hosts.len());
    }

    #[test]
    pub fn test_group_member_config() {
        let log_srv = &mock_log_service(4, 3);
        let binding = log_srv.group_member_as_vec();
        let all_members = binding.as_slice();
        let group_member_config = log_srv.group_member_config(all_members);
        println!("{group_member_config:#?}");
        assert_eq!(2, group_member_config.len());
        let all_config = Vec::from_iter(group_member_config.values())
            .into_iter()
            .join(",");
        println!("all_config={all_config}");
        let config_split = all_config.split(',').count();
        let item_members_count = 2 * log_srv.log_replica();
        assert_eq!(item_members_count, config_split);
    }
}
