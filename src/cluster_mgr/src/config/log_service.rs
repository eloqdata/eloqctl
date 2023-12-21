use indexmap::IndexMap;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const LOG_SRV_REPLICA_NUM: usize = 3;

#[derive(PartialEq, Eq, Debug, Clone)]
struct NodeDiskCell {
    host_idx: i32,
    host: String,
    dist_idx: i32,
    disk: String,
}

impl Default for NodeDiskCell {
    fn default() -> Self {
        Self {
            host_idx: -1,
            host: "_NONE_".to_string(),
            dist_idx: -1,
            disk: "_NONE_".to_string(),
        }
    }
}

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

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct LogCmdItems {
    pub group_members_config: String,
    pub log_member: LogGroupMember,
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
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
    pub readiness: Option<LogReadiness>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct LogReadiness {
    pub timeout_sec: u64,
    pub delay_ms: Option<usize>,
    pub success_threshold: Option<usize>,
}

impl Default for LogReadiness {
    fn default() -> Self {
        Self {
            timeout_sec: 300,
            delay_ms: None,
            success_threshold: Some(3),
        }
    }
}

impl LogService {
    pub fn log_host_unique(&self) -> Vec<String> {
        self.nodes
            .iter()
            .map(|log_node| log_node.host.clone())
            .unique()
            .collect_vec()
    }

    pub fn readiness_opts(&self) -> LogReadiness {
        if let Some(readiness_ref) = self.readiness.as_ref() {
            readiness_ref.clone()
        } else {
            LogReadiness::default()
        }
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
            .map(|idx| {
                if from + idx > node_len - 1 {
                    node_len - 1
                } else {
                    from + idx
                }
            })
            .collect_vec()
    }

    pub fn log_replica(&self) -> usize {
        if let Some(replica_num) = self.replica {
            replica_num as usize
        } else {
            LOG_SRV_REPLICA_NUM
        }
    }

    fn try_set_leader(
        &self,
        members: Option<IndexMap<usize, Vec<LogGroupMember>>>,
    ) -> IndexMap<usize, Vec<LogGroupMember>> {
        let mut log_members = if let Some(input_members) = members {
            input_members
        } else {
            self.group_members()
        };
        log_members
            .iter_mut()
            .skip(1)
            .for_each(|(group_id, member)| {
                member.swap(*group_id, 0);
            });
        log_members
    }

    /// The log_group distribution strategy ensures maximum disk utilization
    /// while maintaining availability for the io-sensitive application, log_service.
    /// There are two aspects to this strategy:
    /// 1. members within a log group are distributed across different machines to balance
    ///    the number of leaders on each node;
    /// 2. to maximize throughput, all disks are utilized as much as possible by ensuring
    ///    uniform distribution of disks across nodes. This ensures balanced load distribution among nodes.
    pub fn group_members(&self) -> IndexMap<usize, Vec<LogGroupMember>> {
        let mut sorted_nodes = self.nodes.clone();
        sorted_nodes.sort_by_key(|log_node| log_node.host.clone());
        let cells_table = self.members_grouping(&sorted_nodes);
        let replica = self.log_replica();
        let cells_count = cells_table.len();
        let mut port_usage = HashMap::new();
        let mut group_members = IndexMap::new();
        let mut group_id = 0_usize;

        for (row_id, cells) in cells_table.iter().enumerate() {
            let hole_count = cells
                .iter()
                .enumerate()
                .filter(|(_idx, cell)| cell.disk.eq("_NONE_"))
                .count();

            let merge_cells = if hole_count > 0 {
                let next_row = row_id + 1;
                if next_row == cells_count {
                    break;
                }
                let next_row = cells_table.get(next_row).unwrap();
                let next_cells = next_row.iter().take(hole_count).cloned().collect_vec();
                let mut cells_copy = cells.clone();
                cells_copy.extend(next_cells.into_iter());
                let filter_cells = cells_copy
                    .iter()
                    .filter(|cell| !cell.disk.eq("_NONE_"))
                    .cloned()
                    .collect_vec();
                if filter_cells.len() == replica {
                    filter_cells
                } else {
                    vec![]
                }
            } else {
                cells.clone()
            };
            if merge_cells.is_empty() {
                continue;
            }
            let members = merge_cells
                .iter()
                .enumerate()
                .map(|(node_id, cell)| {
                    let host_id = cell.host_idx;
                    let node = sorted_nodes.get(host_id as usize).unwrap();
                    let disk = cell.disk.clone();
                    let port = if !port_usage.contains_key(&node.host) {
                        port_usage.insert(node.host.clone(), node.port);
                        node.port
                    } else {
                        *port_usage.get(&node.host).unwrap() + (group_id + node_id) as u16
                    };
                    let node_host = &node.host;
                    LogGroupMember {
                        node_id,
                        group_id,
                        member_host: node_host.to_string(),
                        port,
                        storage_path: format!("{disk}/lg{group_id}/ln{node_id}"),
                        check_health_url: format!("http://{node_host}:{port}/healthz"),
                    }
                })
                .collect_vec();
            group_members.insert(group_id, members);
            group_id += 1;
        }
        group_members
        //self.try_set_leader(Some(group_members))
    }

    fn init_members_table(&self, node_sorted: &[LogServiceNode]) -> Vec<Vec<NodeDiskCell>> {
        let cols = self.nodes.len();
        let rows = self
            .nodes
            .iter()
            .map(|node| node.data_dir.len())
            .max()
            .unwrap();

        let mut table = vec![vec![NodeDiskCell::default(); cols]; rows];
        for (row, item) in table.iter_mut().enumerate().take(rows) {
            node_sorted.iter().enumerate().for_each(|(node_idx, node)| {
                let storage = &node.data_dir;
                let host = &node.host;
                let disk_len = storage.len();
                if row < disk_len {
                    let disk = storage.get(row).unwrap();
                    item[node_idx] = NodeDiskCell {
                        host_idx: node_idx as i32,
                        host: host.to_string(),
                        dist_idx: row as i32,
                        disk: disk.to_string(),
                    };
                };
            });
        }
        table
    }

    fn members_grouping(&self, node_sorted: &[LogServiceNode]) -> Vec<Vec<NodeDiskCell>> {
        let node_host_table = self.init_members_table(node_sorted);
        let member_count = self.log_replica();
        let mut pre_remain_cell = 0_usize;
        let mut group_id = 0_usize;
        let mut result = vec![];
        for (row_id, row) in node_host_table.iter().enumerate() {
            let cols_len = row.len();
            let row_slice = row.as_slice();
            let (t_member, outer_from) = if pre_remain_cell == 0 {
                (vec![], 0)
            } else {
                let pre_row_idx = row_id - 1;
                let pre_row_slice = node_host_table.get(pre_row_idx).unwrap().as_slice();
                let pre_row_member = &pre_row_slice[cols_len - pre_remain_cell..];
                let curr_row_from = member_count - pre_remain_cell;
                let curr_row_member = &row_slice[0..curr_row_from];
                ([pre_row_member, curr_row_member].concat(), curr_row_from)
            };
            if !t_member.is_empty() {
                result.push(t_member);
                group_id += 1;
            }
            let curr_col_len = cols_len - outer_from;
            let curr_group = curr_col_len / member_count;
            let curr_remain = curr_col_len % member_count;
            pre_remain_cell = curr_remain;
            (0..curr_group).for_each(|inner_group_id| {
                let inner_from = inner_group_id + outer_from;
                let to = inner_from + member_count;
                group_id += 1;
                let inner_members = &row_slice[inner_from..to];
                inner_members.to_vec();
                result.push(inner_members.to_vec());
            });
        }
        result
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
    use crate::config::log_service::{LogReadiness, LogService, LogServiceNode};
    use itertools::Itertools;
    use std::collections::HashMap;

    fn mock_log_service(
        host_num: usize,
        replica: usize,
        host_disk: HashMap<usize, usize>,
    ) -> LogService {
        let nodes = (0..host_num)
            .map(|idx| {
                let disks = host_disk.get(&idx).unwrap();
                let disk_path = (0..*disks)
                    .map(|disk_idx| format!("/data/opt/disk_{disk_idx}"))
                    .collect_vec();
                LogServiceNode {
                    host: format!("127.0.0.{idx}"),
                    data_dir: disk_path,
                    port: 9400,
                }
            })
            .collect_vec();
        LogService {
            nodes,
            replica: Some(replica as u32),
            readiness: Some(LogReadiness::default()),
        }
    }

    #[test]
    pub fn test_gen_nodes() {
        let one_host_log_srv = &mock_log_service(1, 3, HashMap::from([(0, 3)]));
        let nodes = one_host_log_srv.gen_log_node_ids(0);
        println!("{nodes:?}");
        let expected_total_nodes = nodes.iter().sum::<usize>();
        assert_eq!(0, expected_total_nodes);
    }

    #[test]
    pub fn test_log_service_groups() {
        let one_host_log_srv = &mock_log_service(1, 3, HashMap::from([(0, 3)]));
        let group = one_host_log_srv.group();
        println!("host=1,group_size={group}");
        assert_eq!(1, group);

        let multi_host_log_srv = &mock_log_service(4, 3, HashMap::from([(0, 3), (1, 3), (2, 3)]));
        let group = multi_host_log_srv.group();
        println!("host=4,group_size={group}");
        assert_eq!(2, group);
    }

    #[test]
    pub fn test_memberships_one_lg() {
        let log_srv = &mock_log_service(1, 1, HashMap::from([(0, 1)]));
        let members = log_srv.group_members();
        println!("test_memberships_one_lg members = {members:#?}");
        assert_eq!(1, members.len());
    }

    #[test]
    pub fn test_memberships_multi_lg() {
        let log_srv = &mock_log_service(3, 3, HashMap::from([(0, 1), (1, 1), (2, 1)]));
        let members = log_srv.group_members();
        println!("test_memberships_multi_lg members = {members:#?}");
        assert_eq!(1, members.len());
    }
}
