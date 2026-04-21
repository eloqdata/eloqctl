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

impl std::fmt::Display for LogProcessKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
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

#[serde_with::skip_serializing_none]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct LogService {
    pub image: Option<String>,
    pub nodes: Vec<LogServiceNode>,
    pub replica: u32,
    pub readiness: Option<LogReadiness>,
    pub bthread_concurrency: Option<u32>,
    // Optional cloud storage flags for log service's rocks backend
    pub aws_access_key_id: Option<String>,
    pub aws_secret_key: Option<String>,
    pub bucket_name: Option<String>,
    #[serde(alias = "endpoint_url")]
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket_prefix: Option<String>,
    pub object_path: Option<String>,
    pub target_file_size_base: Option<String>,
    pub sst_file_cache_size: Option<String>,
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

    pub fn host_used_ports(&self, host: &str) -> Vec<u16> {
        self.nodes
            .iter()
            .filter_map(|node| {
                if node.host.eq(host) {
                    Some(node.port)
                } else {
                    None
                }
            })
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
        self.replica as usize
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
                    // If we're at the last row, just process the current row without merging
                    let filter_cells = cells
                        .iter()
                        .filter(|cell| !cell.disk.eq("_NONE_"))
                        .cloned()
                        .collect_vec();

                    // If we have enough valid cells for at least one replica, use them
                    if filter_cells.len() >= replica {
                        // Take only the first 'replica' number of cells to form a complete group
                        filter_cells.into_iter().take(replica).collect_vec()
                    } else {
                        // If we don't have enough cells for a complete group, skip this row
                        vec![]
                    }
                } else {
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
                .map(|(local_idx, cell)| {
                    let host_id = cell.host_idx;
                    let node = sorted_nodes.get(host_id as usize).unwrap();
                    let disk = cell.disk.clone();
                    let port = if !port_usage.contains_key(&node.host) {
                        port_usage.insert(node.host.clone(), node.port);
                        node.port
                    } else {
                        // below produce port+1,port+2,...; i think it is out of use by the current design.
                        // *port_usage.get(&node.host).unwrap() + (group_id + local_idx) as u16

                        node.port
                    };
                    let node_host = &node.host;

                    // Find the global index of this node in the original nodes list
                    let global_node_id = self
                        .nodes
                        .iter()
                        .position(|original_node| {
                            original_node.host == *node_host && original_node.port == port
                        })
                        .unwrap_or(local_idx); // fallback to local index if not found

                    LogGroupMember {
                        node_id: global_node_id,
                        group_id,
                        member_host: node_host.to_string(),
                        port,
                        storage_path: disk.to_string(),
                        check_health_url: format!("http://{node_host}:{port}/healthz"),
                    }
                })
                .collect_vec();
            group_members.insert(group_id, members);
            group_id += 1;
        }

        // BUGFIX: Correct the node_id to be the index in the final txlog_service_list
        // The node_id should be the index of the host:port pair in the ordered list
        // that gets written to the EloqKV configuration as txlog_service_list
        let mut all_members = Vec::new();

        // Collect all members in the order they were inserted into the IndexMap
        // This preserves the order from the grouping algorithm
        for (_group_id, members) in &group_members {
            all_members.extend(members.clone());
        }

        // Create a mapping from host:port to the correct node_id index
        let mut host_port_to_node_id = HashMap::new();
        for (idx, member) in all_members.iter().enumerate() {
            let host_port = format!("{}:{}", member.member_host, member.port);
            host_port_to_node_id.insert(host_port, idx);
        }

        // Update all members with the correct node_id
        for members in group_members.values_mut() {
            for member in members {
                let host_port = format!("{}:{}", member.member_host, member.port);
                if let Some(&correct_node_id) = host_port_to_node_id.get(&host_port) {
                    member.node_id = correct_node_id;
                }
            }
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
        let replica_count = self.log_replica();
        let mut result = vec![];

        // Group nodes by host to understand the distribution
        let mut nodes_by_host: HashMap<String, Vec<(usize, &LogServiceNode)>> = HashMap::new();
        for (idx, node) in node_sorted.iter().enumerate() {
            nodes_by_host
                .entry(node.host.clone())
                .or_insert_with(Vec::new)
                .push((idx, node));
        }

        // Ensure deterministic order of hosts to avoid inconsistent ordering between
        // multiple invocations (e.g., txlog_service_list vs -conf generation).
        let mut hosts: Vec<String> = nodes_by_host.keys().cloned().collect();
        hosts.sort();
        let total_nodes = node_sorted.len();
        let mut assigned_nodes = vec![false; total_nodes];
        let mut leader_host_index = 0; // Track which host should provide the next leader

        // Create groups ensuring:
        // 1. Each group has nodes from different hosts (host diversity)
        // 2. Leaders (first nodes) of different groups are from different hosts (leader distribution)
        while assigned_nodes.iter().any(|&assigned| !assigned) {
            let mut group_cells = Vec::new();
            let mut hosts_used_in_group = std::collections::HashSet::new();

            // Step 1: Assign leader from a different host than previous groups
            let mut leader_assigned = false;
            let mut attempts = 0;
            let disk_default = "default".to_string();

            while !leader_assigned && attempts < hosts.len() {
                let leader_host = &hosts[leader_host_index % hosts.len()];
                leader_host_index += 1;

                // Find an unused node from this host to be the leader
                if let Some(host_nodes) = nodes_by_host.get(leader_host) {
                    for (node_idx, node) in host_nodes {
                        if !assigned_nodes[*node_idx] {
                            assigned_nodes[*node_idx] = true;
                            hosts_used_in_group.insert(leader_host.clone());

                            let disk = node.data_dir.first().unwrap_or(&disk_default);
                            group_cells.push(NodeDiskCell {
                                host_idx: *node_idx as i32,
                                host: node.host.clone(),
                                dist_idx: 0,
                                disk: disk.clone(),
                            });

                            leader_assigned = true;
                            break;
                        }
                    }
                }
                attempts += 1;
            }

            // Step 2: Fill remaining slots with nodes from different hosts
            for _ in 1..replica_count {
                let mut node_assigned = false;

                // Try to find a node from a host not yet used in this group
                for host in &hosts {
                    if hosts_used_in_group.contains(host) {
                        continue; // Skip hosts already used in this group
                    }

                    if let Some(host_nodes) = nodes_by_host.get(host) {
                        for (node_idx, node) in host_nodes {
                            if !assigned_nodes[*node_idx] {
                                assigned_nodes[*node_idx] = true;
                                hosts_used_in_group.insert(host.clone());

                                let disk = node.data_dir.first().unwrap_or(&disk_default);
                                group_cells.push(NodeDiskCell {
                                    host_idx: *node_idx as i32,
                                    host: node.host.clone(),
                                    dist_idx: 0,
                                    disk: disk.clone(),
                                });

                                node_assigned = true;
                                break;
                            }
                        }
                    }

                    if node_assigned {
                        break;
                    }
                }

                // If we couldn't find a node from a new host, try any available node
                if !node_assigned {
                    for (node_idx, node) in node_sorted.iter().enumerate() {
                        if !assigned_nodes[node_idx] {
                            assigned_nodes[node_idx] = true;

                            let disk = node.data_dir.first().unwrap_or(&disk_default);
                            group_cells.push(NodeDiskCell {
                                host_idx: node_idx as i32,
                                host: node.host.clone(),
                                dist_idx: 0,
                                disk: disk.clone(),
                            });
                            break;
                        }
                    }
                }
            }

            // Only add the group if it has nodes
            if !group_cells.is_empty() {
                result.push(group_cells);
            }
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
        let mut all_members: Vec<LogGroupMember> = self
            .group_members()
            .values()
            .flat_map(|val| val.iter().cloned().collect_vec())
            .collect_vec();

        // Sort by node_id to ensure consistent ordering for txlog_service_list and -conf
        all_members.sort_by_key(|member| member.node_id);
        all_members
    }

    /// log startup command, with host as granularity, key is hostname value is start command.
    pub fn log_start_cmd(&self) -> HashMap<String, Vec<LogCmdItems>> {
        let all_member_vec = self.group_member_as_vec(); //self.group_members();
        let all_member_as_slice = all_member_vec.as_slice();
        let host_members_lookup = self.host_members(all_member_as_slice);

        // BUGFIX: GROUP_MEMBERS should contain all log service nodes in the order from grouping algorithm
        // This matches what gets written to txlog_service_list in EloqKV configuration
        // Use the same order as the grouping algorithm produces, not sorted by host
        let all_members_config = all_member_vec
            .iter()
            .map(|member| format!("{}:{}", member.member_host, member.port))
            .collect::<Vec<String>>()
            .join(",");

        host_members_lookup
            .iter()
            .map(|(host, members)| {
                let cmds = members
                    .iter()
                    .map(|log_member| {
                        // Use the full list of all log service nodes instead of just the group members
                        LogCmdItems {
                            group_members_config: all_members_config.clone(),
                            log_member: log_member.clone(),
                        }
                    })
                    .collect_vec();
                (host.to_string(), cmds)
            })
            .collect::<HashMap<String, Vec<LogCmdItems>>>()
    }

    pub fn log_directories(&self) -> HashMap<String, Vec<String>> {
        self.group_member_as_vec()
            .into_iter()
            .into_group_map_by(|log_member| log_member.member_host.clone())
            .into_iter()
            .map(|(host, members)| {
                let dirs = members
                    .into_iter()
                    .map(|log_member| log_member.storage_path)
                    .collect();
                (host, dirs)
            })
            .collect::<HashMap<String, Vec<String>>>()
    }

    /// Compose rocks cloud flags string for `launch_sv`.
    /// Example: "-aws_access_key_id=minioadmin -aws_secret_key=minioadmin \
    /// -bucket_name=eloqlogservice -endpoint_url=http://127.0.0.1:9000"
    pub fn rocks_cloud_flag(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(v) = &self.aws_access_key_id {
            if !v.is_empty() {
                parts.push(format!("-aws_access_key_id={}", v));
            }
        }
        if let Some(v) = &self.aws_secret_key {
            if !v.is_empty() {
                parts.push(format!("-aws_secret_key={}", v));
            }
        }
        if let Some(v) = &self.bucket_name {
            if !v.is_empty() {
                parts.push(format!("-bucket_name={}", v));
            }
        }
        if let Some(v) = &self.bucket_prefix {
            if !v.is_empty() {
                parts.push(format!("-bucket_prefix={}", v));
            }
        }
        if let Some(v) = &self.region {
            if !v.is_empty() {
                parts.push(format!("-region={}", v));
            }
        }
        if let Some(v) = &self.endpoint {
            if !v.is_empty() {
                parts.push(format!("-endpoint_url={}", v));
            }
        }
        if let Some(v) = &self.object_path {
            if !v.is_empty() {
                parts.push(format!("-object_path={}", v));
            }
        }
        if let Some(v) = &self.target_file_size_base {
            if !v.is_empty() {
                parts.push(format!("-target_file_size_base={}", v));
            }
        }
        if let Some(v) = &self.sst_file_cache_size {
            if !v.is_empty() {
                parts.push(format!("-sst_file_cache_size={}", v));
            }
        }
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use crate::config::log_service::{LogReadiness, LogService, LogServiceNode};
    use itertools::Itertools;
    use serde_yaml::from_str;
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
            image: None,
            nodes,
            replica: replica as u32,
            readiness: Some(LogReadiness::default()),
            bthread_concurrency: None,
            aws_access_key_id: None,
            aws_secret_key: None,
            bucket_name: None,
            endpoint: None,
            region: None,
            bucket_prefix: None,
            object_path: None,
            target_file_size_base: None,
            sst_file_cache_size: None,
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
    }

    #[test]
    pub fn test_log_service_groups_2() {
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

    #[test]
    fn test_log_service_accepts_endpoint_and_endpoint_url() {
        let endpoint_yaml = r#"
image: null
nodes:
  - host: "127.0.0.1"
    data_dir: ["/data/log"]
    port: 9400
replica: 1
endpoint: "https://obs.example.com"
"#;
        let endpoint_url_yaml = r#"
image: null
nodes:
  - host: "127.0.0.1"
    data_dir: ["/data/log"]
    port: 9400
replica: 1
endpoint_url: "https://obs.example.com"
"#;

        let endpoint_cfg: LogService = from_str(endpoint_yaml).unwrap();
        let endpoint_url_cfg: LogService = from_str(endpoint_url_yaml).unwrap();

        assert_eq!(
            endpoint_cfg.endpoint.as_deref(),
            Some("https://obs.example.com")
        );
        assert_eq!(
            endpoint_url_cfg.endpoint.as_deref(),
            Some("https://obs.example.com")
        );
        assert_eq!(
            endpoint_cfg.rocks_cloud_flag(),
            "-endpoint_url=https://obs.example.com"
        );
        assert_eq!(endpoint_cfg, endpoint_url_cfg);
    }
}
