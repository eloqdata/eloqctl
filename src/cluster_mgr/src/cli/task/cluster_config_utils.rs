use anyhow::anyhow;
use std::collections::HashMap;
use tracing::info;

use crate::cli::task::task_utils::{NodeGroupId, NodeId};

/// Represents a node configuration parsed from the RPC response
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub host_name: String,
    pub port: u16,
    pub is_candidate: bool,
}

/// Cluster configuration with node group information
#[derive(Debug, Clone)]
pub struct ClusterGroupConfig {
    pub node_groups: HashMap<NodeGroupId, Vec<NodeConfig>>,
    pub version: u64,
}

/// Format configuration containing node lists with proper separators
#[derive(Debug, Clone)]
pub struct FormattedNodeLists {
    pub masters_str: String,
    pub replicas_str: String,
    pub voters_str: String,
}

/// Parse the cluster config string from RPC response
pub fn parse_cluster_config(config_str: &str) -> anyhow::Result<ClusterGroupConfig> {
    let lines: Vec<&str> = config_str.lines().collect();
    let mut line_idx = 0;
    let mut ng_configs = HashMap::new();

    if lines.is_empty() {
        return Err(anyhow!("Empty cluster configuration"));
    }

    // First line contains the number of node groups
    let ng_count = lines[line_idx]
        .trim()
        .parse::<usize>()
        .map_err(|e| anyhow!("Failed to parse node group count: {}", e))?;
    line_idx += 1;

    // Process each node group
    for _ in 0..ng_count {
        if line_idx >= lines.len() {
            return Err(anyhow!("Unexpected end of config, missing node group data"));
        }

        let ng_line = lines[line_idx].trim();
        let mut parts = ng_line.split_whitespace();

        // First number in the line is the node group ID
        let ng_id = parts
            .next()
            .ok_or_else(|| anyhow!("Missing node group ID"))?
            .parse::<u32>()
            .map_err(|e| anyhow!("Failed to parse node group ID: {}", e))?;

        let mut nodes = Vec::new();

        // Process nodes in groups of 4 (node_id, host_name, port, is_candidate)
        while let Some(node_id_str) = parts.next() {
            let node_id = node_id_str
                .parse::<u32>()
                .map_err(|e| anyhow!("Failed to parse node ID: {}", e))?;

            let host_name = parts
                .next()
                .ok_or_else(|| anyhow!("Missing host name for node {}", node_id))?;

            let port_str = parts
                .next()
                .ok_or_else(|| anyhow!("Missing port for node {}", node_id))?;

            let is_candidate_str = parts
                .next()
                .ok_or_else(|| anyhow!("Missing is_candidate flag for node {}", node_id))?;

            let raw_port = port_str
                .parse::<u16>()
                .map_err(|e| anyhow!("Failed to parse port: {}", e))?;

            // Decrease port by 10000 as required - UNCONDITIONALLY
            let port = raw_port - 10000;

            info!(
                "Parsed port {} from RPC response, adjusted to {}",
                raw_port, port
            );

            let is_candidate = is_candidate_str
                .parse::<u8>()
                .map_err(|e| anyhow!("Failed to parse is_candidate flag: {}", e))?;

            let node_config = NodeConfig {
                node_id,
                host_name: host_name.to_string(),
                port,
                is_candidate: is_candidate != 0,
            };

            nodes.push(node_config);
        }

        line_idx += 1;
        ng_configs.insert(ng_id, nodes.clone());

        info!("Parsed node group {} with {} nodes", ng_id, nodes.len());
    }

    // Last line is the version
    let latest_version = if line_idx < lines.len() {
        let latest_version = lines[line_idx]
            .trim()
            .parse::<u64>()
            .map_err(|e| anyhow!("Failed to parse version: {}", e))?;

        info!(
            "Parsed cluster config with {} node groups, version {}",
            ng_configs.len(),
            latest_version
        );
        latest_version
    } else {
        return Err(anyhow!("Missing version information in cluster config"));
    };

    Ok(ClusterGroupConfig {
        node_groups: ng_configs,
        version: latest_version,
    })
}

/// Generate node lists with proper separators based on node groups
pub fn format_node_lists(cluster_group_config: &ClusterGroupConfig) -> FormattedNodeLists {
    let ng_configs = &cluster_group_config.node_groups;

    // Group masters by node group id: within group '|' and between groups ','
    let masters_str = {
        // Collect one master per node group
        let mut master_groups: Vec<(u32, Vec<String>)> = ng_configs
            .iter()
            .map(|(&ng_id, nodes)| {
                // Create a HashSet to track unique master addresses
                let mut unique_masters = std::collections::HashSet::new();

                // Only include unique masters
                let mut group = Vec::new();
                let mut found_master = false;
                for node in nodes {
                    let addr = format!("{}:{}", node.host_name, node.port);
                    if node.is_candidate && !found_master {
                        // Only add the master if we haven't seen it before
                        if unique_masters.insert(addr.clone()) {
                            group.push(addr);
                            found_master = true;
                        }
                    }
                }
                (ng_id, group)
            })
            .collect();
        master_groups.sort_by_key(|(ng_id, _)| *ng_id);
        let master_parts: Vec<String> = master_groups
            .into_iter()
            .filter_map(|(_, group)| {
                if group.is_empty() {
                    None
                } else {
                    Some(group.join("|"))
                }
            })
            .collect();
        master_parts.join(",")
    };

    // Group replicas by node group id: within group '|' and between groups ','
    let replicas_str = {
        // Skip first candidate (master), collect remaining candidates as replicas
        let mut replica_groups: Vec<(u32, Vec<String>)> = ng_configs
            .iter()
            .map(|(&ng_id, nodes)| {
                // Create a HashSet to track unique replica addresses
                let mut unique_replicas = std::collections::HashSet::new();

                // Only collect unique replicas
                let replicas: Vec<String> = nodes
                    .iter()
                    .filter(|node| node.is_candidate)
                    .skip(1)
                    .map(|node| format!("{}:{}", node.host_name, node.port))
                    .filter(|addr| unique_replicas.insert(addr.clone())) // Only keep addresses we haven't seen before
                    .collect();
                (ng_id, replicas)
            })
            .collect();
        replica_groups.sort_by_key(|(ng_id, _)| *ng_id);
        let replica_parts: Vec<String> = replica_groups
            .into_iter()
            .filter_map(|(_, group)| {
                if group.is_empty() {
                    None
                } else {
                    Some(group.join("|"))
                }
            })
            .collect();
        replica_parts.join(",")
    };

    // Group voters by node group id: within group '|' and between groups ','
    let voters_str = {
        let mut voter_groups: Vec<(u32, Vec<String>)> = ng_configs
            .iter()
            .map(|(&ng_id, nodes)| {
                // Create a vector to track unique voter addresses
                let mut unique_voters = std::collections::HashSet::new();

                // Only collect unique voters
                let voters: Vec<String> = nodes
                    .iter()
                    .filter(|node| !node.is_candidate)
                    .map(|node| format!("{}:{}", node.host_name, node.port))
                    .filter(|addr| unique_voters.insert(addr.clone())) // Only keep addresses we haven't seen before
                    .collect();

                (ng_id, voters)
            })
            .collect();
        voter_groups.sort_by_key(|(ng_id, _)| *ng_id);
        let voter_parts: Vec<String> = voter_groups
            .into_iter()
            .filter_map(|(_, group)| {
                if group.is_empty() {
                    None
                } else {
                    Some(group.join("|"))
                }
            })
            .collect();
        voter_parts.join(",")
    };

    FormattedNodeLists {
        masters_str,
        replicas_str,
        voters_str,
    }
}
