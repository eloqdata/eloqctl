-- deployment database schema
-- Migration: drop legacy t_scale_tx_nodes (scale state tracking removed)
drop table if exists t_scale_tx_nodes;
create table if not exists t_cluster_index
(
    cluster_name      varchar(200) not null primary key,
    topology_path     text         not null,
    host_list         text         not null,
    create_timestamp  timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp  timestamp    not null DEFAULT CURRENT_TIMESTAMP
);
create table if not exists t_deployment
(
    cluster_name      varchar(200) not null primary key,
    deployment_config text         not null,
    host_list         text         not null,
    create_timestamp  timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp  timestamp    not null DEFAULT CURRENT_TIMESTAMP
);
create table if not exists t_task_status (
    cluster_name   varchar(200)  not null,
    task           text          not null,
    command        varchar(20)   not null,
    task_host      varchar(240)  not null, -- local or remote_host
    task_status    integer       not null, -- 0:success,1:failure
    create_timestamp timestamp   not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp timestamp   not null DEFAULT CURRENT_TIMESTAMP,
    primary key (cluster_name, task, command, task_host)
);
drop table if exists t_service_instance;
drop table if exists t_service_config;
create table if not exists t_snapshot_info
(
    cluster_name        varchar(200) not null,
    snapshot_ts         timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    snapshot_status     integer      not null, -- 0:available,1:deleted,2:creating,
    snapshot_path       varchar(500) not null,
    dest_host           varchar(100) not null,
    dest_user           varchar(100) not null,
    primary key (cluster_name, snapshot_ts)
);
create table if not exists t_topology_tx
(
    cluster_name        varchar(200) not null,
    node_group_count    integer      not null,
    node_group_id       integer      not null,
    node_id             integer      not null,
    role                integer      not null,
    host                varchar(100) not null,
    port                integer      not null,
    ini_config          json         not null DEFAULT '{}', -- store as json to make add/remove fields easier
    create_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    primary key (cluster_name, node_group_id, host, port)
);
create table if not exists t_topology_log
(
    cluster_name        varchar(200) not null,
    node_group_count    integer      not null,
    node_group_id       integer      not null,
    node_id             integer      not null,
    host                varchar(100) not null,
    port                integer      not null,
    data_dirs           text,
    create_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    primary key (cluster_name, host, port)
);
