-- deployment database schema
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
create table if not exists t_service_instance
(
    service_instance_id integer      not null primary key AUTOINCREMENT,
    cluster_name        varchar(200) not null,
    service_name        varchar(60)  not null,
    service_status      integer      not null, -- 0:available,1:unavailable,2:stop,3:not-running,
    current_config      integer,
    host                varchar(100) not null,
    create_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    timestamp    not null DEFAULT CURRENT_TIMESTAMP
);
create table if not exists t_service_config
(
    config_id        integer     not null primary key AUTOINCREMENT,
    service_name     varchar(60) not null,
    config           text,
    is_enable        integer     not null, -- 0:enable 0:disable
    create_timestamp timestamp   not null DEFAULT CURRENT_TIMESTAMP,
    update_timestamp timestamp   not null DEFAULT CURRENT_TIMESTAMP
);