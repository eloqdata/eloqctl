## Command Description

All commands in mono_waiter are built on top of the workspace. Because MonographDB is complicated to compile and test
during development, it is especially difficult for newcomers. So we introduce the concept of workspace in the IDE to
manage it in a standard way.

- datafarm —— Data file directory
- etc —— Configuration file directory
- Install —— Install directory. Include so files for mysqld and MonographDB
- source —— MonographDB source code
- third_party —— All dependencies at compile and runtime

```text
├── datafarm
│   ├── data_0
│   ├── data_1
│   ├── data_2
│   └── data_3
├── etc
│   ├── my-conf-3317.cnf
│   ├── my-conf-3318.cnf
│   └── my-conf-3319.cnf
├── install
│   ├── COPYING
│   ├── CREDITS
│   ├── INSTALL-BINARY
│   ├── README-wsrep
│   ├── README.md
│   ├── THIRDPARTY
│   ├── bin
│   ├── include
│   ├── lib
│   ├── man
│   ├── scripts
│   ├── share
│   ├── sql-bench
│   └── support-files
├── source
│   ├── cass
│   ├── log_service
│   ├── mariadb
│   ├── monograph
│   └── tx_service
└── third_party
    ├── apache-cassandra-4.1-alpha1
    ├── aws
    ├── braft
    ├── brpc
    ├── catch2
    └── protobuf-3.17.3
```

### config

Currently, there is the very little configuration required to run mono_waiter, configure
the ```workspace in config/common.toml```.The following points need to be specified

- If you want to git clone a specific branch, you need to configure the branch. For the git command parameters,
  configure options
- Build script only supports bash script, you can modify it yourself
- <font color="red">workspace must be an absolute path</font>

> Note: If you modify the configuration file, you need to exit monograph_waiter and re-enter the command line.

### Command list

| command         | description                                                                                                                                                                                    | idempotent | remark                                                                                                                                                                                                                                    |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| check_deps      | check compiles and runs dependencies:                                                                                                                                                          | ✅          | For specific dependencies, please refer to the file in the config/deps directory                                                                                                                                                          |
| install_deps    | Install system compilation and run dependencies if required.                                                                                                                                   | ✅          |                                                                                                                                                                                                                                           |
| setup_workspace | create a development workspace for MonographDB and download and clone the dependencies needed for compilation and runtime.                                                                     | ❌          | 1. The download is parallel, and the download speed depends on the current network status.                                                                                                                                                |
| ln_source       | The source code directory is organized according to the requirements of MonographDB. Please follow ``monographdb_engine_for_mariadb`` for details.                                             | ✅          |                                                                                                                                                                                                                                           |
| gen_mysql_cnf   | generate the MonographDB configuration files                                                                                                                                                   | ✅          |                                                                                                                                                                                                                                           |
| build_all       | Compile the MonographDB and all dependencies.                                                                                                                                                  | ✅          |                                                                                                                                                                                                                                           |
| build_monograph | Compile the MonographDB                                                                                                                                                                        | ✅          | full compilation every time                                                                                                                                                                                                               |
| playground      | 1. start the storage service and MonographDB (three nodes)<br/>2. create the database user mono with the password mono<br/>3. execute config/mysql/init.sql (create database tables and users) | ✅          | 1.``monograph_waiter`` does not guarantee that init.sql will execute successfully. The developer must ensure the script is correct.<br>2. dependency on ``init_db`` and its previous commands<br>3.sql statements must end in a semicolon |
| init_db         | Initialize the MonographDB database instance.                                                                                                                                                  | ❌          | Same as the mysql_install_db command, which can be seen in the doc [Initialize MySQL Data Directory](https://dev.mysql.com/doc/refman/5.7/en/mysql-install-db.html)                                                                       |
| stop            | stop all MonographDB services (storage services will not be stopped)                                                                                                                           | ✅          |                                                                                                                                                                                                                                           |
| start           | start all MonographDB services. If the storage service is not running, it is started.                                                                                                          | ✅          |                                                                                                                                                                                                                                           |

