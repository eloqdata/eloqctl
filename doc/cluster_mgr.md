## MonographDB Deployment and Management CLI

### Install
```shell
bash src/cluster_mgr/install.sh
# Or
curl http://download.eloqdata.com/eloqctl/install.sh | bash
```

### Design and implementation

#### Terminology

1. Clusters
   <p>A cluster is a logical concept; a cluster includes a set of MonographDB database instances and supports multiple
   cluster installations. Cluster names must be unique.</p>
2. Command
   <p>User input commands, such as deploy, install, start, stop, etc.</p>
3. TaskExecutor
   <p>A task is an indivisible unit of execution and the smallest parallel unit of execution. A command consists of multiple task instances, and a task instance is a specific instantiated task. For example, CassandraCtlTask represents the task that controls Cassandra, while CassandraCtlTask on Host1 is the specific task instance that indicates CassandraCtlTask will run on node Host1.</p>
4. TaskGroup
   <p>Instances of the same or different types of tasks form a task group, and the tasks in the task group are executed in parallel. Commands and task groups are one-to-one.</p>
5. Parallel mechanisms

```
+ --------paralle-------- + Pause  +  ------parallel----- +

+-----+------+------+------+--------+------+-------+-------+-------+
|     |      |      |      |        |      |       |       |       |
|task1| task2| task3| task4| Barrier|task5 | task6 | task7 |       |
+-----+------+------+------+--------+------+-------+-------+-------+
```

6. Status Management
   <p>For some non-idempotent commands(deploy), if the execution of some nodes fails, if re-executed, ClusterCliMgr should skip the nodes that have been executed successfully. For this purpose, after each task is completed, the task execution state is persisted to the local state backend, currently using SQLite. Also, ClusterCliMgr persists in the cluster. It also persists in the topology description file of the cluster.</p>

### Command list

```text
Commands:
  deploy
          Deploy the MonographDB cluster by specifying the cluster_topology.yaml file
          ./cluster_mgr deploy --topology-file  ${PWD}/config/deployment.yaml

  install
          bootstrap MonographDB to generate catalog. You need to specify the cluster name.
          ./cluster_mgr install --cluster $CLUSTER_NAME

  start
          Start the MonographDB cluster(TxService LogService Storage). with the specified cluster name
          ./cluster_mgr start  --cluster $CLUSTER_NAME

  stop
          Stop the MonographDB cluster(TxService LogService Storage). with the specified cluster name.

          ./cluster_mgr stop --cluster $CLUSTER_NAME --force true|false  --all true|false

  restart
          Restart the MonographDB cluster with the specified cluster name.
          ./cluster_mgr restart --cluster $CLUSTER_NAME

  exec
          Execute custom shell commands.
          ./cluster_mgr exec --command 'ls -la /data1/' --topology-file  ${PWD}/config/deployment.yaml
  status
          Check MonographDB cluster status. If the username password is given,
           the connection to the target database is established, otherwise, the ps command is executed.
          ./cluster_mgr status --cluster $CLUSTER_NAME --user $DB_USER --password $DB_PASSWORD

  run-deps
          Install MonographDB runtime dependencies.
          ./cluster_mgr run-deps --topology-file ${PWD}/config/deployment.yaml

  monitor
          Start or stop monitoring components,including prometheus, grafana,node_exporter,mysql_exporter.
          ./cluster_mgr monitor --cluster $CLUSTER_NAME --command start | stop

  log-service
          Start or stop LogService This command is only available if LogService is deployed standalone
           ./cluster_mgr log-service --cluster $CLUSTER_NAME --command start | stop

  upgrade
          According to the deployment.yaml, update the related monograph_db cluster by stopping the cluster, replacing the package, and starting the cluster.
          ./cluster_mgr upgrade --topology_file ${PWD}/config/deployment.yaml

  update-conf
          Update the configuration file and restart the tx service (the default value of restart is true). Note: Please edit conf/my_template.cnf first
           ./cluster_mgr update-conf --cluster $CLUSTER_NAME --restart true | false
  help
          Print this message or the help of the given subcommand(s)
```

