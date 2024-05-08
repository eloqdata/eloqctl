## Start REST API Server

```shell
cd ${PATH}/EloqWaiterRest
./rest_api --config ${PWD}/config --port $PORT --addr 0.0.0.0
```

## REST API reference

The cluster manager backend expose an HTTP API, which is the same functionality as the ClusterMgrCli interface.
All current long-task API interfaces are asynchronous. For example, the deployment interface only submits the task and
queries the deployment status through the status interface.

### Deploy cluster API

```text
POST /deploy 
```

#### Request and response example

Request Body

```json
{
  "connection": {
    "username": "mono",
    "auth_type": "keypair",
    "auth": {
      "keypair": "/home/ubuntu/.ssh/id_ed25519_mono"
    }
  },
  "deployment": {
    "install_image": "http|file",
    "cluster_name": "mono-poc",
    "install_dir": "/data1/opt",
    "port": {
      "mysql_port": 3300,
      "monograph_port": {
        "start": 8100,
        "end": 8200
      }
    },
    "mono_service": {
      "host": [
        "127.0.0.1"
      ]
    },
    "storage_service": {
      "cassandra": {
        "download_url": "https://archive.apache.org/dist/cassandra/4.1.0/apache-cassandra-4.1.0-bin.tar.gz",
        "storage_cluster": "mono-cass-cluster",
        "host": [
          "127.0.0.1"
        ]
      }
    }
  }
}
```

| field                    | meaning                                                                                                                                                                                                                               |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| deployment.install_image | MonographDB installation media location currently supports HTTP and local file. Example.<br/>http://XXX/monographdb-ubuntu-release.tar.gz <br/>file:///home/ubuntu/monographdb-ubuntu-release.tar.gz file format only supports tar.gz |
| deployment.install_dir   | The installation directory for the MonographDB cluster. Read and write permissions are required.                                                                                                                                      |
| deployment.cluster_name  | The MonographDB cluster name, the name must be unique.                                                                                                                                                                                |

Always return OK, but returning success does not mean the deployment status.

```text
HTTP/1.1 200 OK
Connection: close
Content-Length: 0
Date: Mon, 06 Feb 2023 03:57:15 GMT
```

### Command Status API

```text
GET /ctl_cmd_status/{cluster}/{command}
```

The following commands are supported

- start
- stop
- install
- deploy
- status

#### Request and response example

response data

```json
{
  "code": 200,
  "data": {
    "failure": [],
    "status": "success|failure|none|progress",
    "success": []
  },
  "msg": null
}
```

| field        | meaning                                                                    |
|--------------|----------------------------------------------------------------------------|
| data.status  | - none: If the cluster does not exist or the command has not been executed |
| data.success | The list of successful tasks                                               |
| data.failure | The list of failure tasks                                                  |

The deployment command request.

```text
# Examples of querying deployment status
GET /ctl_cmd_status/mono-poc/deploy
```

The deployment command response.

```json
{
  "code": 200,
  "data": {
    "failure": [],
    "status": "success",
    "success": [
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:13",
        "host": "172.31.35.17",
        "task": "apache-cassandra-4.1.0-bin.tar.gz_unpack"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:03",
        "host": "172.31.35.17",
        "task": "cassandra_download"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:08",
        "host": "172.31.35.17",
        "task": "cassandra_upload"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:07",
        "host": "172.31.35.17",
        "task": "db_config_upload"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:07",
        "host": "172.31.35.17",
        "task": "install_monograph_script_upload"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:12",
        "host": "172.31.35.17",
        "task": "monograph_config_upload"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:07",
        "host": "172.31.35.17",
        "task": "monograph_install_db_conf_upload"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:33",
        "host": "172.31.35.17",
        "task": "monographdb-ubuntu20-release-bin.tar.gz_unpack"
      },
      {
        "cmd": "deploy",
        "cmd_datetime": "2023-02-06 07:55:02",
        "host": "172.31.35.17",
        "task": "monogrphdb_download"
      }
    ]
  },
  "msg": null
}
```

The installation command response.

```json
{
  "code": 200,
  "data": {
    "failure": [],
    "status": "progress",
    "success": [
      {
        "cmd": "install",
        "host": "172.31.35.17",
        "task": "cassandra_config_upload",
        "cmd_datetime": "2023-02-06 07:55:07"
      }
    ]
  },
  "msg": null
}
```

### Cluster Control API

```text
POST /ctl_cmd/{cluster}/{command}
```

The following commands are supported

- start
- stop
- install
- deploy

### Check cluster status API

```text
POST /cluster_status/{cluster}
```

#### Request and response example

Request Body.

```json
{
  "user": "mono",
  "password": "mono"
}
```

> The username and password for the database must be provided for the Check Cluster Status interface.
