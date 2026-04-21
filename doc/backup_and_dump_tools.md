# Backup management

You can use `eloqctl` to create and manage backup of current EloqKV cluster.

## Create backup

Create a cluster backup and save it at given path on a specified node (for local storage) or in S3 (for cloud storage).

```
# For local storage (path required)
eloqctl backup ${cluster_name} start --path /path/to/backup

# For cloud (S3) storage (path optional)
eloqctl backup ${cluster_name} start
```

Options:

* **\--path**:  
The full path to where the backup is stored. **Required for local storage, optional for cloud (S3) storage.** When using cloud storage, the backup is automatically stored in S3 and this option can be omitted.
* **\--dest-user**:  
User of the destination node where the backup is stored. _(default: current user)_  
**Note**: Not used for cloud storage.
* **\--dest-node**:  
Node address where the backup is stored. If you want to convert backups to AOF or RDB later, this node must be on of the tx server nodes. _(default: current node)_  
**Note**: Not used for cloud storage.
* **\--password**:  
Cluster password if set _(default: "")_

## List backup of a cluster

List current available backups of a cluster.

```
eloqctl backup ${cluster_name} list
```

The output includes:
- Cluster name
- Snapshot timestamp
- Snapshot path (manifest filename for cloud backups, file path for local backups)
- Destination host (empty for cloud backups)
- Destination user (empty for cloud backups)
- Storage type (cloud (S3) or local)

## Cleanup backup of a cluster

```
eloqctl backup ${cluster_name} remove [OPTIONS]
```

If no option is provided, remove will delete all backups of the current cluster.  
Options:

* **\--until <PERIOD>**:  
Deletes all snapshots older than the specified period. Accepted formats:
- '2 days'
- '15h'
- '1 week'
- '3 months'
- '1y 6mo 2w 4d 3h 5m 7s'

See https://docs.rs/humantime/latest/humantime/fn.parse_duration.html for more details.

* **\--before <TIMESTAMP>**:  
Deletes all snapshots created before this timestamp. Accepted formats:
- RFC 3339: '2024-11-14T15:01:00Z'
- 'YYYY-MM-DD HH:MM:SS' (assumed local time zone)
- 'YYYY-MM-DDTHH:MM:SS' (assumed local time zone)

* **\--force**:  
Force deletion: Delete records from metadata table regardless of S3/file deletion result. When this option is used, the backup records in the metadata table (`t_snapshot_info`) will be removed even if the actual backup file deletion (from S3 or local filesystem) fails. This is useful for cleaning up orphaned records or when files are already deleted. By default (without `--force`), metadata records are only deleted after successful file deletion.

**Behavior:**
- **Without `--force`** (default): Metadata records are only deleted after successful S3 or filesystem deletion. If deletion fails, the record remains in the database.
- **With `--force`**: Metadata records are deleted regardless of file deletion result. Useful for cleaning up orphaned records.

## Restore backup of a cluster

**Note**: This command is only supported for cloud storage (S3) backups. Local storage backups cannot be restored using this command.

Restore a cluster to a previous snapshot state. This operation will override the current database data with the snapshot data.

```
eloqctl backup ${cluster_name} restore --snapshot-ts <TIMESTAMP>
```

**Prerequisites:**
- The cluster must be stopped before performing restore. Use `eloqctl stop ${cluster_name} --all` to stop the cluster first.
- The snapshot must exist in the metadata table. Use `eloqctl backup ${cluster_name} list` to see available snapshots.
- The snapshot must have status "Finished" (status: 0).
- The snapshot must be for cloud storage (S3), not local storage.

Options:

* **\--snapshot-ts <TIMESTAMP>**:  
The timestamp of the snapshot to restore. Must match a snapshot timestamp from `eloqctl backup ${cluster_name} list`. Accepted formats:
- RFC 3339: '2024-11-14T15:01:00Z'
- 'YYYY-MM-DD HH:MM:SS' (assumed UTC)
- 'YYYY-MM-DDTHH:MM:SS' (assumed UTC)

**Example:**
```
eloqctl backup eloqkv-cluster restore --snapshot-ts 2024-11-14T15:01:00Z
```
**Warning:** This operation will permanently override the current database state with the snapshot data. Make sure you have a backup of the current state if needed.

**Error Cases:**
- If the cluster is running, the command will fail with an error message instructing you to stop the cluster first.
- If the snapshot is not found, the command will fail with an error message suggesting to use `eloqctl backup ${cluster_name} list` to see available snapshots.
- If the snapshot is for local storage, the command will fail with an error message indicating that only cloud storage snapshots are supported.
- If the snapshot status is not "Finished", the command will fail with an error message indicating that only finished snapshots can be restored.

## Convert existing backup to AOF file

```
eloqctl backup ${cluster_name} dump-aof [OPTIONS] --rocksdb-path <ROCKSDB_PATH> --output-file-dir <OUTPUT_FILE_DIR>
```

**Note**: This command is only supported for local storage backups. Cloud (S3) storage backups cannot be converted to AOF files at this time.

eloqctl will convert a previous backup in this cluster to AOF files. AOF files will be written to the same node where the backup is stored.  
Options:  
\-**\--rocksdb-path**:  
Path to the backup location. Must match one of the backup path returned in `eloqctl backup list`. Only local backup paths are supported.  
\-**\--output-file-dir**:  
Path where the AOF files will be written to.  
\-**\--thread-count**:  
Worker thread count for converting backup to AOF. Each worker will consume 1 vcpu on the target node. _(default:1)_

## Convert existing backup to RDB file

```
eloqctl backup ${cluster_name} dump-rdb [OPTIONS] --rocksdb-path <ROCKSDB_PATH> --output-file-dir <OUTPUT_FILE_DIR>
```

**Note**: This command is only supported for local storage backups. Cloud (S3) storage backups cannot be converted to RDB files at this time.

eloqctl will convert a previous backup in this cluster to RDB files. RDB file will be written to the same node where the backup is stored.  
Options:  
\-**\--rocksdb-path**:  
Path to the backup location. Must match one of the backup path returned in `eloqctl backup list`. Only local backup paths are supported.  
\-**\--output-file-dir**:  
Path where the RDB file will be written to.  
\-**\--thread-count**:  
Worker thread count for converting backup to RDB. Each worker will consume 1 vcpu on the target node. _(default:1)_

## Example of Dumping Data from EloqKV and Importing to Other Servers

**Note**: This example uses local storage. Dump commands (dump-aof and dump-rdb) are not supported for cloud (S3) storage backups.

1. Dump data:  
```  
eloqctl backup eloqkv-cluster start --path /data/backup  
```
2. After the backup is created, check available backups.  
```  
eloqctl backup eloqkv-cluster list  
available snapshots: [  
 (  
     "eloqkv-cluster",  
     2024-12-04T10:02:36.165807800Z,  
     "/data/backup/eloqkv-cluster/2024-12-04-10-02-36",  
     "172.31.42.205",  
     "ubuntu",
     "local",
 ),  
]  
```
3. Convert backup to AOF file.  
```  
eloqctl backup eloqkv-cluster dump-aof --rocksdb-path /data/backup/eloqkv-cluster/2024-12-04-10-02-36 --output-file-dir /home/workspace/output_aof  
```
4. Check AOF files  
```  
redis-check-aof /home/workspace/output_aof/0.aof  
```  
The output will look like:  
```  
AOF analyzed: size=411068632, ok_up_to=411068632, diff=0  
AOF is valid  
```
5. Import the AOF files to another server using `redis-cli`:  
```  
redis-cli --pipe < /home/workspace/output_aof/0.aof  
```  
After importing, the output will look like this:  
```  
All data transferred. Waiting for the last reply...  
Last reply received from server.  
errors: 0, replies: 6567541  
```
6. Remove previous snapshot

```
eloqctl backup eloqkv-cluster remove --until 1min
```

Or with force option to clean up orphaned records:

```
eloqctl backup eloqkv-cluster remove --until 1min --force
```
