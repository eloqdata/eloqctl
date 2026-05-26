# Upgrades

## Upgrade `eloqctl`

Install the desired release tag with the installer:

```sh
curl -fsSL https://raw.githubusercontent.com/monographdb/eloq_waiter/main/install.sh | sh -s -- v1.6.7
```

For local development builds, reinstall from the current checkout:

```sh
scripts/install-dev.sh
```

The release installer now runs `eloqctl upgrade` automatically when it detects
an existing local state directory, so previously registered clusters remain
visible after an upgrade.

## Upgrade Local State Schema

Run the SQLite schema upgrade command after installing a newer `eloqctl` if local state needs migration:

```sh
eloqctl upgrade
```

Current state storage keeps launch-compatible topology YAML under `$ELOQCTL_HOME/clusters/<cluster>/topology.yaml` and stores only a cluster index plus operational metadata in SQLite.

## Upgrade EloqKV Cluster Version

Upgrade an existing EloqKV cluster to a specific version:

```sh
eloqctl update <cluster> <version>
```

`eloqctl` now resolves available versions and EloqKV tarballs from GitHub Releases for `eloqdata/eloqkv`. For example, asset `eloqkv-1.2.2-rocks_s3-ubuntu24-amd64.tar.gz` is treated as version `1.2.2` for store `rocks_s3`.

Download the required tarballs into the local cache without applying the update:

```sh
eloqctl update <cluster> <version> --download-only
```

`--download-only` still resolves the version against the saved cluster topology, so provide the existing cluster name and the target version exactly as you would for a real upgrade. It downloads the matching EloqKV release tarballs into `$ELOQCTL_HOME/download/...` and exits without changing remote files, processes, or cluster metadata.

Typical workflow:

```sh
eloqctl versions
eloqctl update <cluster> 1.2.2 --download-only
eloqctl update <cluster> 1.2.2
```

Update only one monitor component without touching the EloqKV cluster itself:

```sh
eloqctl monitor update --cluster <cluster> --component grafana
```

Override the monitor tarball URL for a monitor-only update:

```sh
eloqctl monitor update --cluster <cluster> --component grafana --url https://dl.grafana.com/oss/release/grafana-11.0.0.linux-amd64.tar.gz
```

If the selected monitor component is configured in topology but not currently deployed or running, `eloqctl monitor update` still downloads, unpacks, uploads config, and starts it.

GitHub-hosted EloqKV tarballs are now validated with the asset `sha256` digest when available. Cached files are reused only when the digest still matches, so a re-published asset under the same tag can be refreshed correctly.

Upgrade to the latest available version:

```sh
eloqctl update <cluster> latest
```

Use `--force` only when graceful shutdown is impossible or the cluster is already down:

```sh
eloqctl update <cluster> <version> --force
```

Use `eloqctl status <cluster> --wait 60` after an upgrade to verify live health.
