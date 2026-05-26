# E2E Tests

这套测试只讲两种用法。

## 方式 1：用 CLI 跑完整测试

先构建本地 `eloqctl`：

```sh
cd /home/starrysky/workspace/eloqdata-kernel/eloq_waiter
cargo build -p cluster_mgr
```

统一入口：

```sh
tests/e2e/devctl.sh --help
```

最常用命令：

```sh
# 启动 Docker 环境
tests/e2e/devctl.sh env-up

# 构建 + 启动环境 + 安装到 control node + 生成 topology + launch
tests/e2e/devctl.sh full

# 查看集群和 monitor 状态
tests/e2e/devctl.sh status

# 只升级 Grafana
tests/e2e/devctl.sh grafana-update

# 运行完整 stress test
tests/e2e/devctl.sh stress

# 只跑 SDK stress，流量都在容器里，不在宿主机
tests/e2e/devctl.sh stress py-stress,go-stress,ts-stress

# 删除 Docker 环境
tests/e2e/devctl.sh env-down
```

说明：

- `devctl.sh stress` 实际调用 `tests/e2e/cmd_stress_test.sh`
- Python 压测跑在 `stress-python`
- Go 压测跑在 `stress-go`
- TypeScript 压测跑在 `stress-ts`
- `env-up` 默认复用本地已有镜像；如果你要强制重建，用 `FORCE_DOCKER_BUILD=1 tests/e2e/devctl.sh env-up`
- `env-up` 只负责启动一个新的 Docker 测试环境；control node 里的 `eloqctl` 运行目录会是全新的

## 方式 2：用 CLI 启动环境，再登录 control node 手动跑 eloqctl

### 1. 启动环境

```sh
cd /home/starrysky/workspace/eloqdata-kernel/eloq_waiter
cargo build -p cluster_mgr
tests/e2e/devctl.sh env-up
tests/e2e/devctl.sh install-control
tests/e2e/devctl.sh render-topology
```

### 2. 登录 control node

```sh
tests/e2e/devctl.sh control-shell
```

等价命令：

```sh
ssh -i tests/docker_ha/id_ed25519 -p 2224 eloq@127.0.0.1
```

control node 里的关键路径：

- 仓库：`/workspace/eloq_waiter`
- `eloqctl`：`/usr/local/bin/eloqctl`
- `ELOQCTL_HOME`：`/home/eloq/.eloqctl`
- 渲染后的 topology：`/home/eloq/topology.generated.yaml`

### 3. 手动 launch / update / status

在 control node 里执行：

```sh
eloqctl stop test-e2e --all --force || true
eloqctl remove test-e2e --force || true
eloqctl launch --skip-deps /home/eloq/topology.generated.yaml
```

查看状态：

```sh
eloqctl status test-e2e --wait 180
eloqctl monitor status --cluster test-e2e
```

手动升级 Grafana：

```sh
eloqctl monitor update --cluster test-e2e \
  --component grafana \
  --url 'https://dl.grafana.com/grafana/release/13.0.1+security-01/grafana_13.0.1+security-01_25720641773_linux_amd64.tar.gz'
```

在现有集群上安装 Alertmanager：

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

再次更新 Alertmanager：

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

在现有集群上安装 Alertmanager，并同时安装 `alertmanager-webhook-adapter`：

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz'
```

在现有集群上安装 Alertmanager，并同时启用飞书告警转发：

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz' \
  --feishu-robot-url 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
```

这条命令还会一并部署 `alertmanager-webhook-adapter`，并下发仓库内置的飞书中文模板：

- 模板语言：`zh`
- 默认签名：`EloqKV`
- 模板文件：`src/cluster_mgr/config/feishu.zh.tmpl`
- 远端部署目录：`/home/eloq/test-e2e/alertmanager-webhook-adapter/templates/feishu.zh.tmpl`

再次更新 Alertmanager，或用相同命令恢复一次失败安装：

```sh
eloqctl monitor update --cluster test-e2e \
  --component alertmanager \
  --url 'https://github.com/prometheus/alertmanager/releases/download/v0.32.1/alertmanager-0.32.1.linux-amd64.tar.gz' \
  --feishu-robot-url 'https://open.feishu.cn/open-apis/bot/v2/hook/xxx'
```

安装完后再次检查：

```sh
eloqctl monitor status --cluster test-e2e
```

如果你以前装过旧的独立 `PrometheusAlert`，想把残留进程和目录删干净，可以在 control node 里手工执行：

```sh
ssh -i /home/eloq/.ssh/id_ed25519 eloq@172.28.10.14 \
  "pkill -f '/home/eloq/test-e2e/prometheusalert/PrometheusAlert' || true; \
   rm -rf /home/eloq/test-e2e/prometheusalert"
```

这一步只用于清理旧实现残留。新的飞书告警链路部署目录是 `/home/eloq/test-e2e/alertmanager-webhook-adapter`。

导出 topology：

```sh
eloqctl export test-e2e --output /home/eloq/test-e2e-export.yaml
```

### 4. 看 monitor 网页

宿主机浏览器访问：

- Grafana: `http://127.0.0.1:13301`
- Prometheus: `http://127.0.0.1:19500`
- Alertmanager: `http://127.0.0.1:19093`，安装 `alertmanager` 后可访问
- Alertmanager Webhook Adapter: `http://127.0.0.1:18080`，安装 `alertmanager` 后可访问

Grafana 默认账号密码：

```text
admin / admin
```

也可以用命令验证：

```sh
curl -fsS http://127.0.0.1:13301/login >/dev/null
curl -fsS http://127.0.0.1:19500/-/healthy
curl -fsS http://127.0.0.1:19093/-/healthy
curl -fsS http://127.0.0.1:18080 >/dev/null
```

### 5. 删除环境

宿主机执行：

```sh
tests/e2e/devctl.sh env-down
```

## Stress Test 常用变量

`tests/e2e/cmd_stress_test.sh` 支持这些常用覆盖项：

| Variable | Default |
|----------|---------|
| `STEPS` | `launch,monitor-update,eloqctl-mutate,py-stress,go-stress,ts-stress,remove` |
| `DURATION_SECONDS` | `300` |
| `INFO_ONLY_DURATION_SECONDS` | `300` |
| `WORKERS` | `16` |
| `INFLIGHT` | `4` |
| `KEY_COUNT` | `256` |
| `CMD_TIMEOUT` | `5` |
| `TLS_ENABLED` | `1` |
| `SKIP_DEPS` | `1` |

例子：

```sh
STEPS=py-stress,go-stress,ts-stress \
  DURATION_SECONDS=15 \
  INFO_ONLY_DURATION_SECONDS=15 \
  WORKERS=4 \
  INFLIGHT=2 \
  tests/e2e/devctl.sh stress
```
