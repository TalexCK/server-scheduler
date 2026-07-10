# Server Scheduler

`server-scheduler` 是运行在宿主机上的 Minecraft 子服务器调度器。它根据
`servers/<server-id>.json` 组装临时实例、选择 Java 21 或 Java 25、分配端口、
启动服务端进程，并通过仅监听 `127.0.0.1` 的 HTTP API 提供管理和 Bridge 通信。

Scheduler 自身以后台进程运行，不提供持续交互式控制台。服务端命令、日志和实例
状态通过 CLI 或 HTTP API 访问。

## 构建

需要 Rust 工具链：

```bash
cargo build --release --locked
```

生成的可执行文件位于 `target/release/server-scheduler`。

## 配置

默认读取当前工作目录下的 `config.json`，也可以通过全局参数指定路径：

```bash
./server-scheduler --config /path/to/config.json start
```

所有相对路径都以 `config.json` 所在目录为基准。完整配置结构如下：

```json
{
  "api": {
    "port": 25566,
    "token": "replace-with-an-admin-token-of-at-least-32-characters"
  },
  "bridge": {
    "token": "replace-with-a-bridge-token-of-at-least-32-characters",
    "heartbeat_timeout_seconds": 45,
    "transfer_retry_seconds": 30,
    "transfer_timeout_seconds": 45,
    "transfer_max_attempts": 2
  },
  "postgresql": {
    "home": "/path/to/postgresql",
    "host": "127.0.0.1",
    "port": 5432,
    "username": "minigames",
    "password": "replace-with-a-strong-password"
  },
  "ports": {
    "velocity": 25565,
    "start": 50001,
    "end": 65535
  },
  "enable-frpc": false,
  "frpc-core": "frpc",
  "java": {
    "21_home": "/path/to/zulu-21",
    "25_home": "/path/to/zulu-25"
  },
  "paths": {
    "cores": "cores",
    "plugins": "plugins",
    "mods": "mods",
    "dependencies": "dependencies",
    "files": "files",
    "network": "network",
    "servers": "servers",
    "worlds": "worlds",
    "runtime": ".runtime",
    "logs": "logs",
    "pid_file": "scheduler.pid",
    "scheduler_log": "scheduler.log"
  }
}
```

`api.token` 与 `bridge.token` 都必须至少包含 32 个字符。PostgreSQL 主目录、连接
参数以及 Java 21、Java 25 的 Home 均由此文件统一配置。

Velocity 使用 `ports.velocity` 的固定入口端口。普通子服务器从 `ports.start` 开始
选择第一个未被当前 Scheduler 实例或宿主机其他进程占用的端口，并在进程退出后释放；默认范围是
`50001-65535`。Velocity 端口必须位于子服务器端口范围之外。

`enable-frpc` 默认为 `false`。设为 `true` 后，Scheduler 会从
`paths.network/frpc-core` 启动 FRPC，并固定传入 `-c ./frpc.toml`。默认组合等价于在
`network/` 中执行 `./frpc -c ./frpc.toml`。FRPC 意外退出后每 5 秒重启一次，Scheduler
停止时也会停止该进程。

## 资源目录

Scheduler 使用以下目录布局：

```text
cores/<core-kind>/<minecraft-version>/
plugins/<plugin-id>/<minecraft-compatibility>/
mods/<mod-id>/<minecraft-compatibility>/
dependencies/objects/<sha256-prefix>/<sha256>
dependencies/manifests/servers/<server-id>.json
files/<file-layer>/
network/frpc
network/frpc.toml
servers/<server-id>.json
worlds/<world-id>/
```

`network/frpc` 是目标宿主机平台的可执行文件，`network/frpc.toml` 是其运行配置。
只有启用 FRPC 时才要求这两个文件存在。

插件和模组的第二层目录表示 Minecraft 兼容版本，不是插件或模组自身版本。例如：

```text
plugins/luckperms/1.21.11/
plugins/scheduler-bridge/1.21.11-26.2/
plugins/viaversion/common/
mods/fabric-api/1.21.11/
```

服务器定义中的插件和模组条目通常只写 `id`，兼容目录按以下顺序自动解析：

Core 自带的 `libraries/`、`versions/`、`cache/` 和 `.fabric/` 会随 core 组装。插件或
模组在运行期间新增的 library、cache、remap 文件会在进程退出后写入内容寻址缓存。
相同 SHA-256 的文件只保存一个 object；实例创建时按服务器 manifest 恢复，优先使用
硬链接，跨文件系统时回退为复制。客户端 `assets/` 不属于专用服务端启动依赖。

1. 与 `core.minecraft_version` 完全相同的目录
2. 包含该 Minecraft 版本的兼容范围目录
3. `common` 目录

若多个范围同时匹配，会优先选择边界更接近目标版本的范围。部署资源应保证同一个
Minecraft 版本下，同一个 core、plugin 或 mod 只保留一份。

可选的 `version` 字段只用于固定一个 Minecraft 兼容目录名，不表示插件或模组自身
版本。

插件的可选配置模板位于 `plugins/<plugin-id>/<config-profile>/`。模组配置模板位于
`mods/<mod-id>/<config-profile>/`，并可通过服务器定义选择复制到 `config/` 或
`mods/<mod-id>/`。

## 服务器定义

`servers/<server-id>.json` 描述一个可启动服务。文件名去掉 `.json` 后就是 CLI 和
API 使用的服务器 ID。

```json
{
  "core": {
    "kind": "paper",
    "minecraft_version": "1.21.11",
    "java": "21",
    "jar": "paper.jar"
  },
  "memory": {
    "initial_mib": 2048,
    "maximum_mib": 4096
  },
  "autostart": false,
  "restart_delay_seconds": 5,
  "idle_timeout_seconds": 300,
  "gamevoting": {
    "order": 10,
    "id": "example",
    "name": "&b&lExample Game",
    "description": [
      "&7Example description"
    ],
    "material": "DIAMOND_SWORD",
    "custom_model_data": 0,
    "min_version": "1.21.11",
    "max_version": "26.2",
    "min_players": 2,
    "max_players": 16
  },
  "plugins": [
    {
      "id": "scheduler-bridge"
    }
  ],
  "mods": [],
  "world": {
    "mode": "template",
    "id": "example-world"
  },
  "files": [
    {
      "source": "example-server",
      "target": "."
    }
  ],
  "render_files": [],
  "properties": {
    "max-players": "16",
    "resource-pack": "https://example.invalid/resource-pack.zip",
    "resource-pack-sha1": "0000000000000000000000000000000000000000",
    "require-resource-pack": "true"
  },
  "environment": {},
  "jvm_args": [],
  "server_args": [],
  "nogui": true,
  "startup_timeout_seconds": 180
}
```

服务器定义不包含端口字段，也不应在 `properties` 中维护 `server-port`。Scheduler
在启动时分配端口，并最后写入临时实例的 `server.properties`。分配结果同时通过
`SCHEDULER_SERVER_PORT` 传给子进程。除 Velocity 外，Scheduler 会在最后强制写入
`server-ip=127.0.0.1`，服务器定义和已有文件都无法覆盖该绑定。

`world.mode` 支持：

- `generated`：不复制世界模板，由服务端生成新世界。
- `template`：将 `worlds/<world-id>/` 的内容复制到临时实例。

材质包 URL、SHA-1 和是否强制下载由当前服务器的 `properties.resource-pack`、
`properties.resource-pack-sha1` 与 `properties.require-resource-pack` 配置，不放在全局
配置中。

可投票子服通过可选的 `gamevoting` 节点进入 Scheduler 游戏目录。`order` 控制菜单顺序，
`id` 是投票命令使用的游戏 ID，启动用的服务器 ID 直接取当前 JSON 文件名；显示文本、
图标、客户端版本范围和人数范围都只在此处维护。Scheduler 会拒绝重复游戏 ID、非法
材质标识以及无效人数范围。

`render_files` 中列出的文本文件会在实例准备阶段替换以下变量：

```text
${SERVER_ID}
${INSTANCE_ID}
${SERVER_PORT}
${POSTGRES_HOST}
${POSTGRES_PORT}
${POSTGRES_USER}
${POSTGRES_PASSWORD}
${RESOURCE_PACK_URL}
${RESOURCE_PACK_SHA1}
```

材质包变量从服务器定义的 `properties` 获取。渲染完成后仍存在未识别变量会导致实例
准备失败。

## 实例生命周期

每次启动都会在 `.runtime/` 创建独立工作目录，并按顺序合并 core、插件、模组、
世界模板和文件层。Scheduler 写入 `eula.txt`、合并 `server.properties`，完成模板
渲染后使用服务器定义选择的 Java 启动进程。

进程退出后，Scheduler 先回收运行期新增的共享依赖，再立即删除该实例的整个临时工作
目录。Scheduler 启动时也会清空 `.runtime/` 中的历史残留，完整控制台输出只保留在
独立的 `logs/` 目录中。

Scheduler 自动向子进程注入：

```text
SCHEDULER_BRIDGE_URL
SCHEDULER_BRIDGE_TOKEN
SCHEDULER_SERVER_ID
SCHEDULER_INSTANCE_ID
SCHEDULER_SERVER_PORT
```

实例启动后保持 `Starting` 状态。游戏服 Bridge 上报 READY 后切换为 `Ready`，随后
才会创建待执行的玩家传送任务。Bridge 每 10 秒发送心跳；超过配置的启动或心跳
超时时间，实例会被停止并标记为失败。

启用 `autostart` 的服务器由后台监控自动启动。非自动启动服务器设置
`idle_timeout_seconds` 后，会在无人连接达到指定秒数时停止。

## CLI

启动 Scheduler 后台进程：

```bash
./server-scheduler start
```

直接以前台模式运行守护进程：

```bash
./server-scheduler daemon
```

启动一个服务器，并提供待传送玩家列表：

```bash
./server-scheduler start skywars --player \
  00000000-0000-0000-0000-000000000001 \
  00000000-0000-0000-0000-000000000002
```

`--player` 是列表参数，可接收多个值，也可以使用逗号分隔；`--players` 是同一参数的
别名。

其他管理命令：

```bash
./server-scheduler list
./server-scheduler log skywars --lines 200
./server-scheduler command skywars list
./server-scheduler terminate skywars
./server-scheduler stop
./server-scheduler restart
```

`command` 会将命令写入目标进程的标准输入，并返回随后短时间内收到的服务端输出。
Scheduler 日志写入 `scheduler.log`，子服务器日志写入 `logs/`。

## HTTP API

HTTP 服务固定绑定 `127.0.0.1:<api.port>`。健康检查无需认证：

```text
GET /health
```

管理接口使用 `Authorization: Bearer <api.token>`，JSON 请求和响应：

```text
GET  /admin/v1/servers
GET  /admin/v1/players
POST /admin/v1/servers/<server-id>/launch
POST /admin/v1/servers/<server-id>/terminate
POST /admin/v1/servers/<server-id>/restart
GET  /admin/v1/servers/<server-id>/logs?lines=100
POST /admin/v1/servers/<server-id>/command
POST /admin/v1/system/shutdown
```

Bridge 接口使用 `Authorization: Bearer <bridge.token>`。Bridge POST 请求采用
`application/x-www-form-urlencoded`，实例与待传送任务列表采用 TSV 文本：

```text
POST /bridge/v1/ready
POST /bridge/v1/heartbeat
POST /bridge/v1/idle
POST /bridge/v1/players
GET  /bridge/v1/players
GET  /bridge/v1/definitions
GET  /bridge/v1/games
GET  /bridge/v1/servers
POST /bridge/v1/servers/<server-id>/launch
POST /bridge/v1/servers/<server-id>/transfers
POST /bridge/v1/servers/<server-id>/terminate
POST /bridge/v1/servers/<server-id>/restart
GET  /bridge/v1/servers/<server-id>/logs?lines=100
POST /bridge/v1/servers/<server-id>/command
GET  /bridge/v1/transfers
POST /bridge/v1/transfers/result
```

传送失败后，Scheduler 会按照 `bridge.transfer_retry_seconds` 重新提供任务，但只在
`bridge.transfer_timeout_seconds` 内且不超过 `bridge.transfer_max_attempts`。玩家已经
离线、目标实例退出或任务过期时会立即终止该任务，旧任务不会在玩家重连后再次触发。
子服务器定义的 `idle_timeout_seconds` 会作为
`SCHEDULER_IDLE_TIMEOUT_SECONDS` 注入进程。Bridge 仅在连续空服达到该时长后调用
`/bridge/v1/idle`；Scheduler 校验实例 UUID、READY 状态、非自动启动实例和非空超时
配置后异步关服。常驻 Proxy 与 Lobby 的值为 `null`，不会被空服通知关闭。

## 安全与运行数据

- 不要将 HTTP 服务转发到外网。
- 为管理 API、Bridge 和 PostgreSQL 配置不同的强密码或 Token。
- `config.json` 包含敏感数据，应限制文件权限和仓库可见性。
- `.runtime/`、`logs/`、PID、运行日志和构建产物不应提交到版本库。
