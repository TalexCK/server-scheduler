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
  "public-database": {
    "enabled": false,
    "url": "postgresql://scheduler:password@public-db.example.com:5432/scheduler_web?sslmode=require",
    "publish_interval_seconds": 5
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

`public-database.enabled` 默认为 `false`。启用后必须提供公网 PostgreSQL URL；Scheduler
会在启动时建表，并按 `publish_interval_seconds` 将服务器定义、运行实例和玩家位置作为
一个事务快照发布。连接或初次发布失败时 Scheduler 会拒绝启动，避免 Web 长期展示过期
数据；运行期间的短暂同步失败会记录错误并在下一周期重试。Scheduler 停止时会清空实例
和在线玩家表，但保留服务器目录与排行榜数据。

Scheduler 与 `scheduler-web` 不直接调用彼此的 HTTP API，唯一共享边界是以下四张表：

- `scheduler_server_definitions`：可配置服务器及公开游戏目录信息。
- `scheduler_server_instances`：当前启动的实例、状态、端口、心跳和在线人数。
- `scheduler_players`：当前在线玩家、延迟及所在服务器。
- `scheduler_leaderboard_entries`：结构化排行榜读模型。Bingo Stats Mod 通过受 Bridge
  Token 保护的 `POST /bridge/v1/leaderboards` 提交月榜与总榜完成用时，Scheduler
  校验后写入；Web 只读此表。Bingo 的 `score` 单位为毫秒，数值越小排名越高。

公网数据库账号应只授予这四张表及其索引所需权限。Web 后端应使用单独的只读账号，
并通过环境变量注入连接 URL，不应复用 Scheduler 的写入账号。

Velocity 使用 `ports.velocity` 的固定入口端口。普通子服务器从 `ports.start` 开始
选择第一个未被当前 Scheduler 实例、宿主机其他进程或端口复用冷却占用的端口。子服
进程退出后立即释放占用，但该端口会进入 30 秒进程内冷却，期间新实例直接选择下一个
可用端口，不等待冷却结束。默认范围是 `50001-65535`。Velocity 端口必须位于子服务器
端口范围之外，且不进入复用冷却；Scheduler 整体重启不会持久化冷却状态。

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
  "daily_restart": {
    "hour": 4,
    "minute": 0,
    "empty_grace_seconds": 300
  },
  "idle_timeout_seconds": 300,
  "gamevoting": {
    "order": 10,
    "id": "example",
    "name": "&b&lExample Game",
    "description": [
      "&7Example description"
    ],
    "rules": [
      "First rule",
      "Second rule"
    ],
    "aliases": [
      "example-game"
    ],
    "material": "DIAMOND_SWORD",
    "custom_model_data": 0,
    "min_version": "1.21.11",
    "max_version": "26.2",
    "min_players": 2,
    "max_players": 16,
    "solo": false,
    "solo_mode": "shared",
    "solo_startup": "on_demand",
    "solo_max_players": 2,
    "solo_retention_days": 10
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
规则、别名、图标、客户端版本范围和人数范围都只在此处维护。Scheduler 会拒绝重复游戏
ID、非法别名、非法材质标识以及无效人数范围。

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
SCHEDULER_LEADERBOARD_ENABLED
SCHEDULER_SERVER_ID
SCHEDULER_INSTANCE_ID
SCHEDULER_SERVER_PORT
```

`SCHEDULER_LEADERBOARD_ENABLED` 与 `public-database.enabled` 保持一致。值为 `false`
时，游戏统计模组不得向排行榜 Bridge API 提交数据。

实例启动后保持 `Starting` 状态。游戏服 Bridge 上报 READY 后切换为 `Ready`，随后
才会创建待执行的玩家传送任务。Bridge 每 10 秒发送心跳；超过配置的启动或心跳
超时时间，实例会被停止并标记为失败。

启用 `autostart` 的服务器由后台监控自动启动。非自动启动服务器设置
`idle_timeout_seconds` 后，会在无人连接达到指定秒数时停止。

`daily_restart` 使用宿主机本地时区。到达 `hour` 和 `minute` 指定的每日时间时，
若服务器无人在线则立即重启；若仍有玩家，则等待全员退出并连续空服
`empty_grace_seconds` 后重启。空服等待期间有玩家重新进入会重新计时。当天计划时间后
新启动或已手动重启的实例不会重复执行当天的计划重启。需要长期运行的服务器同时设置
`autostart: true`。

`operator_players` 默认为 `false`。启用后，Scheduler 会在每次创建传送任务前，根据
Velocity 在线状态把玩家 UUID 解析为合法用户名，并先向目标子服写入 `op <username>`；
写入失败、玩家已离线或用户名不合法时不会传送。该选项只应在已由 Solo roster 隔离、且
地图功能确实要求玩家拥有服务端操作权限的实例中启用。

`gamevoting.solo` 默认为 `false`。启用后，`solo_mode` 支持 `shared` 和
`player_world`，`solo_startup` 支持 `always` 和 `on_demand`，默认分别为 `shared`
和 `on_demand`。`solo_max_players` 限制单次启动请求人数；请求还必须同时满足游戏的
`min_players` 与 `max_players`。`player_world` 仅允许 1 或 2 人，并强制使用
`on_demand`、关闭服务器级 `autostart`，且 `level-name` 必须为 `world`。

`shared` 使用服务器定义对应的开放静态后端，不冻结玩家名单。`always` 会让 Scheduler
保持后端运行，`on_demand` 在首个请求时启动。任意在线玩家都可通过 Solo 请求启动或加入
同一实例，每次请求中的玩家列表只用于该次传送。组合启动接口会原子串行化启动与传送，
并发请求不会相互覆盖。

`player_world` 为每份名单创建独立动态实例、端口和 Velocity 服务器 ID。地图母版必须放在
`worlds/<game-id>/world`，持久副本位于
`worlds/<game-id>/<owner-uuid>/world`，旁边的 `config.json` 保存游戏、所有者、冻结
名单和创建、更新、过期时间。同一游戏中每名玩家最多占用一个世界；双人名单共享一份，
名单内任一在线成员都能复用原所有者目录，不要求另一名成员同时在线；只有本次请求中在线
且属于冻结名单的玩家会被传送，名单外玩家始终被拒绝。实例退出时先以 staging 和原子
替换方式回写世界，再删除临时运行目录；`session.lock` 不会进入持久副本。

成功创建、启动和退出都会把过期时间延长 `solo_retention_days`，默认 10 天。Scheduler
每天使用宿主机本地时区在 04:00 清理已过期且未运行的玩家世界；04:00 后启动时会补跑
当天清理，运行中的世界不会被删除。

玩家主动销毁时，Scheduler 会先等待运行实例完整停服并回写，再删除持久目录和会话。
销毁任务独立于 HTTP 客户端连接存活，重复请求和目录已经不存在均按成功处理，避免网络
超时后留下不可重试的半完成状态。

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
GET  /admin/v1/solo/sessions
POST /admin/v1/solo/sessions
POST /admin/v1/solo/sessions/<session-id>/start
POST /admin/v1/solo/sessions/<session-id>/destroy
POST /admin/v1/system/shutdown
```

Bridge 接口使用 `Authorization: Bearer <bridge.token>`。Solo 会话的查询、创建和销毁
管理接口保留 JSON；指定会话启动和其他 Bridge POST 请求采用
`application/x-www-form-urlencoded`，实例、会话索引与待传送任务列表采用 TSV 文本：

```text
POST /bridge/v1/ready
POST /bridge/v1/heartbeat
POST /bridge/v1/idle
POST /bridge/v1/players
GET  /bridge/v1/players
GET  /bridge/v1/definitions
GET  /bridge/v1/games
GET  /bridge/v1/games/details
GET  /bridge/v1/solo/access
GET  /bridge/v1/solo/session-index
POST /bridge/v1/solo/<game-id>/launch
POST /bridge/v1/solo/<game-id>/destroy
GET  /bridge/v1/solo/sessions
POST /bridge/v1/solo/sessions
POST /bridge/v1/solo/sessions/<session-id>/start
POST /bridge/v1/solo/sessions/<session-id>/destroy
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

`POST /bridge/v1/solo/<game-id>/launch` 接收表单字段 `owner=<uuid>` 和
`players=<comma-separated-uuid-list>`。`owner` 必须包含在 `players` 中，所有玩家必须
当前连接到 Velocity。返回值与普通启动接口相同，为 5 列实例 TSV。
`POST /bridge/v1/solo/<game-id>/destroy` 接收 `player=<uuid>`，只用于销毁
`player_world`；重复调用保持幂等。`GET /bridge/v1/solo/access` 每行输出
`base64(server-id)<TAB>instance-uuid<TAB>open|roster<TAB>comma-separated-uuid-list`。
Velocity 应先匹配当前实例 UUID；`open` 允许任意玩家进入，`roster` 只允许最后一列的
冻结名单进入。
`GET /bridge/v1/solo/sessions` 保持返回完整 JSON 会话列表。
`GET /bridge/v1/solo/session-index` 每行输出
`base64(game-id)<TAB>base64(session-id)<TAB>owner-uuid<TAB>comma-separated-uuid-list`。
`POST /bridge/v1/solo/sessions/<session-id>/start` 接收
`players=<comma-separated-uuid-list>`，只启动指定的现有会话并只为请求名单中的在线成员
创建传送任务。名单必须非空且全部属于该会话；会话不存在时返回 `404`，该接口绝不会
创建新世界。管理 API 的同名启动接口不接收表单，继续使用完整会话名单并返回 JSON。
`GET /bridge/v1/games` 在原有 12 列后追加 `solo`、Base64 编码的 `solo_mode`、Base64
编码的 `solo_startup`、`solo_max_players` 和 `solo_retention_days`，共 17 列。
`GET /bridge/v1/games/details` 保持前 17 列一致，并追加以 `U+001F` 连接后 Base64 编码
的 `rules` 和 `aliases`，共 19 列，供 Velocity `/game` 显示完整介绍与规则。旧 Bridge
继续使用 17 列接口，不受详细目录扩展影响。

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
