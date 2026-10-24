# Server Scheduler

`server-scheduler` 是运行在宿主机上的 Minecraft 子服务器调度器。它根据
`servers/<server-id>.json` 组装临时实例、选择 Java 21 或 Java 25、分配端口、
启动服务端进程，并通过仅监听 `127.0.0.1` 的 HTTP API 提供管理和 Bridge 通信。

Scheduler 自身以后台进程运行，不提供持续交互式控制台。服务端命令、日志和实例
状态通过 CLI 或 HTTP API 访问。

服务重启一旦提交，由 Scheduler 独立完成停服、清理和重新启动；即使发起请求的
Velocity 自身退出、HTTP 连接断开或客户端超时，重启仍继续执行。相同服务器的并发
启动/重启会被拒绝，避免重复进程；Scheduler 整体关闭时不会再为未完成的重启拉起替代进程。

停止接口等到进程退出、存档保存和运行目录清理完成后才返回。CLI 停止请求和 Bridge
停止/重启请求等待最多 180 秒，避免使用普通查询的短超时而误报正常关服失败。
保存或清理真正失败时仍返回错误，并保留相应恢复资料。

## 实现结构

部署根目录为 `shtechcraft-minigames/`，调度器源码位于相邻的 `server-scheduler/`，
跨平台游戏插件位于 `scheduler-bridge/`。玩家先连接 Velocity，进入常驻 Lobby；
大厅投票、Solo 入口或管理命令经 Bridge 请求 Scheduler，目标实例 READY 后由
Velocity 注册后端并执行传送。外部服务器只作为固定目标注册和传送，不由 Scheduler
启动或保存世界。

| 模块 | 职责 |
| --- | --- |
| `main.rs` / `client.rs` | 后台进程、CLI、启动停止、回环 HTTP 客户端 |
| `config.rs` / `layout.rs` | 服务器目录、Java 与版本选择、临时实例组装、配置渲染 |
| `manager.rs` | 进程和端口管理、READY/心跳、自动启动、空服关闭、维护、玩家与传送队列 |
| `solo.rs` / `world_store.rs` | Solo 冻结名单、存档过期清理、持久世界保存与中断恢复 |
| `dependency_cache.rs` / `runtime.rs` | 依赖内容寻址缓存、运行目录暂存与清理 |
| `api.rs` / `protocol.rs` | 独立 Admin/Bridge Token、管理 API 与游戏内通信协议 |
| `network.rs` | 可选 FRPC 子进程与异常重启 |

服务器定义、世界存档和依赖缓存保存在本机文件系统；当前实例、在线玩家和传送队列
由 Scheduler 内存维护。本地 PostgreSQL 的连接参数由 Scheduler 渲染给游戏插件，
用于 LuckPerms、Bingo Stats、成就等业务。Scheduler 自身已不再使用 SQL 客户端。
独立的 `scheduler-web/` 仍是读取旧 `scheduler_*` 表的 Next.js 项目；此版本不再为它
生产数据，历史表保留在原数据库中。

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

Scheduler 已移除面向 `scheduler-web` 的公网数据库发布与排行榜上传功能：不再连接、
建表或写入 Web 数据库，`POST /bridge/v1/leaderboards`、`POST /bridge/v1/sync` 和
Velocity 的 `/network sync` 已删除。旧配置中的 `public-database` 会被忽略，升级不会
连接或删除其中指定的数据库。游戏插件使用的本地 PostgreSQL 配置与 Bridge 的玩家
状态上报、目录轮询和传送功能继续保留。

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
.resources/generations/<revision>/cores/<core-kind>/<minecraft-version>/
.resources/generations/<revision>/plugins/<plugin-id>/<minecraft-compatibility>/
.resources/generations/<revision>/mods/<mod-id>/<minecraft-compatibility>/
dependencies/objects/<sha256-prefix>/<sha256>
dependencies/manifests/servers/<server-id>.json
files/<file-layer>/
network/frpc
network/frpc.toml
servers/<server-id>.json
saves/shared/<world-id>/
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
- `template`：从 `worlds.json` 引用的 depend 地图压缩包解压到临时实例。
- `persistent`：启动时从 `saves/shared/<world-id>/` 装载世界包，进程退出后将运行期
  存档安全写回。默认保存 `world`、`world_nether` 和 `world_the_end`。

普通长期存档服可以这样配置：

```json
"world": {
  "mode": "persistent",
  "id": "survival"
}
```

使用 Multiverse 等插件维护额外世界时，可以显式指定需要保存的相对目录：

```json
"world": {
  "mode": "persistent",
  "id": "survival",
  "paths": [
    "world",
    "world_nether",
    "world_the_end",
    "dungeons"
  ]
}
```

`paths` 至少包含一项并且必须包含 `properties.level-name` 指定的主世界目录（默认
`world`）；所有值只允许安全的相对路径，并且不能重复、大小写冲突或互相嵌套。
首次启动前必须创建 `saves/shared/<world-id>/`，并把已有世界目录放在其中。
同一个持久世界 ID 同时只能由一个实例占用，即使两个不同服务器定义引用了相同 ID，
后启动的实例也会被拒绝。

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

进程退出后，Scheduler 先保存 `persistent` 或 `player_world` 存档，再回收运行期新增的
共享依赖，最后删除该实例的整个临时工作目录。普通 `generated` 和 `template` 实例仍不
回写世界。Scheduler 启动时会清空 `.runtime/` 中的普通历史残留，完整控制台输出只
保留在独立的 `logs/` 目录中。

`persistent` 以整个 `saves/shared/<world-id>/` 作为一次提交：先把旧世界包复制到 staging，
再用 runtime 中实际存在的配置目录替换对应旧目录，最后通过 backup 和 rename 更新正式
世界包。未在 runtime 中生成的可选世界会保留上一次副本，`session.lock` 不会进入存档，
任何符号链接或特殊文件都会使保存失败。这样不会出现主世界已经更新、下界却仍是旧版本
的混合存档。

实例准备时会在 persistent runtime 写入 `.scheduler-persistent-world.json` 恢复标记。
只有世界成功写回后才移除该标记。如果保存失败或 Scheduler 进程意外中断，下次启动
会保留对应 runtime，并拒绝再次启动同一世界 ID；管理员需要先检查并处理该恢复目录，
避免从旧存档启动后再次覆盖。若提交恰好中断在正式目录与 backup 的切换窗口，下一次
装载（管理员处理完 recovery runtime 后）会优先恢复唯一的 backup；候选状态无法唯一
判定时会拒绝自动选择。该机制不承诺
在操作系统崩溃、断电或存储设备未完成落盘时仍具有持久性，生产环境仍应定期异机备份。

后台启动会通过 `scheduler.pid` 的原子创建和存活 PID 检查保证单实例运行。即使旧守护
进程已经关闭 HTTP 监听、仍在等待大世界保存完成，新的 `start`/`restart` 也会被拒绝，
不会出现两个 Scheduler 同时操作同一存档的情况。为避免并发启动竞态，Scheduler 不会
自动删除 stale PID 文件；确认旧进程已经退出后，需要由管理员手动删除该文件。

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
和 `on_demand`。`solo_max_players` 限制单次启动请求人数，且不能超过 `max_players`。
共享模式允许单人进入，不受投票的 `min_players` 限制；`player_world` 请求仍须满足
`min_players`。`player_world` 仅允许 1 或 2 人，并强制使用
`on_demand`、关闭服务器级 `autostart`，且 `level-name` 必须为 `world`。

`shared` 使用服务器定义对应的开放静态后端，不冻结玩家名单。`always` 会让 Scheduler
保持后端运行，`on_demand` 在首个请求时启动。任意在线玩家都可通过 Solo 请求启动或加入
同一实例，每次请求中的玩家列表只用于该次传送。组合启动接口会原子串行化启动与传送，
并发请求不会相互覆盖。

### 同时支持投票和单人入口的共享存档游戏

`gamevoting.voting` 独立控制投票列表。省略时默认是 `!solo`，保留已有配置行为；
设置 `solo: true`、`solo_mode: "shared"`、`voting: true` 后，同一个游戏同时出现在
投票和单人页面。`player_world` 不允许开启投票，以保留私人世界的固定名单约束。

在服务器 JSON 中合并以下配置（其余 core、memory、plugins 等字段沿用目标服务器）：

```json
{
  "world": {
    "mode": "persistent",
    "id": "shared_adventure",
    "paths": ["world", "world_nether", "world_the_end"]
  },
  "gamevoting": {
    "id": "shared_adventure",
    "name": "共享冒险",
    "material": "GRASS_BLOCK",
    "min_players": 1,
    "max_players": 50,
    "solo": true,
    "voting": true,
    "solo_mode": "shared",
    "solo_startup": "on_demand",
    "solo_max_players": 1
  }
}
```

地图放在 `saves/shared/shared_adventure/world/`（其他维度按实际服务端目录布局配置）。
启动会把持久存档复制到 runtime，停服完成后保存回同一目录，下次启动继续使用保存进度。
共享存档沿用 persistent 的原子保存、并发写入保护和失败恢复机制，不按玩家创建副本，
也不受 `solo_retention_days` 的私人存档到期清理影响。

投票预启动和单人请求共同复用服务器定义的静态 ID。实例已 READY 时直接向该实例
创建传送任务；正在启动时加入待传送队列；两个入口并发启动只创建一个实例。
投票请求人数不受 `solo_max_players` 限制，该限制仅用于单人入口的单次请求。

`player_world` 为每份名单创建独立动态实例、端口和 Velocity 服务器 ID。地图母版通过
`worlds.json` 引用 `depend/worlds/<地图ID>.tar.gz`，持久副本位于
`saves/players/<owner-uuid>/<game-id>/world`，旁边的 `config.json` 保存游戏、所有者、冻结
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
新版 Bridge 请求 `GET /bridge/v1/games?include_voting=true`，追加解析后的 `voting`
布尔值作为第 18 列。未带参数的旧客户端仍获得 17 列；新版客户端兼容旧的 12/17 列响应。
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

## 固定地址外服

可在 `paths.servers` 指定的服务器定义目录内增加 `external-servers.json`，把固定地址和真实 Minecraft
版本的独立外服接入单人游戏目录。示例见 [examples/external-servers.json](examples/external-servers.json)。
此文件聚合所有外服，托管服务器扫描会跳过它；外服不参与 runtime、世界保存、进程生命周期或 network 管控。
目录可以是 `server/` 或 `servers/`，以配置为准。Nix 生成主配置后仍沿用该目录；新版不再读取主配置旁的旧外服文件。

新版 Bridge 使用 `GET /bridge/v1/games?include_voting=true&include_external=true`，
目录包含 20 列：前 18 列保持原义，最后两列是 Base64 编码的外服地址和真实版本。
受管游戏最后两列为空；旧查询不返回外服条目。点击入口调用
`POST /bridge/v1/external-servers/<server-id>/join`，表单 `players=<单个在线玩家 UUID>`，
成功返回 204 并排队直接传送，不启动实例。

完整的远程更新、MUA UUID 保留、`/hub`、`/ping`、Xaero 按子服隔离及旧地图迁移步骤见
[远程外服接入指南](docs/EXTERNAL_SERVERS.md)。

面向玩家的分步操作见 [玩家迁移指南](docs/PLAYER_MIGRATION_GUIDE.md)，涵盖不同实例目录、已有大厅数据的保留、Xaero 路标转移、Voxy 缓存及备份恢复。

## 清单资源与存档布局

部署根目录的 `cores.json`、`mods.json`、`plugins.json`、`worlds.json` 使用 schema_version 1。
每份清单有 artifacts 对象，资源包按 ID/兼容版本命名，files 中每项提供 target 与 source。
一个核心包可以包含多个文件。source 支持 paper、fabric、modrinth_latest、spigot_latest、
curseforge_latest，以及固定 url / modrinth / spigot / curseforge 和本地 file。

默认最新来源固定 Minecraft 版本和加载器，按上游发布时间选择最新文件；Velocity 插件按
代理平台选择。官方来源保存解析后的版本和下载校验值。file 只引用 depend/ 下人工维护的
可读路径，不要求或校验本地哈希；缺少 LFS 内容会明确报错。

Fabric 在资源更新阶段选择固定 Minecraft 版本下最新 Loader，并使用官方安装器
提前生成完整的服务端包，包括 Fabric 服务端启动入口、Minecraft `server.jar` 和
`libraries/`。安装器只在更新阶段执行，不作为游戏实例的启动 JAR。
安装完成的文件按 Minecraft、Loader 和安装器版本缓存；重复更新验证并复用缓存。
实例启动时链接已准备好的服务端文件。任一安装或下载失败都不会激活不完整的代次。

`server-scheduler --config config.json update` 可离线于守护进程执行更新。
游戏内 `/network update` 提交后台任务，由宿主机 Scheduler 在部署根目录执行
`./server.sh update`；`/network update status` 查看结果。任务重复提交时复用正在运行的
任务，标准输出和错误输出保存在部署日志目录的 `resource-update.log`。
脚本非零退出时返回失败状态。脚本中的 Scheduler CLI `update` 执行原生资源更新，
不会再次调用 HTTP 接口，因此不会递归触发同一命令。
Admin API 为 GET/POST /admin/v1/resources/update，Bridge 为
GET/POST /bridge/v1/resources/update；GET /bridge/v1/revision 返回激活代次。
resources.rs 校验完整资源、世界压缩包和服务器定义后原子发布 current.json。
失败保留当前代次，启动中的实例固定代次，运行中的实例继续使用其链接文件。
重复更新复用已验证的远程下载缓存，本地文件直接链接。更新不读写 saves/。

模板来自 depend/worlds/*.tar.gz，每次新实例解压到 runtime；玩家首次创建解谜会话时
直接解压到 saves/players/<owner-uuid>/<game-id>/。双人共享一份拥有者存档。
共享持久世界位于 saves/shared/<world-id>/。这些存档、数据库、运行缓存都应忽略提交。
本地文件更新采用新的版本文件名并修改 source.path，以保留旧实例使用的文件。

日志按宿主机日期轮转，每分钟检查；64 MiB 提前轮转，gzip 压缩后清空当前文件，
归档保留 14 天。启动过程的 stdout/stderr 另存 logs/scheduler-bootstrap.log。
部署 flake 提供 minigames-start/stop/update/check 和 scheduler，.envrc 使用小型子 flake。
