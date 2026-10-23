# 远程接入 survival.talexck.com

本次更新提供源码和构建产物。本地工程目录不是线上服务器；以下操作由管理员在远程执行。

面向玩家的独立说明见 [玩家地图、路标与 Voxy 迁移指南](PLAYER_MIGRATION_GUIDE.md)，可直接作为迁移通知附件。

## 接入行为

- 所有外部服务器统一配置在 `paths.servers` 指定目录内的 `external-servers.json`。例如 `paths.servers` 为 `"server"` 时使用 `server/external-servers.json`，为 `"servers"` 时使用 `servers/external-servers.json`。
- 文件顶层是外部服务器 ID 到配置的映射，可以包含多个外部服务器。托管服务器扫描会跳过这个聚合文件，不能把 `external-servers` 当作托管服务器 ID 启动。
- Nix 即使使用 `database/runtime-config.json`，也会通过已绝对化的 `paths.servers` 找到同一个外服文件。新版不再读取项目根目录或 `database/` 下的旧外服文件；升级时把条目合并迁移到服务器定义目录，并保留旧文件备份。
- 外服仅作为固定地址和固定 Minecraft 版本的目的地，单人菜单点击后直接请求传送。
- 不创建 runtime、不分配托管端口、不复制世界、不等待 READY 或 bridge 心跳，也不执行外服的启动、停止、重启、空闲回收或网络维护。
- 外服无需安装 scheduler bridge。玩家仍保持连接到 Velocity，所以代理上的 `/ping`、`/hub` 可用。
- `/ping` 显示的是玩家到 Velocity 的延迟，不是代理到外服的网络延迟。
- `/hub` 返回服务器 ID 为 `Lobby` 的大厅。大厅必须已注册且可连接。
- 外服不参与投票；原有共享游戏仍可同时配置 `solo: true`、`voting: true`。
- 外服故障时传送失败会由 Velocity/Scheduler 返回或记录失败；不会尝试启动这个外服。

## 1. 先保证身份与原存档一致

**同样使用 MUA 不等于已经验证 UUID 一致。** 代理必须使用和原来直连外服相同的 MUA 身份源，玩家也必须选择同一个角色；后端必须正确接收代理转发的 UUID。

接入功能没有修改外服世界或 playerdata 的代码。只要外服加载原世界，并继续识别到同一个 UUID，原背包、末影箱等玩家数据就会按原 UUID 加载。模组物品还要求外服继续保留原模组及其数据。

远程迁移前：

1. 在原外服正常保存并退出，备份完整世界和模组/插件数据。
2. 用一个已有玩家记录原 UUID。在原外服控制台运行 `data get entity 玩家名 UUID`，或核对原 `world/playerdata/<uuid>.dat` 文件。记录 UUID，不能仅比较玩家名。
3. 配置转发后，用同一个 MUA 角色从大厅进入，再在外服控制台执行相同命令，比较 UUID。
4. UUID 一致后检查背包、末影箱、位置和经验。若 UUID 不同，先修正认证/转发；不要删旧 playerdata，也不要直接把所有文件改名。

当前工程的代理模板使用 `online-mode = true` 和 `player-info-forwarding-mode = "modern"`，远程请核对实际生效的配置。MUA 的认证应在 Velocity 上正确配置，不能为了连接外服随意把代理改成离线认证。

外服必须是可接收 Velocity 转发的**后端**，不能把一个完全不配合的、仍自行执行在线登录验证的公开服务器直接填进去就期待代理切服。若 `survival.talexck.com` 指向另一层代理，应先确定可接入的实际后端地址。

- **Fabric 26.2**：安装兼容 26.2 的 FabricProxy-Lite（不是 scheduler bridge），开启/配置 modern forwarding，secret 与 Velocity 的 forwarding secret 一致；`server.properties` 使用 `online-mode=false`，由入口代理完成 MUA 认证。
- **Paper**：`server.properties` 设置 `online-mode=false`；`spigot.yml` 的 `settings.bungeecord=false`；`config/paper-global.yml` 的 `proxies.velocity.enabled=true`、secret 匹配代理，`proxies.velocity.online-mode` 与代理的 `online-mode` 一致。
- 后端仍由原来的独立进程/服务管理。配置为代理后端后，限制后端入口只接受可信代理访问；原来的公网直连方式可能不再适用。

转发字段与身份机制参见 [Velocity 官方文档](https://docs.papermc.io/velocity/player-information-forwarding/)。

## 2. 更新远程程序

更新这几处实际配置引用的文件：

| 位置 | 产物 |
| --- | --- |
| Scheduler 可执行程序 | `server-scheduler-linux-x86_64`，远程为 Linux x86_64 时使用 |
| Velocity 插件目录/对应 Scheduler 插件模板 | `scheduler-bridge-velocity-0.1.0-SNAPSHOT.jar` |
| Lobby 的 Paper 插件目录/对应 Scheduler 插件模板 | `scheduler-bridge-paper-0.1.0-SNAPSHOT.jar` |
| Lobby 的 GameVoting 插件 | `GameVoting-1.1.4.jar` |

如果远程不是 Linux x86_64，请在目标平台从更新后的 Scheduler 源码构建：`cargo build --release --locked`。不要上传 macOS 二进制到 Linux。

Velocity 上的 ViaVersion 请使用 **5.11.0 或兼容的更新正式版**。本次编译与协议测试使用 5.11.0；旧 API 5.9.1 不认识 26.2。26.2 支持从 5.10.0 引入，5.11.0 还修复了相关问题，见 [官方发行说明](https://github.com/ViaVersion/ViaVersion/releases/tag/5.11.0)。

在维护窗口执行更新：

1. 先把新产物上传到单独的暂存目录，备份原程序和相关 jar。
2. 使用原有服务管理方式停止 Scheduler，等待所有受管服完成正常保存及退出。若直接用 CLI，则在配置目录执行 `./server-scheduler stop`，并确认原进程已经退出。不要用强杀替代持久地图的正常保存。
3. 替换 Scheduler 可执行文件并赋予执行权限。替换 Velocity、Lobby 的上述 jar；同一插件不要同时保留两个版本。Scheduler 从模板生成 runtime 的部署，应更新模板引用的文件，而非仅改旧 runtime。
4. 完成下面的配置，再以原方式启动；直接 CLI 模式为 `./server-scheduler --config /实际路径/config.json start`。

这些重启针对受管网络和插件更新，**不会让 Scheduler 接管或关闭外部生存服务器**。外服为启用身份转发需要的重启由外服原来的管理方式执行。

## 3. 配置外服菜单入口

将 `examples/external-servers.json` 的内容合并到 `paths.servers` 指向的目录中的 `external-servers.json`。文件包含所有外部服务器，不是每个外服单独建一个托管定义文件。已有条目时合并对象，不要覆盖掉原条目。

```text
项目目录/
├── config.json
├── database/runtime-config.json   # Nix 生成的主配置（若使用 Nix）
└── servers/                      # 以 paths.servers 的实际值为准，也可以叫 server/
    ├── Proxy.json
    ├── Lobby.json
    └── external-servers.json     # 所有外服的聚合配置
```

本次路径变更需要替换并完整重启 Scheduler，使新程序加载。原 `minecraft.nix` 在检测到已有 Scheduler 时只打印 `Scheduler is already running` 后退出，因此仅重复启动该 Nix 文件不会替换旧进程。后续只修改外服条目时，在大厅执行 `/vote reload` 并重新打开 `/solo` 即可，无需为条目修改重启 Scheduler。

示例使用 `survival.talexck.com:25565`，**25565 是默认端口假设，未探测远程实际端口**。若原连接依赖 DNS SRV 或非默认端口，请填写实际后端 `host:port`；本功能不执行 SRV 解析。

地址支持 IPv4、主机名、`[IPv6]:port`。主机名按固定名字注册，DNS 可正常解析。

`minecraft_version` 是外服真实版本；未提供游戏版本范围时，入口默认仅允许该版本客户端。本例为 26.2。允许更多版本时可显式设置游戏的 `min_version`/`max_version`，但必须自行确保跨版本协议与模组兼容。

`max_players: 50` 是菜单元数据，不会修改外服的 `max-players`。每次点击只传送点击者自己，不创建队伍世界。

GameVoting 使用：

```yaml
game-config-mode: scheduler
```

重启 Lobby 后重新打开单人菜单。只改目录配置时，Velocity 约 2 秒自动刷新；GameVoting 需重载游戏配置或重启 Lobby 才能看到新条目。完整升级请按维护窗口重启，避免旧 Bridge 只读取 17/18 列目录导致外服入口被忽略。

## 4. Xaero 按子服务器隔离（不是禁用地图）

在 **Velocity 进程环境变量** 中添加：

```text
SCHEDULER_XAERO_WORLD_IDS=true
```

若 Velocity 由 Scheduler 启动，把该键合并到它的 `servers/<代理ID>.json` 的 `environment` 对象中：

```json
"environment": {
  "SCHEDULER_XAERO_WORLD_IDS": "true"
}
```

重启 Velocity 生效。默认不开启，避免更新插件时无意改变已有玩家的地图绑定。

开启后，代理给各子服务器发送不同且稳定的 Xaero server level ID。服务器 ID 大小写不影响标识，重启也不改变；修改服务器 ID 则会改变地图绑定。主世界、下界和末地仍由 Xaero 自己按维度区分。大厅、生存服及其他游戏均可使用地图，各自绑定自己的数据。

实现只发送/替换世界标识，不发送禁用小地图、禁用路径点、重置配置或修改客户端本地设置的指令。客户端主动手选别的地图仍然是允许的；它不是禁止查看本地文件的访问控制。

协议按官方 Fabric 26.2 版 Xaero Minimap 26.5.0、World Map 1.46.0 的数据格式核实。它不要求统一整合包，但要求客户端模组支持对应的 server level ID 协议；不安装 Xaero 的客户端不受影响。其他客户端版本尚未逐一实机验证。

客户端 World Map 的地图选择模式应选 **Server**；Minimap 的自动子世界与当前地图关联。已有绑定需按下面方法确认一次。若玩家强制使用 Single 模式、手动固定到旧地图，代理无法强制替其修正本地设置。

代理在切服、客户端注册通道时发送标识，并每 2 秒补发一次，覆盖同一后端内切换维度后标识重置的情况。没有运行中的客户端做端到端验证；首次上线应实际测试大厅 → 生存服 → 下界 → /hub → 生存服，观察是否有短暂显示延迟或错误绑定，再通知玩家迁移地图。

### 旧地图和路径点的一次性迁移

原来直连 `survival.talexck.com`，改从大厅进入后，Xaero 看到的是**玩家连接的大厅地址**，不是代理的后端地址。服务器无法自动替玩家重命名电脑上的 Xaero 文件夹，因此不能承诺客户端完全零操作迁移。

先退出游戏，备份整个 `xaero` 文件夹。新版本数据一般位于：

```text
.minecraft/xaero/world-map/Multiplayer_<地址>/...
.minecraft/xaero/minimap/Multiplayer_<地址>/...
```

旧版可能使用 `XaeroWorldMap`、`XaeroWaypoints`。实际文件夹名可能经过字符替换，请以客户端生成的名称为准。

- **首次改地址，新的代理地址没有需要保留的地图时**：保留原地址目录的备份，将原 `survival.talexck.com` 的地图和路标目录各复制一份，副本改成客户端为大厅地址生成的目录名。不要删除原文件。
- **代理地址已有大厅或其他游戏地图时**：不要直接覆盖整个新地址目录。应先备份并辨认各维度和地图实例，再按 Xaero 的地图目录迁移说明逐个迁移；不同版本的目录结构可能不同，不能用一条通用覆盖命令安全处理。
- 进入生存服，World Map 的 Map Selection 中选旧地图并确认 Server 绑定；Minimap 路标菜单选择原子世界，在 Options 中使用 **Make Sub-World Auto**。每个需要恢复的维度分别确认。
- 回大厅后确认自动选择大厅地图，不显示生存路标；再回生存服，确认探索记录与路标保留。

上述操作改变的是客户端地图缓存的归属，不影响服务器背包。依据 [Xaero World Map 官方说明：地址迁移、Server 模式及子世界绑定](https://modrinth.com/mod/xaeros-world-map)。

## 5. Voxy 的独立限制与缓存迁移

Voxy 也应按子服务器隔离，不能把“给 Xaero 发送世界 ID”当成同时支持了 Voxy。本次插件没有添加禁用 Voxy 的逻辑，也没有声称实现了原版 Voxy 的代理隔离协议。

核查日期为 2026-09-14。上游源码版本 `b02ff02683cf6db5d048f0a36626d91ce35b031d` 中：

- 多人缓存根路径使用客户端连接地址：`.minecraft/.voxy/saves/<连接地址>/`，地址中的冒号替换为下划线。
- 存储世界 ID 是 `biomeSeed + dimension key` 的 SHA-256 前 32 个十六进制字符；不包含 Velocity 的子服务器名，也不读取本次 Xaero 标识。
- 因而不同种子/维度通常会落入不同缓存；如果两个子服的这些标识相同，就不能保证分开。部分服务端或插件可能对发给客户端的种子标识做隐藏/归零，不能仅看真实世界 seed 不同就认定安全。

依据：[缓存根目录实现](https://github.com/MCRcortex/voxy/blob/b02ff02683cf6db5d048f0a36626d91ce35b031d/src/main/java/me/cortex/voxy/client/VoxyClientInstance.java)、[世界 ID 实现](https://github.com/MCRcortex/voxy/blob/b02ff02683cf6db5d048f0a36626d91ce35b031d/src/main/java/me/cortex/voxy/commonImpl/WorldIdentifier.java)。不同 Voxy 版本需按实际版本核对。

远程上线时，用 Voxy 玩家分别进入大厅、生存服和下界，再退出游戏观察代理地址目录下产生的世界缓存子目录，并检查远景是否出现其他子服地形。测试前备份 `.voxy`，避免测试中的新缓存污染原数据。客户端未统一且未安装专用适配时，不能保证所有版本无条件自动隔离。

原来直连的缓存可以尝试复用，但前提是该 Voxy 版本的存储格式与客户端收到的维度/种子标识不变：

1. 完全退出 Minecraft，备份整个 `.voxy`。
2. 找到原 `.voxy/saves/survival.talexck.com/` 和客户端为大厅入口生成的新地址目录；端口存在时名称可能带 `_端口`。
3. 只复制确认为生存服的完整世界缓存子目录到新地址下，保持世界目录名不变。新地址不存在同名缓存时才能直接复制。
4. 同名目录已存在时先停止操作，核对归属。不要合并两套 RocksDB 文件，也不要仅删 `LOCK` 文件强行打开；不要在游戏运行中操作数据库。
5. 再进入生存服检查旧 LOD 是否恢复；若世界 ID 已变化，旧目录不会自动匹配，需要针对客户端版本进一步适配或重新生成缓存。

若实测存在串缓存，可靠的后续方案是提供一个客户端兼容模组，让 Voxy 的缓存标识额外包含稳定子服 ID，并处理旧数据别名迁移。该方案需要玩家安装相应组件；单靠一个原版客户端不识别的代理消息无效。本次没有修改世界 seed、篡改发给所有客户端的生物群系种子或重命名维度来绕过这一点。

## 6. 远程验收

1. 26.2 玩家从大厅单人菜单能看到生存服，点击后直接进入，没有“正在开服”的流程。
2. 在生存服执行 `/ping` 和 `/hub`，确认代理命令可用。
3. 再进入生存服，核对与原来直连时相同的 UUID、背包、末影箱、位置、经验。
4. Scheduler 托管列表中不出现 `survival` 实例，不产生它的 runtime 目录。
5. 生存服保持由原管理方式运行；Scheduler 的网络启动/停止/同步不改变该外服进程。
6. 开启 Xaero 隔离后实测切服与维度切换，检查大厅不串出生存路径点，生存旧地图能恢复。
7. 单独检查 Voxy 缓存归属；Xaero 正常不代表 Voxy 已隔离。

## 7. 单人菜单缺少外服入口时

先在大厅执行 `/vote reload`，查看 GameVoting 的 `Loaded game` 日志。若重载成功但没有 `survival`，说明它收到的目录已经缺少该游戏，不是菜单按版本或在线状态隐藏了它。若重载失败，旧目录会保留，须先处理报错。

新版 Scheduler 内置只读检查命令，不需要 Python、curl 或手动输入 token：

```bash
./server-scheduler --config ./config.json catalog survival
```

也可以把新二进制以 `server-scheduler-check` 的名字放在原程序旁边，执行 `./server-scheduler-check --config ./config.json catalog survival`；不必替换正在运行的程序或重启 daemon。该命令从指定配置读取 API 端口和 bridge token，先检查本地外服定义，再请求正在运行的 daemon，打印总条目数、响应列数和匹配的游戏。不会启动、停止服务器、初始化 runtime 或输出 token。这里的“本地定义”指执行命令的那台远程服务器上的文件；配置路径应与 daemon 实际使用的路径一致。

下方 Python 脚本是可选的替代方式。

将 `scripts/check-game-catalog.py` 复制到远程 Scheduler 所在机器，使用 Python 3 执行（下面路径需替换为实际路径）：

```bash
python3 check-game-catalog.py \
  --config /实际配置目录/config.json \
  --jar /实际大厅目录/plugins/scheduler-bridge.jar
```

`--jar` 可省略；填写时应指向大厅使用的 Paper Bridge。脚本只读取指定文件并对本机 Scheduler 发起 GET 请求，不启动、停止或修改任何服务器，不输出 token 或完整配置。磁盘 jar 的检查不代表当前 JVM 已加载同一文件。

- `Requested game in API: True`：Scheduler 已返回入口，继续检查大厅请求所用的 Bridge 与 API 地址。
- `Requested game in API: False` 且 `Column counts: [20]`：API 支持外服协议，但该次读取未包含入口；对照实际 daemon 启动时的 `--config` 路径及其中的 `paths.servers`。
- 列数仅为 17 或 18：当前响应没有采用请求的外服协议，需检查实际提供 API 的 daemon 构建。目录为空时无法仅凭列数判断版本。
- HTTP 非 200：按执行时间查看 Scheduler 日志中的详细错误。
- `Bridge jar requests external catalog: False`：这份 jar 不请求外服列表。更新后的 Bridge 应发送 `include_external=true`。

2026-09-14 排查发现，直接使用 Gradle 构建时，旧的短文件名 `build/libs/scheduler-bridge.jar` 可能遗留，而带版本号的 jar 已更新。构建流程现已修正，每次生成运行 jar 后会同步更新短文件名；两者内容应一致。原 `2026-09-14-external.tar.gz` 中的 Paper Bridge 已包含外服请求参数。
