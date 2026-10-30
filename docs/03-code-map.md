# 03 · 代码地图与阅读顺序

## 文件清单

| 文件 | 行数 | 职责 | 关键点 |
|------|-----:|------|--------|
| `main.go` | 12 | 占位入口 | 实际入口在 cmd/server |
| `cmd/server/main.go` | 64 | 服务装配 | DI 顺序、`ws.RegisterActionHandlers()` 必须在启动前调用 |
| `cmd/consumer/main.go` | 75 | 消费者入口 | ConsumerGroup 的 `for { Consume() }` 循环与重平衡处理 |
| `cmd/client/main.go` | 162 | CLI 客户端 | 交互式发弹幕/点赞，调试用 |
| `cmd/benchmark/main.go` | 323 | WS 压测 | `-c` 连接数、`-r` 发送间隔 |
| `cmd/benchmark/kafka/kafka.go` | 107 | Kafka 纯写压测 | 测 producer 上限 |
| `internal/api/chat_handler.go` | 136 | HTTP → WS 升级 | uid 从 query 取、`CheckOrigin` 全放开 |
| `internal/service/chat_service.go` | 28 | 业务层 | 很薄，仅透传 repo（回放功能） |
| `internal/repo/message_repo.go` | 65 | 数据访问 | `CreateInBatches`、按 `(room_id, send_time)` 游标查询 |
| `internal/model/message.go` | 91 | 协议 + 表结构 | `WsPacket`、三个 type 常量、复合索引 `idx_room_time` |
| `internal/ws/client.go` | 125 | 单连接 | ReadPump / WritePump / Close |
| `internal/ws/dispatcher.go` | 25 | 路由表 | 全局 map + `RegisterAction` |
| `internal/ws/router.go` | 98 | 业务处理器 | 补全服务端元数据、点赞聚合 |
| **`internal/ws/manager.go`** | **443** | **核心** | 房间表、广播、Redis 订阅、统计、worker |
| `internal/infra/redis.go` | 246 | Redis 封装 | key 模板、Publish、点赞计数、在线数聚合 |
| `internal/infra/kafka.go` | 127 | Kafka 封装 | Async Producer 调参、ConsumerGroup 调参 |
| `internal/infra/db.go` | 40 | MySQL 初始化 | GORM AutoMigrate |
| `internal/consumer/handler.go` | 446 | 落盘 | msgChan、dbWorker、重试、Cleanup |
| `internal/logger/logger.go` | 48 | 日志 | dev 彩色 / prod JSON |

> `handler.go` 前 215 行是**被注释掉的旧实现**（同步 flush + mutex + sync.Pool 双缓冲）。新旧对比非常有价值：旧版在 `ConsumeClaim` 里直接 flush，会阻塞 Kafka 消费；新版拆出 `dbWorker` 后消费循环变成纯内存操作。**读的时候别被注释误导**。

---

## 六轮阅读顺序

不要从 `manager.go` 第一行硬读，会迷路。按链路走。

### 第 1 轮：连接是怎么建立起来的（约 20 分钟）

1. `cmd/server/main.go` —— 看清 DI 装配和路由注册
2. `internal/api/chat_handler.go:43` —— `ConnectWebSocket`
3. `internal/ws/client.go` —— `Client` 结构体字段含义

**自测**：
- [ ] 一个客户端连上来时，起了几个 goroutine？
- [ ] 为什么 `client.ReadPump(h.manager)` 前面没有 `go`？

<details><summary>答案</summary>

两个：`WritePump` + `ReadPump`。`ReadPump` 不 go 是因为它需要在当前 HTTP handler 的 goroutine 里阻塞，handler 返回就意味着连接结束，生命周期刚好对齐。如果加 `go`，handler 会立刻返回，Gin 可能回收 context，且无法感知读循环结束。
</details>

### 第 2 轮：消息进来之后去哪了（约 30 分钟）

1. `internal/ws/client.go:42` —— `ReadPump` 的解析 + `Dispatch`
2. `internal/ws/dispatcher.go` —— 25 行，看懂注册表
3. `internal/ws/router.go` —— 两种消息各自的处理

**自测**：
- [ ] 客户端只发了 `{"content":"hi"}`，最终进 Kafka 的 JSON 里为什么有 `room_id`、`user_id`、`send_time`？
- [ ] 点赞消息为什么没有进 `m.Broadcast`？

<details><summary>答案</summary>

服务端在 `router.go` 里重新组装了 `fullMsg`，因为客户端上报的 uid 不可信（可伪造），房间归属也必须以连接上下文为准。点赞只更新 `m.localLikes`，由 3 秒定时器批量汇总，避免高频写 Redis。
</details>

### 第 3 轮：核心 manager.go，分四段读（约 1.5 小时）

| 段落 | 行号 | 读什么 |
|------|------|--------|
| 结构与常量 | 20–107 | `Rooms` 为什么是 `map[string]map[*Client]struct{}`（第二层用 struct{} 省内存，相当于 set） |
| 主循环 | 110–132 | 单 goroutine select 四个来源，为什么统计要 `go` |
| 注册/注销 | 175–235 | 房间订阅的创建与 cancel |
| 广播 | 236–443 | `handleBroadcast` / `processDanmaku` / `subscribeToRoom` / `broadcastToLocalClients` |

**自测**：
- [ ] 如果 `handleBroadcast` 里也对本机做一次 `broadcastToLocalClients`，会发生什么？
- [ ] `m.cancelSub` 不 delete 会怎样？

<details><summary>答案</summary>

本机用户会收到两条一样的弹幕（一条来自本地直发，一条来自 Redis 回环）。`cancelSub` 不 delete 会导致房间反复进出时 map 无限增长 + 旧 cancel 覆盖新 cancel，旧订阅协程永不退出（goroutine 泄漏）。
</details>

### 第 4 轮：Redis 层（约 40 分钟）

1. `internal/infra/redis.go` 顶部常量 —— 先记住 4 个 key 模板
2. `PublishToRoom` —— 2 秒超时
3. `UpdateServerOnline` / `GetTotalOnline` —— 心跳与聚合
4. 回看 `manager.go:broadcastStats`

**自测**：
- [ ] TTL 为什么是 7 秒而不是 3 秒？
- [ ] `GetTotalOnline` 里 `val == nil` 时为什么要 `go rdb.SRem(...)`？

<details><summary>答案</summary>

容忍一次心跳丢失（网络抖动）而不误判服务器死亡。`SRem` 用 goroutine 是为了不阻塞聚合主流程——它只是清理动作，失败也无所谓，下次还会再清。
</details>

### 第 5 轮：Kafka 与落盘（约 50 分钟）

1. `internal/infra/kafka.go` —— producer / consumer 两套 config
2. `internal/consumer/handler.go:216` 之后（新实现）
3. `internal/repo/message_repo.go` —— `CreateInBatches`

**自测**：
- [ ] `msgChan` 满了会怎样？这是 bug 还是 feature？
- [ ] `Cleanup` 里 `close(msgChan)` 之后为什么还要 `wg.Wait()`？

<details><summary>答案</summary>

会阻塞 `ConsumeClaim`，Kafka 拉取变慢，offset 提交也变慢——这是有意的背压，防止内存被无限堆积撑爆。`wg.Wait()` 是等 dbWorker 把 buffer 里剩下的数据刷完再退出，否则重平衡时会丢数据。
</details>

### 第 6 轮：压测工具（约 30 分钟）

`cmd/benchmark/main.go` 重点看：连接如何批量建立、如何统计发送/接收速率、如何避免压测机自己成为瓶颈（`ulimit -n`、复用 buffer）。

---

## 一张图记住调用链

```
GET /ws
  └─ ChatHandler.ConnectWebSocket
       ├─ upgrader.Upgrade
       ├─ NewClient
       ├─ Manager.Register ──► handleRegister ──► go subscribeToRoom(ctx)
       ├─ go WritePump ◄─────────────── Client.Send ◄── safeSend ◄── worker
       └─ ReadPump
            └─ Dispatch(type)
                 ├─ 101 Danmaku ──► router: 补元数据 ──► Manager.Broadcast
                 │                                        └─ handleBroadcast
                 │                                             └─ processDanmaku
                 │                                                  ├─ Kafka.PushToInput
                 │                                                  └─ Redis.PublishToRoom
                 │                                                       └─ subscribeToRoom
                 │                                                            └─ broadcastToLocalClients
                 └─ 103 Like ────► localLikes[room] += n   （等 3s ticker 汇总）
```

---

上一篇：[02 性能设计的 12 个手法](02-performance-tricks.md) ｜ 下一篇：[04 本地跑通与压测](04-run-and-benchmark.md)
