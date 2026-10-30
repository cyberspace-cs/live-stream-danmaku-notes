# 07 · 七天动手计划

目标不是「看完」，是**能自己改**。每天 2~3 小时，每天都有可验收的产出。

---

## Day 1 · 跑通全链路

**任务**

1. 装 Go 1.25，`docker-compose up -d`，等 Kafka 就绪
2. 起 server / consumer / client 三个进程
3. 浏览器开两个标签页，用 Console 互发弹幕
4. 查 MySQL 确认落库

**验收**

- [ ] 两个标签页都能收到对方和**自己**发的弹幕
- [ ] 3 秒内收到 `type:102` 的统计包
- [ ] 关掉一个标签页，下一轮 `online` 从 2 变 1
- [ ] `select count(*) from danmaku_messages` 有数据
- [ ] `redis-cli monitor` 能看到 `PUBLISH room:1001:pubsub` 和 `SET room:1001:online:*`

**产出**：一段 30 秒的录屏或截图，记录第一轮跑通。

---

## Day 2 · 吃透「连接」

**任务**

1. 读 `chat_handler.go` + `client.go`
2. 在 `handleRegister` / `handleUnregister` 里各加一行日志，打印 `len(m.Rooms[room])`
3. 用 `pprof` 看 goroutine 数：连 10 个客户端，应该是 `10 × 2 + 常数`

**验收**

- [ ] 能不看代码说出「一个连接对应几个 goroutine、分别是什么」
- [ ] 能解释 `ReadPump` 为什么不加 `go`
- [ ] `go tool pprof http://localhost:6060/debug/pprof/goroutine` 数字对得上

> 需要先注册 pprof：`import _ "net/http/pprof"` + `go http.ListenAndServe(":6060", nil)`。这一步顺手完成，后面都要用。

**产出**：一张自己画的连接生命周期时序图。

---

## Day 3 · 吃透「广播」

**任务**

1. 精读 `manager.go` 的 236–443 行
2. **破坏性实验**：在 `handleBroadcast` 的 `TypeDanmaku` 分支末尾加一行 `m.broadcastToLocalClients(packet.RoomID, payload)`，观察是否出现重复弹幕
3. 改回来，然后把 `WorkerCount` 从 100 改成 1，压测观察 QPS 变化
4. 把 `SendChanSize` 从 4096 改成 16，观察丢包日志

**验收**

- [ ] 实验 2 复现「本机收到两条」
- [ ] 能说出 `WorkerCount` 从 100 → 1 时 QPS 下降多少倍、瓶颈在哪
- [ ] 能解释为什么 `sync.Pool` 归还前要清零（虽然原代码没清）

**产出**：一页实验记录（改了什么 / 观察到什么 / 为什么）。

---

## Day 4 · 吃透「统计」

**任务**

1. 读 `redis.go` 的 4 个 key 设计和 `broadcastStats`
2. 用 `redis-cli --scan --pattern 'room:*'` 观察 key 结构
3. 手动 `redis-cli del room:1001:online:<serverID>`，模拟实例崩溃，观察 7 秒内人数自愈
4. 把 `BroadCastInterVal` 改成 10 秒，观察前端数字的「跳变」感

**验收**

- [ ] 能画出「三个实例 → 聚合出全局在线数」的数据流
- [ ] 能解释为什么 TTL 是 7 秒而不是 3 秒
- [ ] 能说出 INCR/DECR 方案在哪种故障下会失效

**产出**：Redis key 设计表（自己默写一遍）。

---

## Day 5 · 吃透「落盘」

**任务**

1. 读 `consumer/handler.go`（注意跳过前 215 行注释）
2. 用 `cmd/benchmark/kafka/kafka.go` 灌 100 万条，观察 MySQL 的 `INSERT` 频率
3. 在 `flushBuffer` 里打印 `len(batch)`，确认每批大小
4. 把 `msgChan` 容量改成 10，观察 Kafka 消费是否被拖慢（背压生效）
5. **故障演练**：压测中途 `kill -9` 消费者，重启后对比 Kafka offset 与 MySQL 行数，算出差值 = 丢失量

**验收**

- [ ] 能说出「入队即 MarkMessage」为什么是 at-most-once
- [ ] 实测出丢失量的量级（应该是几百到两千条）
- [ ] 能解释 `Cleanup` 里 `close(msgChan)` + `wg.Wait()` 的顺序为什么不能反

**产出**：一张「数据丢失窗口」示意图。

---

## Day 6 · 压测与定位瓶颈

**任务**

1. `ulimit -n 200000`，`sysctl` 调参
2. 从 `-c 1000` 开始，逐步加到 `-c 60000`，记录每一档的 CPU / 内存 / 连接数 / 丢包率
3. 在峰值时抓 `go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30`
4. 看 top 函数，判断瓶颈是 `safeSend`、`json.Marshal` 还是 GC

**验收**

- [ ] 画出一条「连接数 vs CPU」曲线，找到拐点
- [ ] 能说出当前机器上的真实瓶颈（大概率是内存带宽或 fd）
- [ ] 遇到端口耗尽时知道怎么解（扩端口 / 加 IP）

**产出**：压测报告（表格 + 曲线 + 瓶颈结论）。

---

## Day 7 · 做一个真正的改造

三选一，做完就是简历上能写的东西：

### 方案 A（简单）：加 JWT 鉴权 + 限流

- `ConnectWebSocket` 校验 `token` 参数
- 每个 uid 一个 `rate.Limiter`，超过 10 条/秒直接丢弃
- 验收：伪造 token 连不上；刷屏被限流

### 方案 B（中等）：把 Pub/Sub 换成 Redis Stream

- `XADD room:{id}:stream MAXLEN ~ 10000 * payload` 替代 `PUBLISH`
- 每个实例用 `XREADGROUP` + 独立 consumer group，处理完 `XACK`
- 验收：手动 `kill -9` 服务端再重启，断线期间的弹幕能从 Stream 补回来

### 方案 C（较难）：加 Prometheus 指标

- 暴露 `ws_connections{room}`、`ws_broadcast_total`、`ws_dropped_total{reason}`、`db_flush_duration_seconds`
- Grafana 画板：连接数、广播速率、丢包率、落库延迟
- 验收：压测时能实时看到曲线

**产出**：一个 PR（提到自己的 fork 仓库），带 README 说明改了什么、怎么验证。

---

## 进阶改造题目（挑感兴趣的做）

| 难度 | 题目 | 考察点 |
|------|------|--------|
| ⭐ | 加应用层 Ping/Pong 心跳 | WS 生命周期管理 |
| ⭐ | 内容长度截断 + 敏感词过滤 | 输入校验 |
| ⭐⭐ | 按 roomID hash 到固定 worker，保证房间内有序 | 并发与顺序 |
| ⭐⭐ | 消费端幂等：唯一索引 + INSERT IGNORE | 至少一次语义 |
| ⭐⭐ | Kafka 写入解耦到独立 goroutine + 有界队列 | 故障隔离 |
| ⭐⭐⭐ | Redis 按 roomID 分片（多个 Redis 实例） | 水平扩展 |
| ⭐⭐⭐ | 100 万人在线房间的分层扇出 | 架构设计 |
| ⭐⭐⭐ | 把 WS 网关换成 gnet（epoll 事件循环） | 底层网络编程 |

---

## 学习闭环检查

七天之后，你应该能不看笔记回答：

1. 一条弹幕从客户端发出到另一个客户端收到，经过哪几个组件、几次序列化？
2. 为什么本机也要走 Redis 回环？不走会怎样？
3. 6 万连接的瓶颈为什么是端口而不是 CPU？
4. 在线人数为什么用心跳不用 INCR？
5. 消费端丢数据的窗口有多大？怎么补？
6. 如果房间有 100 万人，架构要怎么改？

答不上来的，回到对应那一天的文档重读。

---

上一篇：[06 面试怎么讲](06-interview-notes.md)
