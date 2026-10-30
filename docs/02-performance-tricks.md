# 02 · 性能设计的 12 个手法

按「从协议到存储」的顺序排列。每条都给出代码位置、原理、以及**照抄时容易踩的坑**。

---

## 1. 信封协议 + 延迟解析

`internal/model/message.go`

```go
type WsPacket struct {
    Type   int             `json:"type"`
    RoomID string          `json:"room_id,omitempty"`
    Data   json.RawMessage `json:"data"`   // 不解析内部结构，原样透传
}
```

上层（Manager、Redis、Kafka）只关心路由字段，全程 `[]byte` 搬运，**一条弹幕在 Go 进程内只被反序列化 1 次**（在 router 里补全元数据那次），后续广播、落盘、消费都不再解析。

用 `json.RawMessage` 而不是 `map[string]interface{}` 的好处：无反射、无内存放大、无类型断言。

> **坑**：`RawMessage` 是 `[]byte` 的别名，`json.Unmarshal` 时会**复用底层数组**。如果 packet 被多个 goroutine 共享且底层 buffer 被回收复用（比如 `sync.Pool` 的 `bufio.Reader`），会出现数据被改写。这个项目里 payload 都经过 `json.Marshal` 重新生成，所以安全；自己改造时要注意。

---

## 2. 分层 + 依赖注入，但不教条

`cmd/server/main.go` 的装配过程：

```go
db := infra.InitDB()
messageRepo := repo.NewMessageRepo(db)
chatService := service.NewChatService(messageRepo)
manager := ws.NewManager()
chatHandler := api.NewChatHandler(chatService, manager)
```

Controller（api）→ Service → Repo，依赖从外往里注入。但**广播链路绕过了 Service/Repo**：`ws.Manager` 直接持有 `RedisClient` 和 `KafkaProducer`，因为这条路径要求极致性能，多一层调用就多一次接口逃逸和一次可能的分配。

> 值得学的态度：分层是为了可测试、可替换，不是为了「必须每层都有」。高频路径允许合理的「不整洁」。

---

## 3. 策略模式做消息分发

`internal/ws/dispatcher.go` 只有 25 行，但很实用：

```go
var actionRegistry = make(map[int]MessageHandler)

func RegisterAction(msgType int, handler MessageHandler) { actionRegistry[msgType] = handler }

func Dispatch(client *Client, manager *Manager, msgType int, data []byte) {
    if h, ok := actionRegistry[msgType]; ok { h(client, manager, data) }
}
```

新增一种消息（比如礼物、进场提醒）只需要在 `router.go` 里 `RegisterAction(104, ...)`，`Dispatch` 和 `Client` 一行不改。这是**开闭原则**在 Go 里最轻量的落地方式。

> **坑**：`actionRegistry` 是全局 map，只在启动时写、运行时只读，所以没加锁是安全的。如果将来支持热更新 handler，必须换成 `sync.Map` 或 RWMutex。

---

## 4. 读写锁分离：广播只持 RLock，且只持「拷贝」这一小段

```go
m.mu.RLock()
clients, ok := m.Rooms[roomID]
targetClients := m.clientPool.Get().([]*Client)
for c := range clients { targetClients = append(targetClients, c) }
m.mu.RUnlock()   // ← 5 万个指针拷完就解锁
```

对比常见写法「持锁遍历并逐个发送」：后者在 5 万人房间 + 慢客户端场景下会持锁几百毫秒，期间所有新连接注册被阻塞，表现为「进房卡顿」。

拷贝 5 万个指针大约 0.2ms（内存带宽 ~20GB/s），这个成本完全可以接受。

---

## 5. 100 个 worker 消费 job 队列

```go
const ( BroadcastJobChanSize = 1000; WorkerCount = 100 )
```

一个 job = 一条消息 + 一个房间的客户端切片。100 个 worker 并发执行「遍历切片 + 非阻塞投递」。

为什么是 100？这是**经验值**：worker 数 ≈ CPU 核数 × (1 ~ 2)，但每个 job 内部的 `safeSend` 有 channel 操作（可能触发调度器抢占），实际是 I/O 密集 + 轻 CPU 的混合负载，所以超配到 100 比 `GOMAXPROCS` 更稳。真实调优应该压测 `-c` 与 worker 数的关系。

> **注意有序性问题**：一条消息只由一个 worker 处理，所以「同一条消息发给同一客户端的多个副本」不会乱序；但**连续两条消息可能被不同 worker 并发处理**，理论上存在 A、B 两条弹幕在某个客户端上顺序颠倒。直播弹幕可以容忍，聊天室/订单场景不能容忍（需要按 roomID hash 到固定 worker）。

---

## 6. sync.Pool 复用客户端切片

```go
clientPool: sync.Pool{ New: func() any { return make([]*Client, 0, 500) } }
```

大房间每秒广播上千条，每条都要一个 5 万元素的切片。不复用的话：`50000 × 8B = 400KB/次`，1000 次/秒 = **400MB/s 的分配速率**，GC 直接跪。

三个必须做对的点：

1. **取出后 `[:0]` 重置长度**，容量保留。
2. **归还前必须清空元素引用**——否则池里长期持有 `[]*Client`，客户端对象无法被回收，这是**内存泄漏**。本项目里 worker 直接 `Put(job.Clients)` 没清零，属于潜在泄漏（见 05 第 6 条）。
3. **丢弃路径也要归还**（`default: m.clientPool.Put(targetClients)`），否则池子被抽干。

---

## 7. 非阻塞投递 + 主动丢弃（load shedding）

```go
select {
case client.Send <- payload:
default:
    // 慢客户端，丢这条，不阻塞 worker
}
```

这是**背压的反面**：不把压力传回上游，而是在最末端丢掉。对弹幕是正确的——丢一条弹幕用户几乎无感，但如果阻塞 worker，整个房间的广播都会卡住。

代价就是作者说的「周期丢包率 < 1%」。

> 更成熟的做法：给每个客户端加「丢弃计数」，连续丢 N 条就断开重连（让客户端拉历史补齐），而不是无限丢。

---

## 8. 永不 close(Send)，用 done + sync.Once 退出

```go
func (c *Client) Close() {
    c.once.Do(func() {
        close(c.done)
        c.Socket.Close()
    })
}
```

- 多个路径可能同时触发关闭（读错误、写错误、Manager 注销），`sync.Once` 保证幂等。
- `Send` 不关闭 → worker 的 `safeSend` 永远不会 panic。
- `WritePump` 同时监听 `c.Send` 和 `c.done`，靠 done 退出；残留的 Send 数据随 GC 回收。

> 这是处理「多写者 → 单读者 channel」生命周期的标准答案。反面教材：谁都能 close，然后靠 `recover()` 兜 panic。

---

## 9. 心跳式在线人数（TTL 覆盖写）

```go
// 3 秒一次，TTL = 3*2+1 = 7 秒（容忍丢一次心跳）
reportTTL := BroadCastInterVal*2 + time.Second
infra.UpdateServerOnline(rdb, roomID, m.ServerID, count, reportTTL)
```

```
SET  room:{id}:online:{serverID} {count} EX 7
SADD room:{id}:servers {serverID}
```

聚合：

```
SMembers room:{id}:servers  → [s1, s2, s3]
MGet room:{id}:online:s1 room:{id}:online:s2 room:{id}:online:s3
→ 某个返回 nil 说明该实例已挂 → 异步 SRem，并从总和中剔除
```

对比 INCR/DECR 方案：进程崩溃时 DECR 丢失，人数永久虚高；心跳方案下最多 7 秒自愈。

对比 `KEYS room:*:online:*` 扫描：作者早期版本用 `KEYS`，在几十万 key 下会阻塞 Redis 单线程，改成「Set 注册表 + MGet」后复杂度从 O(总key数) 降到 O(实例数)。

---

## 10. 点赞本地聚合，3 秒批量 INCRBY

```go
// router.go：点赞只进内存，不发任何网络请求
m.likesMu.Lock(); m.localLikes[c.RoomID] += likes.Count; m.likesMu.Unlock()

// manager.go：3 秒一次，交换 map 后批量 INCRBY
currentBatchLikes := m.localLikes
m.localLikes = make(map[string]uint64)
```

如果每次点赞都 `INCR`：热门直播间点赞 10 万 QPS，Redis 直接被打满。聚合后写入量降到「房间数 / 3s」，**写入放大倍数从 1 降到 1/30000**。

**交换 map 而不是遍历后清空**：避免在遍历期间长期持锁，也避免 `delete` 造成的 map 碎片。

> 代价：进程崩溃时最近 3 秒的点赞丢失（见 05 第 4 条）。

---

## 11. Kafka Async Producer 参数调优

`internal/infra/kafka.go`

```go
config.Producer.RequiredAcks     = sarama.WaitForLocal  // 只等 leader，不等 ISR
config.Producer.Compression      = sarama.CompressionSnappy
config.Producer.Flush.Messages      = 50
config.Producer.Flush.MaxMessages   = 200
config.Producer.Flush.Frequency     = 50 * time.Millisecond
config.Producer.Flush.Bytes         = 8 * 1024
config.Producer.Return.Successes = false   // 不要成功回执，省一次 channel 操作
config.Producer.Return.Errors    = true    // 但错误必须收，否则 Errors channel 满了会阻塞
config.Producer.Partitioner      = sarama.NewRandomPartitioner
config.ChannelBufferSize         = 16384
```

逐条解释取舍：

| 参数 | 选择 | 理由 |
|------|------|------|
| `WaitForLocal` | 只等 leader 落盘 | 弹幕是「可丢失的日志」，不需要 `WaitForAll` 的 RTT 代价 |
| Snappy | 压缩率/CPU 平衡点 | 弹幕文本重复度高，压缩比通常 3~4 倍，省下的网卡比 CPU 更贵 |
| Flush 50 条 / 50ms / 8KB | 三个条件任一满足就发 | 低峰期靠时间兜底（最多延迟 50ms），高峰期靠条数/字节数打满批次 |
| `Return.Successes=false` | 关闭成功回执 | AsyncProducer 的成功回执要走一个 channel，高吞吐下这个是纯开销 |
| `Return.Errors=true` + 后台消费 | 必须有人读 | **不读 Errors channel，Kafka 客户端会在写满后阻塞整个 producer** |

最后一条是最容易出事的地方，代码里专门起了 goroutine 消费：

```go
go func() { for err := range producer.Errors() { logger.Log.Error(...) } }()
```

同理，ConsumerGroup 的 `group.Errors()` 也必须有人读。

---

## 12. 消费端「channel 解耦 + 攒批 + 双超时」

```go
ChannelBufferSize = 2000      // Kafka 消费 → DB worker 的缓冲
FlushInterval     = 2 * time.Second
MaxDBRetries      = 3
BackoffDuration   = 500 * time.Millisecond
```

```
Kafka ──► ConsumeClaim（只解析，微秒级）
              │ msgChan 满 → 阻塞 → Kafka 消费自然变慢（背压）
              ▼
         dbWorker（攒够 batchSize 或 2s 超时 → 一次 INSERT）
              │ 失败 3 次，每次退避 500ms
              ▼
         GORM CreateInBatches(msgs, len(msgs))  // 一条 SQL 插 N 行
```

效果：单条 INSERT 的固定开销（网络 RTT + 解析 + 事务）被摊薄到 batch 大小。500 万条 / 2 分钟 ≈ 40k QPS，相当于每秒只发几十条 SQL。

**双缓冲/切片复用**：`buffer = buffer[:0]` 复用底层数组，`flush` 期间不阻塞接收（因为 flush 在同一个 goroutine 里其实是阻塞的——严格说这里是单缓冲，注释里的 "Double Buffering" 只在旧版实现里成立，见 05 第 9 条）。

> **坑**：`CreateInBatches(msgs, len(msgs))` 会把整个 batch 拼成一条 SQL。batch 太大（比如 5000 行 × 500 字节）会超过 MySQL 默认 `max_allowed_packet=64MB` 或拖长事务。生产建议 `batchSize ∈ [500, 2000]`，并显式设置 `batchSize` 而不是 `len(msgs)`。

---

## 手法清单速查

| # | 手法 | 解决的问题 |
|---|------|-----------|
| 1 | 信封 + RawMessage | 重复反序列化、反射开销 |
| 2 | 分层但允许旁路 | 高频路径的接口/逃逸开销 |
| 3 | 策略注册表 | 新增消息类型要改核心代码 |
| 4 | RWMutex + 只锁快照 | 广播期间阻塞注册 |
| 5 | 100 worker 分片 | 单协程扇出瓶颈 |
| 6 | sync.Pool 复用切片 | 大对象分配打爆 GC |
| 7 | 非阻塞投递 + 丢弃 | 慢客户端拖垮整个房间 |
| 8 | done + Once 退出 | send on closed channel |
| 9 | 心跳在线数 | 崩溃后人数虚高 |
| 10 | 点赞本地聚合 | Redis 写被打满 |
| 11 | Async Producer 调参 | Kafka 吞吐与延迟 |
| 12 | channel 解耦 + 攒批 | MySQL 单条写入开销 |

---

上一篇：[01 架构与四条链路](01-architecture.md) ｜ 下一篇：[03 代码地图](03-code-map.md)
