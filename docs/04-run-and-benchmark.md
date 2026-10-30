# 04 · 本地跑通与压测

## 0. 环境要求

| 组件 | 版本 | 备注 |
|------|------|------|
| Go | **1.25+** | `go.mod` 写的是 `go 1.25.0`，低于此版本直接报错 |
| Docker + Compose | 任意较新版本 | 起 Redis / MySQL / ZK / Kafka |
| 系统 | Linux / macOS / WSL2 | 6 万连接必须在 Linux 内核下调参 |

```bash
# Ubuntu/Debian 装 Go 1.25
wget https://go.dev/dl/go1.25.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.25.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version
```

---

## 1. 起中间件

```bash
git clone https://github.com/cyberspace-cs/live-stream-danmaku.git
cd live-stream-danmaku
docker-compose up -d
docker-compose ps
```

四个容器：`danmaku_redis:6379`、`danmaku_mysql:3306`、`danmaku_zookeeper:2181`、`danmaku_kafka:9092`。

验证：

```bash
redis-cli ping                          # PONG
mysql -h127.0.0.1 -uroot -proot -e "show databases;"   # 能看到 danmaku_db
```

> **Kafka 就绪需要 20~40 秒**（ZK 先起，Kafka 再注册）。立刻启动 Go 服务会 `panic: Failed to start Kafka async producer`。等 `docker logs danmaku_kafka | grep "started"` 出现再继续。

---

## 2. 启动三个进程

```bash
# 终端 1：WS 服务（会 AutoMigrate 建表）
go run cmd/server/main.go -port 8080

# 终端 2：Kafka 消费者（落盘）
go run cmd/consumer/main.go

# 终端 3：CLI 客户端
go run cmd/client/main.go -uid 1001 -room 1001 -port 8080
```

CLI 里直接输入文字就是发弹幕，输入 `1` 是点赞。

**表结构**由 `internal/infra/db.go` 的 `AutoMigrate(&model.DanmakuMessage{})` 自动创建：

```sql
CREATE TABLE danmaku_messages (
  id        bigint unsigned AUTO_INCREMENT PRIMARY KEY,
  room_id   varchar(50)  NOT NULL,
  user_id   bigint unsigned NOT NULL,
  content   varchar(500) NOT NULL,
  send_time datetime(3)  NOT NULL,
  KEY idx_room_time (room_id, send_time),   -- 支撑「按房间按时间游标翻页」
  KEY idx_danmaku_messages_user_id (user_id)
);
```

---

## 3. 用浏览器验证（最快）

开两个标签页，各自在 Console 里执行：

```js
const ws = new WebSocket('ws://localhost:8080/ws?uid=1001&name=alice&room=1001');
ws.onopen = () => console.log('connected');
ws.onmessage = e => console.log('recv:', JSON.parse(e.data));

// 发一条弹幕（room_id 可以省，服务端以连接上下文为准）
ws.send(JSON.stringify({ type: 101, room_id: '1001', data: { content: 'hello 弹幕' } }));

// 点 10 个赞
ws.send(JSON.stringify({ type: 103, room_id: '1001', data: { count: 10 } }));
```

预期：

- 两个标签页**都能**收到自己发的弹幕（因为走了 Redis 回环）。
- 3 秒内收到 `{"type":102,"room_id":"1001","data":{"online":2,"likes":10}}`。
- 第二个标签页关闭后，下一轮统计 `online` 变 1。

验证落盘：

```bash
docker exec -it danmaku_mysql mysql -uroot -proot danmaku_db \
  -e "select count(*), room_id from danmaku_messages group by room_id;"
```

---

## 4. 压测

```bash
# 先放大文件描述符，否则 3 万连接直接 too many open files
ulimit -n 200000

# 6 万连接，每个客户端每秒发 1 条
go run cmd/benchmark/main.go -c 60000 -r 1s

# 只测 Kafka 写入上限
go run cmd/benchmark/kafka/kafka.go
```

### 为什么「6 万连接」是个坎：端口数学

Linux 客户端默认临时端口范围 `net.ipv4.ip_local_port_range = 32768 60999`，共 **28232** 个。

TCP 连接由四元组 `(源IP, 源端口, 目的IP, 目的端口)` 唯一确定。压测机 IP 固定、服务端 IP:Port 固定时，**最多只能建立约 2.8 万条连接**，而不是 6 万。

要突破，必须满足其一：

```bash
# 方案 A：扩大端口范围（最多约 6.4 万）
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1

# 方案 B：给压测机加多个 IP（每个 IP 各自有 2.8~6.4 万端口）
sudo ip addr add 127.0.0.2/8 dev lo
```

> 所以作者说的「受限于本地端口/内存」是准确的。**在同一台机器上同时跑服务端和压测端时，两端共享同一个端口空间，实际上限更低**。想要真 6 万+，服务端和压测端要分开机器。

### 服务端也要调（否则先崩在这）

```bash
sudo sysctl -w fs.file-max=2000000
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
# 进程级
ulimit -n 200000
```

### 观察点

```bash
# 连接数
ss -s ; ss -tn state established '( sport = :8080 )' | wc -l
# 进程 goroutine / 内存
curl -s localhost:8080/debug/pprof/goroutine?debug=1 | head   # 需自行注册 pprof
# Redis 广播量
redis-cli info stats | grep total_commands_processed
# Kafka 积压
docker exec danmaku_kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group <groupID>
```

---

## 5. 我踩过 / 预判会踩的坑

| # | 现象 | 原因 | 解决 |
|---|------|------|------|
| 1 | `panic: Failed to start Kafka async producer` | Kafka 还没就绪 | 等 30s，或加重试 |
| 2 | `Authentication plugin 'caching_sha2_password' cannot be loaded` | MySQL 8 默认认证插件 | compose 里已加 `--default-authentication-plugin=mysql_native_password` |
| 3 | `go: module requires Go 1.25` | 本地 Go 版本低 | 升级到 1.25 |
| 4 | 压测到 2.8 万就上不去 | 本地端口耗尽 | 见上面「端口数学」 |
| 5 | `too many open files` | `ulimit -n` 太小 | `ulimit -n 200000` |
| 6 | 改了仓库名后编译报错 | `go.mod` 的 module 是 `github.com/charlesAcmen/livestream-danmaku`（**无连字符**），仓库名是 `live-stream-danmaku` | 目录名随便叫都行；真要改 module 名，必须全局替换所有 import |
| 7 | 消息发出去了但库里没有 | 消费者没启动，或 topic 未创建 | 先起 `cmd/consumer`；确认 broker 的 `auto.create.topics.enable=true` |
| 8 | 两个客户端都收到两次 | 你自己在 `handleBroadcast` 里又加了一次本地广播 | 删掉，只保留 Redis 回环这一条路径 |
| 9 | 在线人数一直是 0 | `broadcastStats` 里 Redis 写失败被吞了（2s 超时） | 看日志 `[REDIS INFRA] Failed to report server stats` |
| 10 | 点赞数不涨 | 点赞只进内存，等 3 秒周期 | 等一个 tick，或看 `[MANAGER] Room Stats` 日志 |

---

## 6. 一键自检脚本

```bash
#!/usr/bin/env bash
set -e
echo "== 中间件 =="
redis-cli ping
docker exec danmaku_mysql mysqladmin -uroot -proot ping

echo "== 服务 =="
curl -s -o /dev/null -w "ws endpoint: %{http_code}\n" "http://localhost:8080/ws"

echo "== 数据 =="
docker exec danmaku_mysql mysql -uroot -proot danmaku_db \
  -e "select count(*) as total from danmaku_messages;"
```

`/ws` 返回 `400`（缺 uid/room）是**正常**的，说明路由通了。

---

上一篇：[03 代码地图](03-code-map.md) ｜ 下一篇：[05 设计边界与改进清单](05-limits-and-improvements.md)
