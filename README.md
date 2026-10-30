# live-stream-danmaku 源码学习笔记

> 拆解 [charlesAcmen/live-stream-danmaku](https://github.com/charlesAcmen/live-stream-danmaku)：一个 Go + Redis + Kafka + MySQL 的直播弹幕后端，单机 6 万长连接、150 万 QPS 下行广播。

## 这个仓库是什么

| 仓库 | 说明 |
|------|------|
| [live-stream-danmaku](https://github.com/cyberspace-cs/live-stream-danmaku) | 原项目的 fork，代码原样保留，用于对照阅读 |
| **live-stream-danmaku-notes**（本仓库） | 我自己的学习笔记：架构拆解、关键代码、跑通步骤、设计边界与改进清单 |

**源码规模**：21 个 Go 文件，约 2670 行。核心在 `internal/ws/manager.go`（443 行）和 `internal/consumer/handler.go`（446 行）。

**技术栈**：Go 1.25 / Gin / Gorilla WebSocket / Redis Pub-Sub / Kafka(Sarama) / MySQL 8(GORM) / Zap。

## 一句话总结架构

> 上行只做两件事：**写 Kafka 存起来**、**发 Redis 广播出去**；谁都不在本地直接扇出，而是统一由「Redis 订阅回调」触发本地扇出，天然避免了本机用户收到重复消息。

```
写路径：WS ─► Manager.Broadcast ─┬─► Kafka (持久化缓冲)
                                  └─► Redis Pub/Sub (跨机分发)
读路径：Redis 订阅 ─► 本地房间客户端切片 ─► 100 个 worker ─► 各自 Send chan(4096) ─► 写回 WS
统计：  3s 定时器 ─► 上报本机在线数(SETEX 7s) ─► 聚合全局 ─► 只广播本机客户端
落盘：  Kafka ─► ConsumerGroup ─► msgChan(2000,背压) ─► 攒批 ─► 批量 INSERT
```

## 关键数据（作者本地 WSL2 实测）

| 指标 | 数值 | 瓶颈在哪 |
|------|------|----------|
| 长连接 | 6 万+ | 本地临时端口范围（32768–60999 只有约 2.8 万个/单目标） |
| 下行广播 | 150 万 QPS | 单机 CPU 与网卡 |
| 上行处理 | 2.5 万 QPS | Redis Publish + Kafka 写入 |
| 积压落盘 | 500 万条 / 2 分钟（约 40k QPS） | MySQL 批量 INSERT |

## 文档索引

| 文档 | 内容 |
|------|------|
| [01 架构与四条链路](docs/01-architecture.md) | 上行 / 下行 / 统计 / 落盘四条链路的完整时序与代码位置 |
| [02 性能设计的 12 个手法](docs/02-performance-tricks.md) | 逐条拆解：分片 worker、sync.Pool、非阻塞投递、心跳在线数、双缓冲落盘…… |
| [03 代码地图与阅读顺序](docs/03-code-map.md) | 按 6 轮循序渐进的读码路线，附每个文件的关键点 |
| [04 本地跑通与压测](docs/04-run-and-benchmark.md) | docker-compose、启动顺序、连接数上限的数学解释、常见坑 |
| [05 设计边界与改进清单](docs/05-limits-and-improvements.md) | 13 条可改进点：Pub/Sub 不可靠、at-most-once、点赞丢失、写放大…… |
| [06 面试怎么讲这个项目](docs/06-interview-notes.md) | STAR 讲法 + 四个难点的标准答法 + 追问防御 |
| [07 七天动手计划](docs/07-seven-day-plan.md) | 从跑通到改造，每天一个可交付的小目标 |

## 快速开始

```bash
git clone https://github.com/cyberspace-cs/live-stream-danmaku.git
cd live-stream-danmaku && docker-compose up -d

go run cmd/server/main.go -port 8080     # 终端 1：WS 服务
go run cmd/consumer/main.go              # 终端 2：Kafka 落盘
go run cmd/client/main.go -uid 1001 -room 1001 -port 8080   # 终端 3：CLI 客户端
```

## 声明

本项目代码版权归原作者 **charlesAcmen** 所有（MIT License）。本仓库仅存放个人学习笔记，不含源码副本；笔记中的结论基于对 fork 版本 `bc6c8ff` 的静态阅读，未做全量压测复现。
