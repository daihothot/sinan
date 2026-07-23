# 量化交易系统 7 层目标架构设计文档

## 1. 系统定位

本系统定位为一个 **event-driven multi-service quant trading architecture**，面向黄金 `XAUUSD` 与比特币 `BTCUSD` 的中低频量化交易研究、模拟盘验证与后续实盘执行。

核心方向：

```text
交易品种：XAUUSD + BTCUSD
策略类型：趋势交易为主，中期统计套利为辅
起始周期：H1 / H4
执行路径：MT5 模拟盘 → 小资金实盘 → 扩展策略组合
```

核心原则：

```text
MQL5 不做策略核心
Compute Services 不做流程控制
Strategy & Decision Control Plane 做慢决策、AI 编排和研究工作流
Trading Core 做低延迟交易内核、强风控、执行状态机和 broker adapter routing
Trading Core State Store / append-only log 是执行状态与执行事实的本地强一致存储
Redis Streams 可做跨服务事件分发和异步审计 fanout，但不是执行状态唯一来源
Risk Engine 是 Trading Core 内的执行前硬边界
Execution Event 是执行事实来源，执行状态是事实流投影
Agent / LLM 只能辅助，不直接下单
外部研究 / 回测服务只做研究、分析、审查、候选生成
```

---

## 2. 总体架构

```text
                          ┌──────────────────────────────────┐
                          │ Debug / Test UI                   │
                          │ TUI / Desktop / Dashboard         │
                          └───────────────┬──────────────────┘
                                          │ WebSocket / HTTP
                                          │ state / summary / manual action
                                          ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ Strategy & Decision Control Plane                                          │
│ decision workflow / Agent / rule engine / human review / TradeIntent       │
└───────────────┬───────────────────────────────────────────────▲────────────┘
                │ HTTP POST /trade-intents                      │ WS /events
                │ HTTP GET /state /time                         │ summaries
                ▼                                               │
┌────────────────────────────────────────────────────────────────────────────┐
│ Trading Core                                                               │
│                                                                            │
│  ┌─────────────────────────── Trading Gateway ──────────────────────────┐  │
│  │ TransportAdapter: Native TCP / Execution WS / Event WS / HTTP        │  │
│  │ GatewayInboundRouter / GatewayOutboundRouter                         │  │
│  └───────────────┬───────────────────────────────┬──────────────────────┘  │
│                  │                               │                         │
│  ┌───────────────▼──────────────┐  ┌─────────────▼─────────────────────┐   │
│  │ Risk Engine                  │  │ Execution Engine                  │   │
│  │ hard risk gate               │  │ plan / command / lifecycle        │   │
│  └───────────────┬──────────────┘  └─────────────┬─────────────────────┘   │
│                  │                               │                         │
│  ┌───────────────▼───────────────────────────────▼─────────────────────┐   │
│  │ Trading Core State Store                                            │   │
│  │ SQLite / append-only log / idempotency / reconciliation / spool      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└───────────────┬───────────────────────────────┬────────────────────────────┘
                │                               │
                │ Native TCP / Execution WS     │ Redis Streams / audit fanout
                │ Execution Client Protocol     │ optional, not source of truth
                ▼                               ▼
┌──────────────────────────────┐   ┌────────────────────────────────────────┐
│ Execution Client / Adapter    │   │ Long-term Audit / Replay / Research    │
│ MT5 / Exchange / Paper         │   │ Postgres / ClickHouse / object store   │
│ command inbox / broker bridge  │   └────────────────────────────────────────┘
└───────────────┬──────────────┘
                │
                │ broker API / terminal API
                ▼
┌──────────────────────────────────┐
│ Broker / Exchange / MT5 Terminal │
└──────────────────────────────────┘

Strategy & Decision Control Plane
  └── HTTP calls
      ▼
┌────────────────────────────────┐
│ Compute & Research Services     │
│ indicators / IC / backtest / ML  │
└────────────────────────────────┘
```

关键方向：

```text
Execution Client 通过 Execution Client Protocol 接入 Trading Gateway；transport binding 可以是 Native TCP 或 Execution WebSocket。
Strategy & Decision Control Plane 只通过 HTTP / WS 接入 Trading Core。
Debug / Test UI 默认连接 Strategy & Decision Control Plane；测试 / 故障场景允许只读直连 Trading Core。
Debug / Test UI 不直接连接 Execution Client。
Debug / Test UI 直连 Trading Core 必须使用独立 debug/read-only credential，默认只允许 GET /state 和 WS /events。
Compute & Research Services 由 Strategy & Decision Control Plane 调用，不直接参与 execution.command。
Compute Services 返回的 position sizing 结果只能作为研究或决策建议；最终可执行 lots 必须由 Trading Core 内的 Risk Layer 本地、确定性地计算和批准。
Redis Streams / audit fanout 是分发和审计辅助，不是执行事实来源。
Trading Core State Store 是执行状态、idempotency 和 reconciliation 的本地强一致来源。
```

Debug / Test UI 有两条路径：

```text
normal path:
  Debug UI → Strategy & Decision Control Plane
  → 查看 workflow / agent / research / manual review state

break-glass read-only path:
  Debug UI → Trading Core GET /state / WS /events
  → 只读查看 execution summary / system.event / session health
  → Control Plane 挂掉时仍能诊断 Trading Core 和 Execution Client
```

---

## 3. 七层职责定义

### 3.1 MT5 Adapter Layer

#### 技术形态

```text
MQL5 Expert Advisor
TCP execution client
MT5 account / symbol / order adapter
local terminal guard
```

#### 职责

```text
连接 MT5 交易终端
采集 market.tick / market.bar
采集 symbol metadata / broker trading constraints
向 Gateway 推送市场事件
向 Gateway 推送 symbol.metadata
接收 execution.command
执行订单或模拟执行
上报 execution.event
在关键时机发布 account.snapshot / position.snapshot / order.snapshot
记录本地兜底日志
维护本地 command 去重缓存
维护连接状态
发送心跳
执行 time sync，维护 effective_server_now_ms
断线重连
处理基础拆包 / 粘包
维护本地 send queue / command inbox
```

#### MQL5 Execution Client 运行模型约束

MQL5 Adapter 的协议实现必须尊守每个 EA 实例的串行事件回调模型。这不表示 MT5 终端全局只有一个线程，而是表示单个 Execution Client EA 不能假设自己拥有可并行推进协议状态的后台 worker。

```text
每个 EA 实例通过单一事件队列串行处理 OnInit / OnTimer / OnTick / OnTradeTransaction / OnDeinit。
实现不得假设回调并行或可重入，不得创建或依赖后台 worker / thread。
协议状态、frame buffer、command / execution journal、send queue 和 command inbox 只能在该串行事件模型内推进。

OnInit
  → 加载配置与本地 command / execution journal
  → 初始化协议状态、frame buffer、send queue、command inbox 与 timer
  → 不得通过循环连接或等待网络就绪阻塞初始化

OnTimer
  → 是 connect / session.hello / auth / time sync / socket read / framing / execution.command receive / socket write / transport.ack / heartbeat / timeout detection / reconnect 的唯一 network liveness owner
  → 负责有界地 drain command inbox 与 send queue
  → 即使市场没有 tick，也必须维持连接、time sync、heartbeat 和 reconnect

OnTick
  → 只采集并合并最新 market.tick、识别已关闭 market.bar，并将待发送市场事件入队
  → 不负责 connect / socket read / socket write / heartbeat / reconnect
  → 协议活性不得依赖市场 tick

OnTradeTransaction
  → 捕获 broker 订单、成交和持仓状态变化
  → 先更新本地 execution journal，再将 execution.event 及受影响 snapshot 入队
  → 不直接执行 socket I/O

OnDeinit
  → 停止 timer，持久化本地 journal，关闭 socket
  → 只允许有界 best-effort flush，不得无限等待网络或队列清空
```

`OnTimer` network pump 必须是可中断、可恢复的有界状态机：

```text
每次 pump 同时受以下三个本地配置上限约束：
  max_pump_messages_per_turn
  max_pump_bytes_per_turn
  max_pump_duration_ms

达到任一上限即结束本轮 OnTimer，保留 partial frame buffer 和队列游标，下一次 OnTimer 继续。
max_pump_duration_ms 只用本地 monotonic clock 计量，不写入业务 payload。
socket read / write 必须非阻塞，或使用不超过本轮剩余预算的有限 timeout。
禁止 Sleep、等待数据的 busy loop、无界 read / write / parse，以及无界 drain command inbox / send queue。
网络突发或停滞不得长时间饿死 OnTick / OnTradeTransaction。
未处理的 durable command / execution event 不得因本轮预算耗尽而丢弃；market.tick 按 latest-only 语义合并。
```

#### 不负责

```text
策略判断
指标计算
组合风控
Agent 决策
多策略调度
组合仓位管理
长期审计存储
```

#### 设计结论

MQL5 层不再是完整 EA Framework，而是：

```text
MT5 execution adapter
```

它是交易终端适配器，不是策略大脑。

更一般地说，MQL5 只是 `Execution Client` 的一种实现。后续 Binance、IBKR、Paper Trading、Backtest Executor 等其他执行平台也可以实现同一套 Execution Client Protocol 接入 Gateway。

---

### 3.2 Trading Core Layer

#### 技术形态

```text
Rust implementation / single binary first
WS / HTTP API for UI and Strategy & Decision Control Plane
Execution Client Protocol server
Native TCP / Execution WebSocket transport binding
Execution Client session registry
broker adapter router
heartbeat monitor
time sync authority
transport message framing
hard Risk Engine
Execution Engine
Command Lifecycle State Machine
SQLite / append-only State Store
local spool / audit writer
```

#### 服务边界

Trading Core 是交易正确性边界，不是 Strategy & Decision Control Plane 的内部 helper。

```text
Strategy & Decision Control Plane
  ↔ HTTP / WS TradeIntent API
Trading Core
  ↔ Execution Client Protocol
MT5 Adapter / Exchange Adapter
```

#### 职责

```text
接收 Strategy & Decision Control Plane 产生的 TradeIntent / StrategyDecision
读取最新 account / position / order / symbol metadata / market snapshot
组装完整 RiskRequest / position sizing candidates
执行强风控 hard risk gate
本地确定性计算并批准最终 lots
从上游有效期和 execution policy 派生 execution.command.expires_at
生成 execution.plan / execution.command
维护 command / plan lifecycle state
维护 SQLite / append-only execution event store
维护 command idempotency journal
维护 Execution Client 与后端的双向 TCP 连接
接收 Execution Client 上报的 market event
向 Execution Client 下发 execution.command
维护 session_id / client_id / platform / terminal_id / account_id 映射
执行心跳检测
提供 time.sync.request / response，并作为 Execution Client Protocol 的时间权威
维护连接健康状态
执行消息 framing encode / decode
管理 command delivery ack timeout
处理 DELIVERY_UNCONFIRMED / reconciliation
执行 broker adapter routing
维护 execution.event / system.event local append-only spool
向 Strategy & Decision Control Plane / UI / Redis 发布聚合事件和状态变化
```

#### 不负责

```text
策略判断
指标计算
LLM / Agent 推理
宏观分析
研究 / 回测编排
策略候选 promotion
解释性报告生成
```

#### Strategy & Decision Control Plane 与 Trading Core 的关系

```text
Strategy & Decision Control Plane 不直接维护 MT5 / Execution Client TCP 连接
Strategy & Decision Control Plane 不直接生成最终 execution.command
Strategy & Decision Control Plane 只提交 TradeIntent / StrategyDecision / manual approval
Trading Core 负责最终 risk gate、command 生成、dispatch、state projection
Trading Core 负责上报 EXECUTION_CLIENT_CONNECTION_LOST / RESTORED 等 system.event
Trading Core 可以拒绝任何过期、不匹配、不安全或不可确认的 intent
```

#### Trading Core 对 Strategy & Decision Control Plane 的 API

Strategy & Decision Control Plane 调用 Trading Core 时提交的是 `TradeIntent`，不是最终订单，也不是同步执行函数。

推荐 API：

```text
POST /trade-intents
  → 输入 TradeIntent
  → 返回 intent_id / status / reason / correlation_id
  → status = ACCEPTED / RISK_BLOCKED / REJECTED / DUPLICATE

GET /state
  → 返回 Trading Core 当前 accounts / positions / orders / risk / sessions summary
  → 只读，用于 Strategy & Decision Control Plane 决策上下文，不作为执行事实来源替代

GET /time
  → 返回 Trading Core server_now_ms、server_receive_at、server_send_at、clock_health、max_internal_server_skew_ms
  → 返回 max_decision_time_skew_ms、max_decision_time_sync_age_ms、max_decision_time_sync_rtt_ms、control_plane_time_sync_interval_ms、max_decision_intent_age_ms
  → Strategy & Decision Control Plane 用于生成 StrategyDecision.timestamp / TradeIntent.requested_at / signal_expires_at
  → server_now_ms 必须等于 server_send_at，避免响应中出现两个权威时间

WS /events
  → 推送 market.snapshot / risk.summary / execution.summary / system.event
  → 不推送每个 raw tick 给 Strategy & Decision Control Plane
```

规则：

```text
POST /trade-intents 只表示 Trading Core 已接收并完成初步校验，不表示 broker 已成交。
Trading Core 必须在本地 state store 中先记录 `TradeIntent.intent_id` / `TradeIntent.idempotency_key`，再进入 hard risk 和 execution flow。
Strategy & Decision Control Plane 不得通过 HTTP retry 造成重复执行；重复 `intent_id` 或 `idempotency_key` 必须由 Trading Core 幂等处理。相同 `idempotency_key` 对应不同 payload 时必须返回 `IDEMPOTENCY_KEY_CONFLICT`，不得进入 hard risk 或 execution flow。
Broker 生命周期只通过 WS /events 或 event fanout 返回。
```

#### Trading Gateway Transport Abstraction

Trading Gateway 需要支持多种 transport，但 transport 不是业务边界。TCP、WebSocket、HTTP 都只是 adapter，后面必须进入同一套 Trading Core command bus / query bus / event bus。

设计原则：

```text
Transport Adapter 负责连接、鉴权、framing、读写队列、backpressure、基础 ACK 和连接健康。
Protocol Router 负责把 wire message 映射为 Trading Core 内部 command / query / event。
Trading Core domain module 负责 risk、execution、state projection、idempotency 和 reconciliation。
Transport Adapter 不得直接调用 broker，不得直接改 execution state，不得绕过 Risk Layer。
```

推荐抽象：

```text
TransportAdapter
  → start / stop
  → accept connection or request
  → authenticate
  → decode inbound envelope
  → encode outbound envelope
  → enforce max frame / payload size
  → manage send queue and backpressure
  → report transport health

SessionContext
  → transport_type = TCP / WEBSOCKET / HTTP
  → session_id
  → client_id
  → account_id
  → terminal_id
  → platform
  → authenticated_at
  → capabilities
  → remote_addr

GatewayInboundRouter
  → compose small pipeline components
  → no business state mutation

GatewayOutboundRouter
  → compose small pipeline components
  → no command lifecycle mutation
```

Router 实现约束：

```text
GatewayInboundRouter / GatewayOutboundRouter 只是 pipeline composition root。
每个 stage 必须可以独立单测。
新增业务类型时优先新增 handler / stage，不在 Router 主体里堆 if / switch。
Router 只返回 routing result / delivery result，不返回 risk result 或 execution state transition。
```

Router 组件拆分：

```text
InboundDecodeStage
  → decode envelope / payload
  → reject invalid frame or JSON

InboundSchemaStage
  → validate schema_version / message type / required fields
  → send invalid message to deadletter.event

InboundAuthStage
  → validate connection auth / API key / token
  → attach authenticated principal

InboundSessionStage
  → enforce session_id / client_id / account_id / terminal_id binding
  → attach SessionContext

InboundTimeStage
  → attach received_at = server_now_ms
  → validate sent_at skew where applicable

InboundDispatchStage
  → route to Trading Core command / query / event handler
  → no domain decision inside router

OutboundSelectStage
  → select active session or event subscribers

OutboundAuthzStage
  → enforce topic / account / client visibility

OutboundDeliveryStage
  → apply transport delivery policy
  → report delivery result to Execution Layer or event stream manager

OutboundEncodeStage
  → encode envelope / payload through selected TransportAdapter
```

Router anti-patterns:

```text
GatewayInboundRouter / GatewayOutboundRouter 不得直接访问 broker。
GatewayInboundRouter / GatewayOutboundRouter 不得执行 risk check。
GatewayInboundRouter / GatewayOutboundRouter 不得生成 execution.command。
GatewayInboundRouter / GatewayOutboundRouter 不得直接更新 ExecutionCommandState。
GatewayInboundRouter / GatewayOutboundRouter 不得持有跨请求业务状态；状态必须在 Session Registry、Event Stream Manager 或 Trading Core State Store。
```

transport 类型：

```text
Native TCP Transport
  → Execution Client Protocol binding
  → MT5 / low-latency local adapter / paper executor
  → full-duplex
  → length-prefixed JSON frame
  → supports transport ack / command.received / execution.event / heartbeat / time sync

Execution WebSocket Transport
  → Execution Client Protocol binding
  → exchange adapter / cloud adapter / environment where WebSocket is easier than raw TCP
  → full-duplex
  → one WebSocket message carries one WireMessage JSON
  → supports transport ack / command.received / execution.event / heartbeat / time sync
  → may carry execution.command

Event WebSocket Transport
  → UI / Debug Tool / Strategy & Decision Control Plane event stream
  → subscription-oriented
  → pushes market.snapshot / risk.summary / execution.summary / system.event
  → may accept subscribe / unsubscribe / ping / cursor resume
  → must not carry execution.command

HTTP Transport
  → request / response API
  → POST /trade-intents
  → GET /state
  → GET /time
  → optional internal refresh / admin endpoints
  → must not carry execution.command
```

可靠性语义：

| Transport | 主用途 | 可靠性语义 | 可否承载 execution.command |
|---|---|---|---:|
| Native TCP | Execution Client Protocol binding | session sequence + transport ack + command.received + idempotency journal | 是 |
| Execution WebSocket | Execution Client Protocol binding | session sequence + transport ack + command.received + idempotency journal | 是 |
| Event WebSocket | event stream / dashboard / Control Plane feedback | event_id cursor resume；断线后用 `/state` 校准 | 否 |
| HTTP | intent submit / query | request idempotency；服务端 state store 为准 | 否 |

Event WebSocket 与 HTTP 不得提供绕过 Trading Core 的执行通道。即使后续允许 WebSocket 写入人工操作，也只能写 manual action / audit request / review decision，不能写 `execution.command` 或 broker order。

Execution WebSocket 与 Event WebSocket 必须是不同 endpoint、不同 auth scope、不同 router。Execution WebSocket 面向 Execution Client，可以承载 `execution.command`；Event WebSocket 面向 UI / Control Plane，只能订阅事件和提交受审计的非执行操作。

推荐 endpoint 命名：

```text
Native TCP listener
  → 0.0.0.0:<execution_client_tcp_port>
  → Execution Client Protocol only

WS /execution-client
  → Execution WebSocket
  → Execution Client Protocol only

WS /events
  → Event WebSocket
  → UI / Control Plane subscriptions only

HTTP /trade-intents /state /time
  → Control Plane / Debug query API
```

Execution Client transport binding 运行约束：

```text
Native TCP 与 Execution WebSocket 必须复用同一个 session registry、client authenticator、inbound admission port、resume admission port、outbound sink contract，以及 Gateway 级 connection / pending-handshake semaphore。
max_connections / max_pending_handshakes / max_frame_bytes / max_message_bytes / outbound_queue_capacity / handshake_timeout / write_timeout / inbound_admission_timeout / event_write_timeout 必须显式配置且有界；shutdown 必须有 grace bound 和 cancel fallback，不得无界等待 connection task。
inbound_admission_timeout 必须短于 handshake_timeout，使及时发送 session.hello 时内部 resume admission timeout 在总 deadline 前具有独立上限；若仍有写预算则 best-effort 返回 typed session.rejected，否则直接 fail closed。总 deadline 始终覆盖 upgrade / hello read / auth / resume admission / durable activation / accepted write 的完整握手。
一个 authenticated connection 只能有一个 transport reader 和一个 transport writer；所有 Gateway → Client frame，包括 session.accepted、transport.ack、time.sync.response、execution.command 和 reconciliation.request，都必须经过该 writer，禁止并发直接写 socket。
session.accepted 固定占用 Gateway outbound sequence=1，且真实 write 完成后连接才进入 active message loop；bootstrap 失败必须关闭 durable/live session。
control message、execution.command 和 reconciliation.request 共用 execution_client_sessions.last_outbound_sequence；writer 按已 reserve sequence 输出，允许有界重排但禁止越过缺口。
durable reserve 后因 expiry / route / clock / encode 等原因明确不产生 wire frame时，必须提交显式 sequence skip；未解释的 gap、重复 sequence、reorder overflow、queue backpressure 或 gap timeout 必须 fail closed 并关闭当前 session。
time.sync.response 的 server_send_at / server_time / envelope.sent_at 必须在单 writer 即将执行实际 transport write 前从 server clock 同次采样，不能在进入用户态队列时提前采样。
Native TCP 使用 4-byte unsigned big-endian length prefix；Execution WebSocket 严格使用一个 Text message 对应一个 WireMessage。Binary message、raw frame、零长度、超限长度、非 UTF-8 或不完整 frame 都必须 fail closed。
Execution WebSocket endpoint 固定为 /execution-client；/events 使用独立 listener/router/auth scope，不能复用 execution-client upgrade path。
```

Inbound admission 与 ACK 约束：

```text
Transport adapter 只能在 envelope、schema、direction、authenticated identity、session sequence 和 payload identity 全部通过后调用 handler-specific inbound admission port。
InboundAdmission.Accepted 表示消息已进入 crash-recoverable、幂等的 durable handler path；只进入内存队列不满足该承诺。
只有 durable admission 返回 Accepted / Duplicate / typed Rejected 后才能发送 transport.ack；admission error、timeout 或 task cancellation 不发送 ACK，并关闭当前 session。
Transport binding 只报告 wire admission / write evidence，不得修改 ExecutionCommandState，不得决定 retry 或 reconciliation disposition。
session.hello.resume 非空时必须在 session.accepted 前完整交给 durable resume admission port；未配置 handler、admission error 或 timeout必须拒绝握手。resume cursor 只用于 gap 诊断和 reconciliation，禁止触发 execution.command 自动重放。
```

Production admission 不能只在旧 `wire_inbox` 中保存 payload hash。生产实现必须保存完整 canonical wire envelope、authenticated route identity、schema、sequence、received_at 和处理状态，并提供 lease + revision CAS 的 pending dispatcher；`Accepted` 可以在 durable `PENDING` 提交后返回，不要求在 ACK 前完成所有业务 projection，但进程重启后必须能够重新 claim。相同 message identity、route 和 canonical envelope 返回 `Duplicate`；复用 `message_id` 或 `(session_id, sequence)` 且 payload/identity 漂移时，必须持久化稳定 rejection 并返回 typed `Rejected`。

Production dispatcher 必须按 message type 进入 handler-specific 幂等 transaction。typed payload decode/schema 失败必须持久化 `deadletter.event` 和 bounded raw evidence，并把 admission 标记为 terminal failure；不得把失败消息当成有效事实。Execution receipt/event 与 Reconciliation result 只有在其领域事实和相关 projection 能在同一 owner transaction 中提交时才可标记 `HANDLED`，禁止用多个非原子 public Store 调用冒充完整 composition。

resume admission 必须单独保存完整 cursor 和 authenticated identity，并具有同样的 pending/lease/revision recovery。worker 只能据 cursor 创建 gap diagnosis / reconciliation work；无论首次处理还是 crash recovery都禁止自动 replay `execution.command`。

`TransportEventPort` 的生产 adapter 必须把协议/decode/schema/frame 类问题写入 deadletter，其余连接、认证、clock、liveness 问题写入 system event，并可在 fact 成功后发布 bounded summary。raw payload 只允许按显式上限截断保存，同时保存原始长度；token、client auth secret 和 command HMAC 不得进入 detail、metadata、deadletter 或 summary。持久化 error/timeout 必须可观测，不能被静默当作成功。

#### WS /events 重连恢复策略

`WS /events` 是 Trading Core → Strategy & Decision Control Plane / UI 的事件推送通道。它不是执行事实来源，也不为每个 consumer 无限保留私有队列。

推荐语义：

```text
Trading Core State Store
  → 保存执行事实、command state、summary projection

Event Stream Manager
  → 从 State Store / projection / local fanout log 读取事件
  → 推送给 WebSocket subscribers
  → 维护短期 event cursor window
  → 不拥有执行事实

WebSocket subscriber
  → 连接时声明 topics / account scope / last_event_id
  → 收到事件后按 event_id 更新本地 cursor
  → 断线后用 last_event_id resume
```

断线期间事件处理：

```text
结论：不是简单全丢，也不是为每个 consumer 无限 spool。
durable execution facts 必须写 Trading Core State Store。
summary events 保留 bounded replay window。
ephemeral UI events 可以丢弃。

不为单个 WS client 单独 spool 无限事件。
Trading Core 必须保留 bounded replay window，例如最近 N 条或最近 M 分钟 summary events。
如果 last_event_id 仍在 replay window 内：
  → 从 last_event_id 之后补推事件
  → 然后切回 live stream

如果 last_event_id 已过期或 event gap detected：
  → 返回 resume_failed / gap_detected
  → client 必须调用 GET /state 校准当前 summary
  → client 再用新的 cursor 订阅 live stream
```

事件类型分级：

```text
durable execution facts
  → execution.event / command.received / execution.command.state
  → 来源是 Trading Core State Store
  → 可通过 GET /state 或 summary 重建

summary events
  → execution.summary / risk.summary / market.snapshot / system.event
  → 可 replay bounded window
  → gap 后用 GET /state 校准

ephemeral UI events
  → dashboard ping / transient progress / local UI hint
  → 可丢弃，不参与恢复
```

Control Plane 恢复规则：

```text
1. WS 断线不影响 Trading Core 执行。
2. Control Plane 重连后先尝试 last_event_id resume。
3. resume 成功时继续处理补推事件。
4. resume 失败时调用 GET /state / GET /time 重新校准 workflow context。
5. Control Plane 不得根据 WS gap 自行推断执行失败或重发 TradeIntent。
6. 对已提交 intent，只能用原 intent_id 查询 Trading Core state 或幂等重放 POST /trade-intents。
```

Event WS 的 replay 顺序不能仅依赖 `created_at / event_id`。`event_stream_log` 必须为每次成功 append 分配数据库内单调递增的 `stream_sequence`；外部 cursor 仍使用不可猜测的 `event_id`，服务端先由该 ID 定位 sequence，再执行 exclusive replay。相同 `event_id` 只有 topic、account、type、canonical payload 和 `created_at` 全部相同才是 Duplicate，任一字段漂移都是 conflict。

订阅从 replay 切换到 live 时必须遵循固定顺序：先建立有界 live receiver，再读取 Store high-water，然后 replay `(cursor_sequence, high_water]`，最后丢弃 live receiver 中 `sequence <= high_water` 的重复项并继续实时推送。这样并发 append 不会落在 replay/live 之间。找不到 retained cursor 时返回 `CURSOR_EXPIRED`；只有服务端已知发生 continuity loss 时才返回 `GAP_DETECTED`。不得通过错误原因泄露一个未授权账户的 cursor 是否存在。

Event Stream Manager 的 retention count、retention age、单次 replay limit、每 subscriber queue capacity、最大 Text message bytes 和 write timeout 都必须是显式的正数配置。replay 超限、broadcast lag、subscriber queue 满或 write timeout 时必须关闭该连接并要求调用方用最后确认的 cursor 恢复；producer 不得被慢消费者阻塞，也不得在丢失 summary 后继续发送更晚 summary 伪装成连续流。

`account_id` 未指定时，只表示 principal 明确授权的账户集合，不表示数据库中的全部账户。账户事件必须落在该集合内；`account_id = null` 的全局 `system.event` / `deadletter.summary` 对任一具有 `event:subscribe` scope 的合法 principal 可见。空账户授权范围仍可接收这类全局事件，但看不到任何 account-bound event。

内部路由表：

```text
Execution Client Protocol inbound
  via Native TCP / Execution WebSocket
  hello
  heartbeat
  time.sync.request
  market.tick / market.bar
  account.snapshot / position.snapshot / order.snapshot / symbol.metadata
  command.received
  execution.event
  reconciliation.result

Execution Client Protocol outbound
  via Native TCP / Execution WebSocket
  session.accepted
  time.sync.response
  execution.command
  reconciliation.request

Event WebSocket outbound
  market.snapshot
  risk.summary
  execution.summary
  system.event
  deadletter.summary

Event WebSocket inbound
  subscribe
  unsubscribe
  ping
  resume cursor
  manual action request, optional and audited

HTTP inbound
  trade.intent submit
  state query
  time query
  optional manual review / admin request
  debug read-only state query
```

实现边界：

```text
Transport layer can fail a delivery attempt.
Transport layer can mark a session stale.
Transport layer can emit system.event for connection / frame / auth failures.
Transport layer cannot decide whether to retry an execution.command.
Transport layer cannot mutate ExecutionCommandState directly; it reports delivery result to Execution Layer.
Execution Layer owns command lifecycle transitions.
```

#### Execution Client Protocol

执行客户端协议是平台无关协议，`WireMessage` 不绑定 TCP。TCP、Execution WebSocket、后续 Unix Domain Socket 或其他 full-duplex transport 都只是 transport binding。

Transport binding：

```text
Native TCP
  → 4-byte big-endian length prefix + UTF-8 JSON WireMessage payload

Execution WebSocket
  → one WebSocket message = one UTF-8 JSON WireMessage payload
  → 不使用 TCP length prefix
  → 仍必须执行 max_message_bytes、schema validation、session binding、transport ack

Event WebSocket
  → UI / Control Plane event subscription protocol
  → 不属于 Execution Client Protocol binding
  → 不承载 execution.command
```

所有 Execution Client transport payload 都必须包含最小 wire envelope，避免 transport ack、重连去重和协议升级依赖业务字段。

```ts
export interface WireMessage<T> {
  message_id: string
  type: ExecutionClientMessageType
  schema_version: string

  client_id?: string
  session_id?: string
  correlation_id?: string
  causation_id?: string

  sent_at?: number
  sequence?: number

  payload: T
}
```

字段规则：

```text
message_id    = transport ack / 去重的最小单位
type          = hello / time.sync.request / time.sync.response / heartbeat / market.tick / execution.command / command.received 等
schema_version = wire payload schema 版本
session_id    = Gateway 接受 hello 后分配的 Execution Client session ID
sent_at       = server-time domain；Gateway 发送时使用真实 server time，Execution Client 在 time sync 后使用估算的 server time
sequence      = 同一 Execution Client session、同一发送方向内单调递增，可用于诊断乱序和断点恢复
```

`hello` 与 `time.sync.request` 发生在客户端完成时间同步之前，允许不带 `sent_at`。`session.accepted` 之后，所有非 time-sync 的业务消息必须带 `sent_at`，且必须是 server-time domain。

#### Server Time Authority 与 Clock Sync

交易系统中任何用于控制、过期、排序、freshness、TTL、审计和风控判断的时间，必须使用服务器时间。客户端本地 wall clock 不得参与交易控制。

权威时间源：

```text
protocol time authority
  → Trading Gateway / Trading Core server time
  → Unix milliseconds UTC
  → Gateway 是 Execution Client Protocol 对外暴露的时间权威

server process clocks
  → Trading Gateway / Risk Engine / Execution Engine / Trading Core State Store 必须使用同一 server time source
  → 单机部署时使用同一主机时钟
  → 多机部署时必须使用 NTP / chrony / cloud time sync
  → 内部 server clock skew 超过 max_internal_server_skew_ms 时不得生成或投递新的 execution.command

Execution Client local wall clock
  → 不可信
  → 不得用于 expires_at / freshness / event_at / observed_at / sent_at

Execution Client local monotonic clock
  → 只允许用于测量 RTT 和本地 elapsed time
  → 不得写入业务 payload
```

所有跨服务时间字段必须使用 Unix milliseconds UTC，不使用本地时区字符串。

```text
sent_at
  → 发送端创建 WireMessage 时的 server-time domain 时间
  → Gateway 发送时使用 server_now_ms
  → Execution Client 发送时使用 effective_server_now_ms

received_at
  → Gateway / server 收到并完成解析的 server_now_ms

created_at
  → server 创建 EventEnvelope 的 server_now_ms

observed_at
  → 外部状态被 Execution Client 观测到时的 effective_server_now_ms

event_at
  → Execution Client 生成 execution.event 时的 effective_server_now_ms

filled_at
  → Execution Client 观测到 broker 成交时的 effective_server_now_ms

broker_filled_at / broker_time
  → broker 原始时间戳，可选，仅用于审计和对账
  → 不参与 freshness、expires_at、TTL 或风控判断
```

规则：

```text
1. Gateway 必须为每条 inbound WireMessage 补 received_at，且 received_at 必须使用 server_now_ms。
2. 所有 freshness 判断使用 server_now_ms - observed_at。
3. 所有 expires_at / valid_until / TTL 判断使用 server_now_ms 或 effective_server_now_ms。
4. latency 观测使用 received_at - sent_at；缺少 sent_at 的 hello / time.sync.request 不参与业务 latency 统计。
5. 如果 abs(received_at - sent_at) > max_clock_offset_ms，写 system.event: CLOCK_SKEW_DETECTED。
6. 对 execution.command，time sync 不健康时不得继续执行新 command，必须先重新同步或人工处理。
7. 推荐 max_clock_offset_ms = 250。
8. 推荐 max_internal_server_skew_ms = 100。
9. bar 去重使用 symbol + timeframe + timestamp，不使用 received_at。
```

Execution Client 必须维护：

```text
server_time_offset_ms
  → server_now_ms - client_monotonic_now_ms

effective_server_now_ms
  → client_monotonic_now_ms + server_time_offset_ms

last_time_sync_at_server_ms
  → 最近一次有效 sample 的 server_mid_ms

last_time_sync_age_ms
  → effective_server_now_ms - last_time_sync_at_server_ms

last_time_sync_rtt_ms
clock_sync_status
  → SYNCED / DEGRADED / UNSYNCED
```

time sync 协议：

```ts
export interface TimeSyncRequest {
  request_id: string
}

export interface TimeSyncResponse {
  request_id: string
  server_receive_at: number
  server_send_at: number
  server_time: number
}
```

time sync 计算：

```text
client records local monotonic t0
  → send time.sync.request

Gateway receives request at server_receive_at
Gateway sends response at server_send_at
  → server_time = server_send_at

client receives response at local monotonic t3

rtt_ms = t3 - t0
client_mid_ms = (t0 + t3) / 2
server_mid_ms = (server_receive_at + server_send_at) / 2
server_time_offset_ms = server_mid_ms - client_mid_ms
effective_server_now_ms = client_monotonic_now_ms + server_time_offset_ms
```

采样规则：

```text
session.accepted 返回 server_time 后，Execution Client 必须立即完成至少 3 次 time.sync sample。
只接受 rtt_ms <= max_time_sync_rtt_ms 的 sample。
初始 offset 使用最低 RTT sample。
运行中每个 heartbeat_interval_ms 至少做 1 次 time.sync sample。
连续 3 次 sample 失败或 last_time_sync_age_ms > heartbeat_timeout_ms 时，clock_sync_status = UNSYNCED。
offset 抖动超过 max_clock_offset_ms 时，clock_sync_status = DEGRADED，并写 CLOCK_SKEW_DETECTED。
clock_sync_status != SYNCED 时，Execution Client 不得接受新的 execution.command。
clock_sync_status != SYNCED 时，Execution Client 不得把 market / snapshot 作为 fresh state 发布。
clock_sync_status != SYNCED 时，Execution Client 可以本地 journal 已在途订单的 broker 回报，但应在恢复 SYNCED 后再上报 execution.event；如果无法重建准确 server-time domain 时间，必须写 TIME_SYNC_UNHEALTHY 并触发 reconciliation。
```

Strategy & Decision Control Plane 也必须维护 Trading Core server-time offset。它可以通过 `GET /time` 或 WS heartbeat 采样，但生成 `StrategyDecision.timestamp`、`TradeIntent.requested_at`、`signal_expires_at` 时必须使用 Trading Core server-time domain。

Control Plane time sync state：

```ts
export interface ControlPlaneTimeSyncState {
  trading_core_time_offset_ms: number
  effective_trading_core_now_ms: number
  last_sample_at_monotonic_ms: number
  last_sample_at_server_ms: number
  last_rtt_ms: number
  last_time_sync_age_ms: number
  clock_health: "HEALTHY" | "DEGRADED" | "UNHEALTHY"
  consecutive_failures: number
}
```

Control Plane time sync 采样：

```text
Control Plane 使用本地 monotonic t0 发起 GET /time。
Trading Core 收到请求后记录 server_receive_at。
Trading Core 发送响应前记录 server_send_at，并返回 time policy。
Control Plane 收到响应时记录本地 monotonic t3。

rtt_ms = t3 - t0
client_mid_ms = (t0 + t3) / 2
server_mid_ms = (server_receive_at + server_send_at) / 2
trading_core_time_offset_ms = server_mid_ms - client_mid_ms
effective_trading_core_now_ms = monotonic_now_ms + trading_core_time_offset_ms
last_sample_at_server_ms = server_mid_ms
last_sample_at_monotonic_ms = t3
```

采样规则：

```text
启动后必须完成至少 3 次 GET /time sample。
只接受 rtt_ms <= max_decision_time_sync_rtt_ms 的 sample。
初始 offset 使用最低 RTT sample。
运行中每个 decision workflow turn 开始前必须确认 sample 未过期。
后台必须按 control_plane_time_sync_interval_ms 持续刷新。
连续 3 次 sample 失败，clock_health = UNHEALTHY。
offset 抖动超过 max_decision_time_skew_ms，clock_health = DEGRADED。
clock_health != HEALTHY 时不得生成新的 TradeIntent。
```

Control Plane 时间规则：

```text
clock_health = HEALTHY / DEGRADED / UNHEALTHY
1. 使用本地 monotonic clock + trading_core_time_offset_ms 估算 effective_trading_core_now_ms。
2. 每个 decision workflow turn 开始前必须确认最近一次 /time 或 WS heartbeat 未过期。
3. 如果 clock_health != HEALTHY 或 last_time_sync_age_ms > max_decision_time_sync_age_ms，不得生成新的 TradeIntent。
4. Trading Core 收到 TradeIntent 后必须用 server_now_ms 校验 requested_at / signal_expires_at。
5. requested_at 不得晚于 server_now_ms + max_decision_time_skew_ms。
6. requested_at 不得早于 server_now_ms - max_decision_intent_age_ms。
7. signal_expires_at <= server_now_ms 时拒绝，reason = TRADE_INTENT_EXPIRED。
8. 时间字段异常时拒绝，reason = TRADE_INTENT_TIME_INVALID 或 TIME_SYNC_UNHEALTHY。
```

TradeIntent 提交前门禁：

```text
1. 读取 ControlPlaneTimeSyncState。
2. 如果 clock_health != HEALTHY，停止生成 TradeIntent，写 workflow audit: TIME_SYNC_UNHEALTHY。
3. 如果 last_time_sync_age_ms > max_decision_time_sync_age_ms，立即同步 /time。
4. 同步失败或仍过期，停止生成 TradeIntent。
5. 使用 effective_trading_core_now_ms 写 StrategyDecision.timestamp 和 TradeIntent.requested_at。
6. signal_expires_at 必须基于 effective_trading_core_now_ms + strategy signal ttl 派生。
7. 提交 POST /trade-intents 后，Trading Core 仍必须用自身 server_now_ms 二次校验。
```

#### Schema Version 兼容规则

`schema_version` 使用 Execution Client Protocol 固定格式：

```text
ecp.v<major>.<minor>
```

兼容规则：

```text
major 不同
  → 不兼容，拒绝消息，写 deadletter.event

minor 更高
  → 允许读取已知字段，忽略未知字段
  → 如果缺少必填字段，拒绝消息，写 deadletter.event

未知 type
  → 写 deadletter.event，不进入业务流程

字段类型错误
  → 写 deadletter.event，不进入业务流程
```

所有 schema 变更必须保留 golden sample payload，用于 TS / Python / MQL5 跨语言解析测试。

#### Sequence 与重连恢复语义

`sequence` 只描述同一 Execution Client session、同一发送方向内的 wire message 顺序，不是全局事件序号，也不是交易事实来源。跨 session 恢复和去重必须依赖 `message_id`、`command_id`、`idempotency_key` 与 Redis/Event store 中的事实事件。

推荐 hello / resume payload：

```ts
export interface HelloPayload {
  client_id: string
  platform: "MT5" | "BINANCE" | "OKX" | "IBKR" | "PAPER" | "BACKTEST" | "EXCHANGE"
  terminal_id?: string
  account_id: string
  token: string
  capabilities: string[]

  resume?: {
    previous_session_id?: string
    last_gateway_message_id?: string
    last_gateway_sequence?: number
    last_client_message_id?: string
    last_client_sequence?: number
    pending_command_ids?: string[]
  }
}

export interface HelloAcceptedPayload {
  session_id: string
  server_time: number
  heartbeat_interval_ms: number
  heartbeat_timeout_ms: number
  time_sync_interval_ms: number
  max_time_sync_rtt_ms: number
  max_clock_offset_ms: number
  max_inflight_commands: number
  max_frame_bytes: number
  max_message_bytes: number
}
```

`session.accepted.server_time` 只用于 bootstrap，不足以长期判断 clock health。Execution Client 必须在 hello 后通过 `time.sync.request / response` 建立并持续刷新 `server_time_offset_ms`。

capabilities 与 symbol metadata 的边界：

```text
capabilities
  → 表示 Execution Client 支持的协议能力和动作能力
  → 示例：MARKET_ORDER / LIMIT_ORDER / MODIFY_ORDER / CANCEL_ORDER / CLOSE_POSITION / RECONCILIATION_REQUEST

symbol.metadata
  → 表示某个 broker_symbol 在当前账户和终端上的交易约束
  → 示例：digits / volume_step / stops_level / trade_mode / tick_value_loss
```

Strategy & Decision Control Plane / Risk Layer 不得用 `capabilities` 推导价格精度、手数步长或止损距离；这些必须来自 `symbol.metadata`。如果 capabilities 允许某动作但 symbol metadata 显示 `trade_mode=DISABLED` 或约束不满足，Risk Layer 必须拒绝。

session 与 sequence 规则：

```text
1. Gateway 每次接受 hello 后分配新的 session_id。
2. 同一 client_id / account_id / terminal_id 只允许一个 active session。
3. 新 session 建立后，Gateway 必须关闭旧 session 或标记旧 session stale。
4. sequence 在每个 session、每个方向从 1 开始递增。
5. sequence 只用于诊断丢包、乱序和 resume cursor，不用于业务幂等。
6. message_id 必须全局唯一，transport ack 必须通过 `acked_message_id` 按 message_id 回执。
```

sequence 初始化：

```text
session.hello
  → 发生在新 session_id 分配前
  → sequence 可以省略，也可以使用 pre-session sequence = 1
  → 不参与新 session sequence 计数

session.accepted
  → Gateway 分配 session_id
  → Gateway outbound sequence = 1
  → durable session counter 将 1 视为已占用；后续第一个 Gateway outbound message 从 2 开始

session.accepted 后 client 第一条带 session_id 的消息
  → client inbound-to-core sequence = 1

每次重连成功都是新 session
  → client sequence 从 1 重新开始
  → gateway sequence 从 1 重新开始
  → previous_session_id 仅用于恢复诊断和 reconciliation，不延续 sequence
```

Gateway outbound sequence 必须由 State Store 在持有 `BEGIN IMMEDIATE` write transaction 时，连同 active session revision、heartbeat freshness、clock health 和 inflight limit 一起 reserve。禁止在事务外使用 `MAX(sequence)+1`；历史 outbox retention 或并发 writer 都不能造成 sequence 复用。数据库从旧版本升级时，migration 必须把每个 session 的 `last_outbound_sequence` 回填为 `max(1, MAX(wire_outbox.sequence))` 并合法推进 revision；升级后的下一次 reserve 必须得到该高水位 `+1`。新 session replacement 必须 fence 旧 session revision，旧连接的迟到 heartbeat、disconnect 或 ACK 不得推进新 session。

Gateway 下发的 `heartbeat_timeout_ms / time_sync_interval_ms / max_time_sync_rtt_ms / max_clock_offset_ms` 与 session registry 的 freshness / clock route gate 必须来自同一 policy。必须满足 `time_sync_interval_ms <= heartbeat_interval_ms`，Gateway 的 time-sync age 上限必须与下发的 `heartbeat_timeout_ms` 对齐，禁止客户端和服务端使用互相矛盾的健康阈值。

resume cursor 语义：

```text
previous_session_id
  → 客户端认为上一条 session 的 ID

last_gateway_message_id / last_gateway_sequence
  → 客户端最后成功处理的 Gateway → Client message
  → 用于 Gateway 判断客户端可能漏掉哪些 outbound delivery attempt
  → 不自动重放 execution.command

last_client_message_id / last_client_sequence
  → 客户端最后成功写入本地 journal 的 Client → Gateway message
  → 用于 Gateway 诊断 wire gap 和重复上报

pending_command_ids
  → 客户端本地 command journal 中未到 terminal state 的 command_id
  → Gateway 必须交给 Execution / Reconciliation 模块处理
```

恢复规则：

```text
Gateway 不根据 sequence 自动补发 execution.command。
Gateway 可以根据 resume cursor 写 system.event，标记可能的 delivery gap。
Execution Layer 根据 ExecutionCommandState、command.received、execution.event、expires_at 决定是否 reconciliation 或重新投递。
如果 pending_command_ids 与 Trading Core State Store 不一致，进入 reconciliation。
```

frame 边界与协议违规处理：

```text
frame header
  → 4-byte unsigned big-endian payload length

payload length
  → 必须满足 1 <= length <= max_frame_bytes
  → max_frame_bytes 由 Gateway 在 session.accepted 下发
  → 推荐默认值 max_frame_bytes = 262144

length <= 0
  → protocol violation
  → 不发送 transport ack
  → 关闭当前 transport session
  → 写 system.event: WIRE_PROTOCOL_VIOLATION

length > max_frame_bytes
  → 不读取超大 payload
  → 不发送 transport ack
  → 关闭当前 transport session
  → 写 system.event: WIRE_FRAME_TOO_LARGE

payload 不是合法 UTF-8 或 JSON
  → 不发送 transport ack
  → 写 deadletter.event(reason=DECODE_FAILED)
  → 关闭当前 transport session

envelope 可解析但 schema / type 不合法
  → 写 deadletter.event
  → 不进入业务流程
  → transport ack 只允许在 envelope 已通过最小字段校验后发送
```

heartbeat 与 reconnect backoff：

推荐 heartbeat payload：

```ts
export interface HeartbeatPayload {
  effective_server_now: number
  clock_sync_status: "SYNCED" | "DEGRADED" | "UNSYNCED"
  last_time_sync_at_server_ms?: number
  last_time_sync_rtt_ms?: number
  server_time_offset_ms?: number
  send_queue_depth?: number
  command_inbox_depth?: number
}
```

heartbeat 规则：

```text
HeartbeatPayload.effective_server_now 必须使用 server-time domain。
Gateway 收到 heartbeat 后补 received_at = server_now_ms。
每次合法 heartbeat 都必须持久化 last_heartbeat_at 和本次有效 clock_sync_status；time-sync sample 字段独立可选，缺少新 sample 不得跳过 heartbeat 的 clock health 更新。
如果 abs(received_at - effective_server_now) > max_clock_offset_ms，写 CLOCK_SKEW_DETECTED。
如果 clock_sync_status != SYNCED，写 TIME_SYNC_UNHEALTHY，并停止向该 session 投递新 command。
clock_sync_status 恢复 SYNCED 后，Gateway 写 TIME_SYNC_RESTORED。
```

```text
heartbeat_interval_ms
  → Gateway 在 session.accepted 中下发

heartbeat_timeout_ms
  → 建议为 heartbeat_interval_ms * 3

time_sync_interval_ms
  → 建议等于 heartbeat_interval_ms

max_time_sync_rtt_ms
  → 推荐默认值 1000ms
  → 超过则丢弃该 time sync sample

max_clock_offset_ms
  → 推荐默认值 250ms
  → 超过则写 CLOCK_SKEW_DETECTED

server_now_ms - last_legal_heartbeat_at >= heartbeat_timeout_ms
  → Gateway 标记 session stale，并写 EXECUTION_CLIENT_CONNECTION_LOST

time sync unhealthy
  → Gateway 写 TIME_SYNC_UNHEALTHY
  → Execution Client 暂停接收新 execution.command
  → 已在途订单只允许继续上报 execution.event / reconciliation snapshot
  → 这些 snapshot 不得作为 fresh state 驱动新交易
  → 不允许发起新 broker order

time sync restored
  → 连续 3 次有效 sample 且 offset 抖动 <= max_clock_offset_ms
  → clock_sync_status = SYNCED
  → Gateway 写 TIME_SYNC_RESTORED
  → Execution Client 恢复接收新 execution.command

reconnect backoff
  → initial = 500ms
  → multiplier = 2
  → max = 30000ms
  → jitter = 20%
```

重连流程：

```text
Execution Client 检测断线
  → 停止接收新 command
  → 保留本地 command journal / execution journal
  → backoff 后重新 hello，并携带 resume cursor

Gateway 接受新 hello
  → 分配新 session_id
  → 标记旧 session lost / stale
  → 写 EXECUTION_CLIENT_CONNECTION_RESTORED

Execution Client 重连成功后
  → 必须先完成 time sync
  → clock_sync_status = SYNCED 后才允许接收新 command
  → 立即发送 account.snapshot / position.snapshot / order.snapshot / symbol.metadata
  → 重发最近已关闭 market.bar，按 symbol + timeframe + timestamp 幂等去重
  → 对本地已接收但未完成的 command 重发 command.received 或最新 execution.event
```

消息可靠性分级：

```text
heartbeat
  → ephemeral，不 replay

market.tick
  → latest-only，不逐条 replay
  → 重连后用最新 tick / snapshot 覆盖

market.bar
  → idempotent by symbol + timeframe + timestamp
  → 重连后允许重发最近 N 根已关闭 bar

execution.command
command.received
execution.event
  → durable / idempotent
  → 必须用 message_id + command_id + idempotency_key 去重

account.snapshot
position.snapshot
order.snapshot
symbol.metadata
  → latest-state durable / idempotent
  → 必须用 message_id 去重，并按业务 key + observed_at 保留最新状态
  → account.snapshot key = account_id
  → position.snapshot key = account_id + position_id
  → order.snapshot key = account_id + broker_order_id
  → symbol.metadata key = account_id + broker_symbol
```

command 幂等规则：

```text
Execution Client 收到通过 identity / HMAC 校验且未过期的新 execution.command 后，必须先持久化 command_id / idempotency_key 到本地 journal，再返回 command.received。
如果断线前 command.received 已发送但 Gateway 未收到，重连后客户端必须重新发送 command.received。
如果同一 command_id 或 idempotency_key 的已入 inbox command 再次到达，客户端不得重复下单，只能返回 command.received 和当前已知 execution.event。
如果同一 command_id 或 idempotency_key 命中的是 expired rejection record，只能返回当前已知 execution.event(status=EXPIRED)，不得发送 command.received。
如果相同 idempotency_key 对应不同 command payload，客户端必须拒绝执行，并上报 execution.event(status=FAILED, message=DUPLICATE_IDEMPOTENCY_CONFLICT)。
```

command 过期规则：

```text
Execution Layer 不得创建 expires_at 已过期的 execution.command。
Trading Gateway 使用 server_now_ms 判断 expires_at，不得投递 expires_at 已过期的 execution.command。
Execution Client 验签通过后必须先查本地 idempotency journal；已知 command 返回当前状态，不重新执行。
对本地 journal 未见过的新 command，Execution Client 必须使用 effective_server_now_ms 再次检查 expires_at。
新 command 已过期时不得进入可执行 command inbox，不得发送 broker order。
客户端可以持久化 lightweight rejection record，用于 command_id / idempotency_key 幂等。
客户端必须上报 execution.event(status=EXPIRED, error_code=COMMAND_EXPIRED) 或由 Trading Gateway 写 system.event: COMMAND_EXPIRED。
```

expiry ownership：

```text
Strategy & Decision Control Plane / Strategy Runtime owns signal_expires_at
  → 表示交易想法 / 信号什么时候失效
  → Trading Core 可以拒绝异常或过期的 signal_expires_at，但不得延长它

Risk Layer owns risk_result.valid_until
  → 表示本次风控审批什么时候失效
  → 必须综合 snapshot freshness、order snapshot freshness、symbol metadata freshness 与 risk policy

Execution Layer derives execution.command.expires_at
  → 从上游有效期和 execution policy 机械派生
  → 拥有拒绝投递 / 拒绝执行的权利和责任
  → 可以缩短有效执行窗口
  → 不得延长 signal 或 risk approval 的有效期
```

推荐派生公式：

```text
execution.command.expires_at = min(
  strategy_decision.signal_expires_at,
  risk_result.valid_until,
  server_now_ms + execution_policy.max_command_ttl_ms
)
```

Trading Gateway 不应基于 `sequence` 自动重放全部 wire message。Trading Gateway 只负责恢复 session 与 delivery 状态；是否重试 `execution.command` 由 Execution Layer 根据 `ExecutionCommandState`、`expires_at`、reconciliation 结果和 Risk 约束决定。

客户端最小职责：

```text
connect
hello / auth
time sync
maintain effective_server_now_ms
send event.tick / event.bar / snapshot / execution.event
receive execution.command
send transport ack / command.received
heartbeat send
heartbeat timeout detection
disconnect detection
reconnect with backoff
message framing / 拆包 / 粘包处理
local idempotency cache
local execution guard
clock sync guard
```

客户端不负责：

```text
策略判断
风控批准
订单生成
command 全局生命周期状态机
多客户端路由
跨账户风险聚合
复杂 replay 策略
```

---

### 3.3 State / Event Backbone Layer

#### 技术形态

```text
SQLite / WAL / append-only execution log
Local spool
Redis Streams optional fanout
Consumer Groups
Event Envelope
Operational event log
```

#### 职责

```text
Trading Core 本地强一致执行状态持久化
execution event / command state / idempotency journal 持久化
Redis 事件广播
多消费者组
回放
审计链路
模块解耦
策略并行
异步处理
backpressure 观测
```

状态所有权：

```text
Trading Core State Store
  → execution.command state
  → execution.event
  → idempotency journal
  → account / position / order latest state
  → reconciliation checkpoint
  → local append-only audit/spool

Redis Streams
  → cross-service operational fanout
  → Strategy & Decision Control Plane / UI / research consumers
  → replay aid and audit pipeline
  → not the authoritative execution state store
```

#### 推荐 Stream 设计

```text
market.tick
market.bar
symbol.metadata

signal.raw
signal.scored

strategy.decision
trade.intent

agent.review

risk.request          # Trading Core internal / audit fanout
risk.approved         # Trading Core internal / audit fanout
risk.rejected         # Trading Core internal / audit fanout
risk.summary          # aggregate fanout, not source of truth

execution.plan
execution.command
execution.command.state
execution.event
execution.summary     # aggregate fanout, not source of truth
reconciliation.request
reconciliation.result

account.snapshot
position.snapshot
order.snapshot

audit.event
system.event
deadletter.event

external.research.request
external.research.result
external.backtest.request
external.backtest.result
external.strategy.candidate
external.strategy.export
external.strategy.rejected
external.shadow.report
```

#### market.tick 与 market.bar 分工

```text
market.tick
  → 用于实时 MarketSnapshot 更新
  → 主要承载 bid / ask / spread / observed_at
  → 支持风控前的快照新鲜度校验和价差监控
  → 不作为 H1 / H4 指标计算主输入

market.bar
  → 用于指标计算和信号生成主流程
  → 驱动 computeIndicators / scoreSignal / strategy.decision
  → 是中低频趋势策略的主要事件源
```

Trading Core 可以消费 raw `market.tick` 并维护实时 MarketSnapshot。Strategy & Decision Control Plane 默认不消费每个 raw tick，只消费 `market.snapshot`、`market.bar`、`signal.candidate` 或 Trading Core 聚合事件。`market.bar` 推动中低频策略流程。

#### Command / Event 语义

必须严格区分：

```text
Command = 请求某组件做某事
Event   = 已经发生的事实
```

示例：

```text
execution.command = 要求 Execution Client 执行订单
execution.event   = Execution Client 已接受 / 拒绝 / 提交 / 成交 / 失败
```

不要把主事实流命名为 `execution.log`。
日志是副产物，事件是事实。

---

### 3.4 Compute Layer

#### 技术形态

```text
Python + FastAPI
stateless compute service
```

#### 职责

Compute Service 只做无状态计算：

```text
技术指标计算
信号强度计算
研究 / 决策建议性仓位数学计算
组合风险数值计算
hedge ratio
basis
volatility
correlation
regression
regime detection
模型推理
```

#### 不负责

```text
流程控制
事件调度
Redis consumer group 状态机
最终交易批准
最终可执行 lots 批准
live hard-risk position sizing
execution.command 生成
Execution Client 连接管理
TradingState 持久化
```

#### 推荐 API

```text
POST /compute/indicators
POST /compute/signal-strength
POST /compute/position-size  # advisory / research only
POST /compute/portfolio-risk
POST /compute/hedge-ratio
POST /compute/basis
POST /compute/regime
```

#### 设计原则

```text
输入 → 纯计算 → 输出
无状态
无流程判断
无业务副作用
```

Compute Service 不需要知道自己处于哪个交易流程节点。

`/compute/position-size` 的返回值不是 `RiskResult`，也不是 `ExecutionCommand.lots`。Strategy & Decision Control Plane 可以用它形成 `proposed_risk_pct` 或多腿 ratio，但 `TradeIntent` 不得携带 suggested / final lots。Trading Core 不消费 Compute Service 给出的 lots，live hard-risk path 不得依赖 Compute Service 可用性。

---

### 3.5 Strategy & Decision Control Plane

#### 技术形态

```text
TypeScript
Bun
LangGraph optional
Cursor SDK optional
Agent SDK optional
Rule engine optional
Human review workflow
Trading Core HTTP / WS client
Redis Streams consumer / producer for non-critical event fanout
HTTP client to Compute Services
Decision workflow checkpoint store
```

#### 职责

```text
读取 Trading Core 聚合事件 / market snapshot / execution summary
维护慢决策 workflow state
持久化 decision workflow checkpoint
调用 Compute Services
执行条件路由
执行 signal early exit
调用 Agent 节点
聚合策略输出
生成 StrategyDecision / TradeIntent
提交 TradeIntent 到 Trading Core
读取 Trading Core 返回的 intent accepted / risk blocked / command state summary
写 audit.event
写 system.event
调用外部研究 / 回测服务
```

#### 推荐编排拓扑

当前系统不适合使用自由多 Agent swarm 来驱动交易。推荐采用：

```text
Deterministic Main Trading Graph
  → 条件路由和状态推进由代码控制
  → LLM / Agent 只作为受限叶子节点或异步 sidecar
  → 所有交易相关输出必须提交到 Trading Core
```

顶层图拆分：

```text
MarketEventGraph
  → 处理 market.snapshot / market.bar / symbol metadata summary

StrategyDecisionGraph
  → 指标计算、信号评分、策略评估、策略决策聚合

AgentReviewSubgraph
  → 只在条件触发时运行
  → 输出 AgentReview，不输出 command

RiskApprovalGraph
  → 可选 pre-risk / soft-risk / explanation
  → 不作为最终执行批准

ExecutionSagaGraph
  → submitTradeIntent
  → record Trading Core response
  → checkpoint workflow

ExecutionFeedbackGraph
  → 消费 Trading Core execution summary / execution.event fanout
  → 更新解释性 workflow state，不覆盖 Trading Core 状态

ReconciliationGraph
  → 处理 DELIVERY_UNCONFIRMED / SAGA_RECOVERY / REDIS_SPOOL_GAP
  → 请求 Trading Core refresh / reconciliation summary
  → 输出 workflow 恢复决策或人工处理请求

ResearchSidecarGraph
  → 异步研究、回测、策略候选生成
  → 不直接进入 live execution path
```

主交易路径：

```text
receiveMarket
normalizeMarket
updateMarketSnapshot
validateSnapshotFreshness
computeIndicators
scoreSignal
filterSignal
selectStrategies
evaluateStrategies
aggregateStrategyDecision
agentReviewGate
agentReviewSubgraph
buildTradeIntent
optionalSoftRisk
submitTradeIntentToTradingCore
checkpointSaga
audit
```

事件恢复路径：

```text
receiveExecutionFeedback
  → command.received
      → updateWorkflowExecutionSummary
      → checkpointSaga

  → execution.event
      → readTradingCoreExecutionSummary
      → updateWorkflowExecutionSummary
      → checkpointSaga

  → order.snapshot
      → readTradingCoreStateSummary
      → resumeWorkflowIfNeeded
```

条件边：

```text
filterSignal
  ├── signal invalid / stale → audit + END
  └── signal valid → selectStrategies

agentReviewGate
  ├── high confidence / no anomaly → buildTradeIntent
  └── ambiguous / volatile / degraded → agentReviewSubgraph

submitTradeIntentToTradingCore
  ├── accepted → checkpoint + END
  ├── risk blocked → human_review / audit + END
  └── rejected → audit + END

delivery timeout / connection lost
  → wait for Trading Core execution summary / reconciliation result
  → ReconciliationGraph
```

设计规则：

```text
1. Main Trading Graph 不等待 broker 最终成交；Trading Core 接收 intent 后 checkpoint 并结束当前 turn。
2. Trading Core 的 intent accepted / execution.event summary 作为新事件重新进入 ExecutionFeedbackGraph。
3. AgentReviewSubgraph 不能改变 Trading Core execution state，也不能写任何 execution plan / command payload。
4. Trading Core 是最终 risk gate 和 execution gate。
5. ReconciliationGraph 只能恢复 workflow 或要求人工处理，不能直接生成新交易 command，也不能覆盖 Trading Core execution state。
6. ResearchSidecarGraph 只写 external.* 事件或 strategy.candidate，不修改 live registry。
```

#### Strategy & Decision Control Plane 是业务流程 owner

Strategy & Decision Control Plane 负责决定：

```text
何时调用 Python
调用哪个 Compute Service API
如何解释返回值
走哪个条件分支
是否进入 Agent Review
是否调用外部研究 / 回测服务
是否形成 TradeIntent
如何解释 Trading Core 返回的 risk blocked / accepted / rejected
是否进入人工审查或研究流程
```

Compute Services 不做这些判断。
Trading Core 做最终风险批准、command 生成、执行状态投影和 broker reconciliation。

---

### 3.6 Risk Layer

#### 技术形态

```text
Trading Core 内部 domain module
本地 pure deterministic position sizing
不调用 Compute Services，不引入 live hard-risk 网络依赖
最终批准逻辑在 Trading Core implementation 内执行
```

#### 部署决策

当前目标架构中，Risk Engine 作为 Trading Core 内部 domain module 部署，而不是 Strategy & Decision Control Plane 节点，也不是独立服务。

理由：

```text
单机运行，不需要跨进程
避免额外网络调用
减少故障点
更容易访问 Trading Core State Store
更容易读取 pending commands / snapshots
风控穿透代价灾难性，必须靠强一致内核兜底
```

保留清晰模块边界：

```text
risk-engine/domain
risk-engine/policy
risk-engine/evaluator
risk-engine/result
```

当需要多实例部署、多账户、多策略高并发或独立风控审计时，再拆分为独立 `risk-engine` service。

#### 职责

```text
单笔风险检查
日内亏损检查
最大回撤检查
品种风险暴露
多策略风险聚合
多腿风险聚合
禁止无 SL
过期信号拒绝
重复 intent / pending command exposure 拒绝
账户状态校验
snapshot freshness 校验
symbol metadata freshness 校验
broker trading constraints 校验
风险预算控制
将 proposed_risk_pct 本地确定性地换算为最终 lots
按 volume_min / volume_max / volume_step 向下归一化手数
按账户余额、保证金、敞口和多腿最坏止损损失重新校验
风控审批有效期控制
```

#### 输入

```text
strategy.decision
trade.intent
agent.review
account.snapshot
position full-set snapshot + account-level watermark
order full-set snapshot + account-level watermark
symbol.metadata
pending execution.command
pending execution.command state
risk policy
account-scoped market snapshot
risk capacity snapshot
```

#### 输出

```text
risk.approved
risk.rejected
RiskResult.adjusted_legs with final approved lots
```

#### 风险层边界

策略不能直接生成最终执行命令。
正确路径：

```text
strategy.decision
  → trade.intent
  → Trading Core
  → hard risk gate
  → risk.approved / risk.rejected
  → execution.plan / execution.command
```

Risk Layer 是执行前的硬边界，也是最终可执行 lots 的唯一 owner。Risk Layer 只接受 Trading Core 内可信 assembler 组装完成的不可变 `RiskRequest`，自身不读取 State Store、不调用 Compute Service，也不访问 HTTP / socket。assembler 必须在同一一致性读快照内校验账户作用域、完整集合水位和 pending command 对应关系；Risk Layer 对相同完整输入执行纯函数式评估。

Risk Layer 拥有 `risk.approved` 的有效期语义，但 `execution.command.expires_at` 仍由 Execution Engine 根据上游有效期和 execution policy 派生。Execution Layer 只能把已批准 lots 映射到 plan / command，不得重算或放大。

#### Circuit Breaker

Circuit Breaker 是 Trading Core 内部 hard risk gate 的全局保护状态。它不是普通 `risk.rejected`，而是阻断新的交易意图进入执行流程。

状态：

```text
CLOSED
  → 正常状态

OPEN
  → 熔断中，阻断新 TradeIntent 形成 execution.plan / execution.command

HALF_OPEN
  → 恢复验证中，只允许只读 state、reconciliation、manual review，不允许新开仓
```

触发条件：

```text
daily_realized_loss_pct >= max_daily_loss_pct
equity_drawdown_pct >= max_equity_drawdown_pct
consecutive_broker_rejections >= max_consecutive_broker_rejections
consecutive_command_failures >= max_consecutive_command_failures
manual_reconciliation_required_count > 0 且未处理
State Store restored 后 reconciliation 未完成
TIME_SYNC_UNHEALTHY 持续超过 max_time_sync_unhealthy_ms
snapshot_stale_count 连续超过阈值
symbol_metadata_stale_count 连续超过阈值
operator manual trigger
```

OPEN 后阻断：

```text
POST /trade-intents
  → 返回 RISK_BLOCKED
  → reason = RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED
  → 不生成 execution.plan / execution.command

Execution Engine
  → 不创建新的增加风险暴露的 BUY / SELL command
  → 允许风险降低型 CLOSE / CANCEL / MODIFY command，例如只收紧 SL，但必须经过 hard risk gate

Execution Client Protocol
  → 继续允许 command.received / execution.event / snapshots / reconciliation.result
  → 不影响已在途 broker 订单回报

Event Stream
  → 发布 system.event: RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED
  → 发布 risk.summary(circuit_breaker_status=OPEN)
```

重置规则：

```text
自动重置只允许从 OPEN → HALF_OPEN，不允许直接 OPEN → CLOSED。
进入 HALF_OPEN 前必须完成 account / position / order / symbol metadata refresh。
进入 HALF_OPEN 前必须完成 pending command reconciliation。
每项 refresh / reconciliation 证据必须携带服务器时间域的完成时间，且完成时间不得早于当前 breaker triggered_at；单纯 boolean 不足以证明本次熔断后的恢复工作已经完成。
进入 HALF_OPEN 时记录 daily realized loss 与 equity drawdown baseline。造成原 incident 的财务阈值可以暂时仍被触发，但观察窗口内任一财务值高于对应 baseline 都视为新的 hard risk violation 并重新 OPEN，即使新值低于 policy threshold。
HALF_OPEN 观察窗口内不得出现其他新的 hard risk violation。
HALF_OPEN 只能允许 no-op validation 或风险降低动作。
恢复 CLOSED 必须满足：
  → clock health HEALTHY
  → State Store HEALTHY
  → no MANUAL_RECONCILIATION_REQUIRED
  → drawdown / daily loss 未继续恶化
  → operator approval 或配置允许 auto_reset

manual reset 必须写 audit.event，并记录 operator_id / reason / before_state / after_state。
```

恢复证据结构使用可缺省时间戳表达“尚未完成”，不使用完成 boolean：

```ts
export interface HalfOpenReadiness {
  account_refreshed_at_ms?: number
  positions_refreshed_at_ms?: number
  orders_refreshed_at_ms?: number
  symbol_metadata_refreshed_at_ms?: number
  pending_commands_reconciled_at_ms?: number
}
```

进入 `HALF_OPEN` 时，上述字段必须全部存在并且逐项满足 `completed_at_ms >= 当前 breaker triggered_at`。

`OPEN` 和 `HALF_OPEN` 都只阻断风险增加的 TradeIntent / command；读取状态、摄取 snapshot / execution event、reconciliation、manual review、no-op validation 和经过 hard risk gate 证明的风险降低动作仍可继续。非法 policy、输入、服务器时间回退或恢复证据不足必须保持 active 或转为 `OPEN`，不得 fail open。

Breaker 必须把当前 hard-risk violation evidence 纳入 incident fingerprint。`OPEN` 状态重复观察完全相同 evidence 时保持幂等；出现不同的新 violation evidence 时，必须以本次服务器时间推进 `triggered_at` 并开启新的 recovery epoch，使旧 incident 之后、但新 incident 之前完成的 refresh / reconciliation 证据全部失效。进入 HALF_OPEN 前，above-limit 的 daily loss / drawdown 数值变化也属于 fingerprint 变化，不能沿用旧 readiness。不得因为 breaker 已经是 `OPEN` 就沿用旧恢复水位。

`OPEN` 收到不再包含 hard-risk violation 的健康 observation 时仍然保持 `OPEN`，记录服务器时间域的 `incident_evidence_cleared_at`，清除当前 fingerprint，并产生 `IncidentEvidenceCleared` transition；该时间参与后续 transition 的单调时间校验。清除后即使相同 violation 再次出现，也必须作为新 incident 推进 recovery epoch，不能恢复旧 fingerprint 的幂等身份。

非法 policy、输入或时间导致的 safety fallback 必须把具体 `CircuitBreakerError` 纳入 fingerprint，而不是只记录笼统的 `SafetyInvariantViolation`。完全相同的错误保持幂等，不同错误必须开启新的 recovery epoch；durable adapter 不得丢失该错误身份。

面向 `GET /state` 的只读摘要结构：

```ts
export type CircuitBreakerReason =
  | "OK"
  | "DAILY_REALIZED_LOSS_LIMIT"
  | "EQUITY_DRAWDOWN_LIMIT"
  | "CONSECUTIVE_BROKER_REJECTIONS"
  | "CONSECUTIVE_COMMAND_FAILURES"
  | "MANUAL_RECONCILIATION_REQUIRED"
  | "STORE_RECOVERY_RECONCILIATION_PENDING"
  | "TIME_SYNC_UNHEALTHY"
  | "SNAPSHOT_STALE"
  | "SYMBOL_METADATA_STALE"
  | "MANUAL_TRIGGER"
  | "HARD_RISK_VIOLATION_DURING_RECOVERY"
  | "SAFETY_INVARIANT_VIOLATION"

export interface CircuitBreakerSummary {
  status: "CLOSED" | "OPEN" | "HALF_OPEN"
  reason: CircuitBreakerReason
  triggered_at?: number
  triggered_by?: string
  reset_at?: number
  reset_by?: string
  blocked_intent_count: number
  metadata?: Record<string, unknown>
}
```

`CircuitBreakerReason` 是领域内详细触发原因，例如 daily loss、drawdown、broker failure、store recovery 或 safety invariant；它不同于对外拒绝响应使用的通用 `ErrorCode=RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED`。`GET /state.risk.circuit_breaker_active` 等价于 `CircuitBreakerSummary.status != "CLOSED"`。推荐同时返回 `circuit_breaker: CircuitBreakerSummary`，避免只有 boolean 无法判断原因。

`sinan-risk` 继续只提供 pure state transition，不依赖 State Store；它同时定义带 schema version 和完整不变量校验的 durable snapshot codec。`sinan-store` V0003 使用 append-only revision 持久化完整 `OPEN / HALF_OPEN`、recovery epoch、具体 incident / safety-error fingerprint、`incident_evidence_cleared_at`、`half_opened_at`、两项 financial baseline 和 blocked count，`sinan-core` application adapter 负责启动恢复。

恢复时必须先读取可信的 denormalized head metadata，再解析 payload。snapshot 缺失、损坏或版本未知时，adapter 必须从已知最高 recovery epoch 的下一 epoch 创建新的 `OPEN` safety incident，并以 revision CAS 持久化后才能返回；损坏 payload 不能把 epoch 重置为 1。并发 stale revision 可以有界重读。State Store 不可用或 recovery epoch 溢出时必须向调用方返回可检查的 fail-closed `OPEN` outcome，live flow 不得继续。不得把面向 `GET /state` 的简化 `CircuitBreakerSummary` 当作持久化格式。

---

### 3.7 Execution Layer

Execution Layer 是 Trading Core 内部的 execution domain module。它拥有 `execution.plan` / `execution.command` 的状态机，并把客户端返回的 `command.received` 与 `execution.event` 投影为可恢复的执行状态。

#### 技术形态

```text
Execution Plan Builder
Execution Command Builder
Command Lifecycle State Machine
Execution Event Projector
Execution Recovery / Rollback Handler
```

#### 职责

```text
将 risk.approved / trade.intent 转换为 execution.plan
按 leg_id 将 RiskResult.adjusted_legs[].lots 原样映射到 execution.plan
将 execution.plan 拆解为 execution.command
确保 ExecutionCommand.lots 精确等于对应风控审批 lots
从 strategy.decision / risk.result / execution policy 派生 execution.command.expires_at
拒绝投递过期、不匹配、不可确认或超过执行约束的 command
通过 Trading Gateway / adapter router 投递 execution.command
维护 command lifecycle state
维护 plan lifecycle state
接收 command.received
接收 execution.event
将 execution.event 投影到 command / plan 状态
处理 DELIVERY_UNCONFIRMED 的 broker reconciliation
写 audit.event
触发 account.snapshot / position.snapshot / order.snapshot / symbol.metadata 刷新
维护 idempotency journal
维护 SQLite / append-only execution store
```

#### 不负责

```text
维护 TCP socket
Execution Client session registry
心跳检测
连接断线重连
消息 framing encode / decode
真实订单执行
broker order API 适配
定义 signal expiry
延长 risk approval validity
策略判断
LLM / Agent 判断
```

#### 单腿策略路径

```text
risk.approved
  → execution.plan
  → execution.command
  → Gateway delivery request
  → command.received
  → execution.event
  → command / plan status projection
```

#### 多腿策略路径

```text
risk.approved
  → execution.plan
  → execution.command leg 1
  → execution.command leg 2
  → command.received leg 1
  → command.received leg 2
  → execution.event leg 1
  → execution.event leg 2
  → plan status update
```

Execution Layer 不负责策略判断、LLM / Agent 判断或真实 broker 执行。`execution.command.expires_at` 由 Execution Layer 写入 command payload，但它是从上游有效期和执行约束派生出来的字段，不是 Execution Layer 自己拥有的业务有效期。Execution Layer 拥有拒绝执行的权利和责任，但不得延长上游有效期。

Execution Layer 不拥有 position sizing。对风险增加 leg，缺失 `RiskResult.adjusted_legs`、leg_id 不匹配、审批过期，或已批准 lots 无法在当前 price / SL / metadata / margin 下原样执行时，Execution Layer 必须停止 plan / command 生成并重新进入 Risk Layer，不得本地修正 lots。

当前 Execution 领域与持久化里程碑已经实现 pure builder、typed command state machine、leg / plan projector、recovery decision、V0003 immutable plan / leg journal、原子 workflow commit 和 lifecycle CAS。Workflow commit 在同一 transaction 中写入 TradeIntent、RiskResult、plan / legs、commands 和 pristine command states；任一父图、identity、approved lots 或 payload 漂移都必须冲突并整体回滚。command state 使用 immutable identity + expected status + expected `updated_at` CAS，leg / plan 状态作为一致 bundle CAS，不能暴露部分 projection。Gateway session、wire outbox 和真实投递不属于该已完成范围。

---

## 4. Execution Command 完整性与 ACK 回路

`execution.command` 从 Trading Core 到 Execution Client 的路径必须有投递确认，避免内部队列满、连接异常或投递失败导致 command 静默丢失。Strategy & Decision Control Plane 提交的是 `TradeIntent`，不直接投递 `execution.command`。

### 4.1 投递路径

```text
Strategy & Decision Control Plane
  → trade.intent
  → Trading Core
  → hard risk gate
  → execution.plan
  → execution.command
  → Trading Gateway / adapter router
  → Execution Client
  → command.received
  → Trading Core
  → execution.command.state
  → event fanout to Strategy & Decision Control Plane / UI
```

Trading Gateway / adapter router 投递前置校验：

```text
active session
  → 必须存在匹配 client_id / account_id / terminal_id 的 active session
  → 无匹配 session 时不得写 socket，返回 COMMAND_DELIVERY_FAILED

session identity
  → WireMessage.session_id 必须等于 Gateway 当前 active session_id
  → execution.command.account_id / client_id / terminal_id 必须匹配 authenticated session context
  → 不匹配时不得投递，写 system.event: SESSION_IDENTITY_MISMATCH

expires_at
  → Gateway 使用 server_now_ms 判断；server_now_ms 已经超过 expires_at 时不得投递
  → 写 system.event: COMMAND_EXPIRED
  → Execution Layer 将 command state 推进到 EXPIRED

max_inflight_commands
  → 同一 session 下 DISPATCHED 且未收到 command.received 的 command 数量不得超过 session.accepted.max_inflight_commands
  → 超限时 Gateway 不得写 socket
  → 写 system.event: COMMAND_DISPATCH_BACKPRESSURE，metadata.reason=COMMAND_INFLIGHT_LIMIT_REACHED
  → Execution Layer 保持 command 未 DISPATCHED，或在 retry budget 耗尽后标记 DELIVERY_FAILED
```

### 4.2 ACK 规则

Trading Gateway / adapter router 将 command 投递给 Execution Client 后，必须等待客户端返回：

```text
command.received
```

ACK 分三层：

```text
transport ack
  → 客户端收到完整 WireMessage，并通过 acked_message_id / acked_message_type / status 回传协议接纳结果

command.received
  → execution.command 已进入客户端 command inbox
  → payload 必须包含 command_id，WireMessage.causation_id 必须等于源 execution.command message_id
  → 这是 command 进入执行域的边界

execution.event
  → 客户端已拒绝 / 接受 / 提交订单 / 成交 / 失败
```

推荐 ACK payload：

```ts
export interface TransportAck {
  acked_message_id: string
  acked_message_type: ExecutionClientMessageType
  status: "ACCEPTED" | "DUPLICATE" | "REJECTED"
  reason?: ErrorCode | "OK"
  received_at: number
}

export interface CommandReceived {
  command_id: string
  idempotency_key: string
  account_id: string
  terminal_id?: string
  client_id?: string
  received_at: number
  inbox_status: "RECORDED" | "DUPLICATE" | "EXPIRED" | "REJECTED"
  reason?: ErrorCode | "OK"
}
```

`TransportAck` 以 23.1 的 payload 定义为权威规格。`ACCEPTED` 表示 envelope / schema / authenticated identity 校验通过，且消息已进入 crash-recoverable、幂等的 durable handler path；`DUPLICATE` 表示相同 message identity 与 payload 已被 durable handler path 接纳或处理；`REJECTED` 表示可关联消息的 typed rejection 与稳定 reason 已持久化为可幂等重放的 durable decision，此时 `reason` 必填。三种状态都不表示 command 已执行或 execution state 已变更。

`CommandReceived` 以 23.1 的 payload 定义为权威规格。只有 `inbox_status=RECORDED`，或已知同 payload 的 `DUPLICATE`，才能作为 command lifecycle 向 `COMMAND_RECEIVED` 推进的证据；`EXPIRED / REJECTED` 不得推进该状态。

持久化 ACK 口径固定如下：

```text
transport.ack
  → ACCEPTED / DUPLICATE 只把匹配 session_id / acked_message_id / acked_message_type 的 wire_outbox 推进到 ACKED
  → ACCEPTED / DUPLICATE 不释放 max_inflight_commands，不修改 command delivery attempt，不修改 ExecutionCommandState
  → REJECTED 把匹配 outbox 收敛到 FAILED 并保留 reason；结束该次 transport-admission inflight 占用，但不把 attempt 置为 ACKED。并发 write completion 可把 PENDING 收敛为 SENT，已有 ACKED / UNCONFIRMED 等更强或独立证据保持不变；不得据此推进 ExecutionCommandState

command.received
  → Execution service 先验证 command / account / client / terminal / causation identity 和 inbox_status
  → 验证通过后把源 execution.command 的 delivery attempt 推进到 ACKED，并释放该 session 的 command inflight slot
  → Execution service 再依据 typed CommandEvidence 决定 ExecutionCommandState 转换
```

`command.received` 与 timeout / disconnect 并发时，已 `ACKED` 的 attempt 不得被覆盖；late `command.received` 可以把 `UNCONFIRMED` attempt 收敛为 `ACKED`。该收敛只修复 delivery journal，不允许 Gateway 自行推进 execution lifecycle。

推荐超时：

```text
delivery_ack_timeout_ms = 5000
```

### 4.3 超时处理

如果超时未收到 `command.received`：

```text
Trading Gateway 写 system.event: COMMAND_DELIVERY_TIMEOUT
Trading Gateway 以 CAS 将该 session delivery attempt 标记为 UNCONFIRMED，error=COMMAND_DELIVERY_TIMEOUT
Execution Layer 将 command lifecycle state 更新为 DELIVERY_UNCONFIRMED
Execution Layer 触发 account / position / order reconciliation
Reconciliation result 评估只返回 Completed / PendingEvidence
独立的显式 evidence 升级才返回 ManualRequired
缺少权威 execution evidence 时保持 PendingEvidence，不自动 retry
只有 typed dispatch / delivery / reconciliation evidence、command.received、ExecutionEvent 或显式时间 / 人工证据可以按 Execution 状态机推进 lifecycle；snapshot / result 不属于这些 evidence
```

`reconciliation.result` 及其中的 account / position / order snapshots 只是 broker 状态观测，不是执行事实。即使 snapshot 显示订单已提交、已成交或不存在，也不得单独把 command 推进到 `ORDER_SENT / FILLED / FAILED / EXPIRED`，不得据此创建新的 command 或授权自动 retry。`FAILED` 等 broker 结果由 `ExecutionEvent` 表达；expiry 必须由服务器时间域的显式到期证据按状态机处理；人工状态必须有可审计的显式人工 / 超时证据。

### 4.4 Execution Client 断开时的 pending command 处理

当发生：

```text
system.event: EXECUTION_CLIENT_CONNECTION_LOST
```

Trading Gateway 必须：

```text
标记该 Execution Client session 下所有 SENT attempt，以及关联 outbox 已为 WRITE_STARTED 的 PENDING attempt，为 UNCONFIRMED
写 system.event: COMMAND_DELIVERY_UNCONFIRMED
```

Execution Layer 必须：

```text
将已 DISPATCHED 但未收到 command.received 的 command 标记为 DELIVERY_UNCONFIRMED
将 command / plan projection 推进到 RECONCILING
发布 execution.summary / system.event，Strategy & Decision Control Plane 可据此 checkpoint workflow
刷新 account.snapshot / position.snapshot / broker order state
写 audit.event 说明投递状态不确定
不得假设 command 已被 Execution Client 执行
不得在 reconciliation 完成前使用新 command 盲目重试
```

Reconciliation 完成也不等于 retry authorization。任何未来的重新投递或新 command 都必须由独立 Execution policy 基于权威 lifecycle evidence、idempotency contract、服务器时间和有效期作出显式决定；当前 Reconciliation 里程碑不实现该决策。

### 4.5 设计原则

```text
command.received 只代表 Execution Client 已收到 command，不代表订单已成交
ACCEPTED / FILLED / PARTIALLY_FILLED / FAILED 由后续 execution.event 表达
未收到 command.received 的 command 不能视为已进入执行域
未收到 command.received 的 command 也不能视为绝对未执行，必须先 reconcile
DELIVERY_FAILED 只用于 Trading Gateway 明确知道未投递成功的情况，例如无可用 session、认证失败或 socket write 失败且未写入内核发送缓冲区
DELIVERY_UNCONFIRMED 用于 Gateway 无法确认客户端是否收到 command 的情况
```

### 4.6 Reconciliation 协议

`RECONCILING` 必须通过 Trading Gateway 请求 Execution Client 刷新 broker 当前状态，不能由 Strategy & Decision Control Plane 直接连接 broker。

推荐路径：

```text
Execution Layer
  → reconciliation.request
  → Trading Gateway
  → Execution Client
  → reconciliation.result
  → Trading Gateway
  → Reconciliation evaluation + State Store full-set projection
  → command.received / ExecutionEvent 等权威证据另行进入 Execution state machine
  → WS / Redis fanout to Strategy & Decision Control Plane / UI
```

推荐 payload：

```ts
export interface ReconciliationRequest {
  request_id: string
  account_id: string
  terminal_id?: string
  client_id?: string
  reason:
    | "DELIVERY_UNCONFIRMED"
    | "CONNECTION_RESTORED"
    | "MANUAL_REQUEST"
    | "STATE_STORE_RESTORED"
  command_ids?: string[]
  since_server_time?: number
}

export interface ReconciliationResult {
  request_id: string
  account_id: string
  terminal_id?: string
  client_id?: string
  observed_at: number
  account?: AccountSnapshot
  positions: PositionSnapshot[]
  orders: OrderSnapshot[]
  symbol_metadata: SymbolMetadataSnapshot[]
  unresolved_command_ids: string[]
}
```

对账协议的可选字段有严格语义：`command_ids` 缺省表示账户 / route 全量 scope；存在时必须是非空、唯一、稳定排序的 targeted scope。`positions` 和 `orders` 是 `observed_at` 时刻的账户完整集合，数组为空也表示“完整集合为空”，每个元素的 `observed_at` 必须精确等于 result 的 `observed_at`。`account` 缺省表示本次没有 account refresh evidence；`symbol_metadata` 没有声明完整 symbol 范围，因此非空数组本身不能证明 metadata full refresh。

`ReconciliationRequest / ReconciliationResult` 以 23.1 的 payload 定义为权威规格。

Request scope 规则：

```text
command_ids = None
  → 目标 account_id + terminal_id/client_id route 内的全量 command scope
  → 只定义请求范围；不自行证明 application 传给 evaluator 的 command 集合完整
  → terminal_id 或 client_id 存在时，None 仍只覆盖该 route，不代表账户所有 session
  → 完整请求范围必须来自同一可信 Store read snapshot，并以 command_scope_complete=true 显式持久化
  → 只有 terminal_id=None 且 client_id=None 的无路由限制 scope 才有资格推进账户级 pending-command watermark

command_ids = Some([...])
  → targeted command scope
  → 数组必须非空、command_id 唯一，并在持久化前按 command_id 稳定排序
  → Some([]) 非法；不能把它解释成全量 scope
```

`CONNECTION_RESTORED` 和 `STATE_STORE_RESTORED` 每次都必须创建新的 `request_id` 和独立 reconciliation run。无 route 限制时它们可以覆盖账户级全量 scope；指定 `terminal_id` 或 `client_id` 时只覆盖该 route。两种情况都不能把已经由 `command.received` / `ExecutionEvent` 推进的 command 倒退到 `RECONCILING`，也不能复用旧 run 来覆盖旧证据。

Result snapshot 规则：

```text
request_id / account_id / terminal_id / client_id 必须与 request route 一致
observed_at 必须位于 request 之后、Core 接收时间之前的服务器时间域窗口
positions / orders 表示 observed_at 时刻该账户的完整集合；空数组也是有效完整集合
每个 position / order 行必须属于 result.account_id，且行 observed_at == result.observed_at
position_id / broker_order_id 不得在同一 result 中重复
positions / orders / symbol_metadata / unresolved_command_ids 在持久化前必须按各自业务键稳定排序
account 可缺省；存在时 account_id / observed_at 必须与 result 一致，缺省时不能推进 account refresh evidence
每个 symbol_metadata 行的 account_id / observed_at 必须与 result 一致，broker_symbol 不得重复
symbol_metadata 数组不是已声明范围的完整集合；仅凭非空数组不能推进完整 metadata readiness
```

规则：

```text
Reconciliation service owns reconciliation run / result evaluation / disposition
Execution Layer alone owns command lifecycle；Reconciliation 只返回可选 CAS target，持久化 run 不表示 target 已应用
Gateway only routes reconciliation.request / result
Execution Client reads broker terminal state and returns snapshots
reconciliation.result 不是执行事实来源
如果 result 与 ExecutionEvent 冲突，以 ExecutionEvent 为事实来源，并写 audit.event
Completed 只表示该 scope 的投递不确定性已被权威 execution lifecycle evidence 覆盖，且客户端未报告 unresolved、没有既有 manual command finding；它不表示订单 terminal，也不单独证明账户级 command scope 已完整读取
缺少权威 execution evidence 或 unresolved_command_ids 尚未收敛时保持 PendingEvidence
客户端报告 unresolved 时，即使 Core 已有权威 command state 也不能自动覆盖该差异，必须保持 PendingEvidence + finding
本 run 观察到 command 已是 MANUAL_RECONCILIATION_REQUIRED 时先保持 PendingEvidence + finding，不从 result 自动产出 ManualRequired
ManualRequired 必须由调用方提交带服务器时间和非空 reason 的显式人工 / 超时证据
result 缺失本身不触发隐式 timer；调用方可以用显式 missing-result evidence 升级 ManualRequired
durable result commit 只接受 Completed / PendingEvidence；ManualRequired 必须走独立 explicit escalation API
snapshot / result 不决定 FAILED / EXPIRED，不授权自动 retry
```

Reconciliation 领域里程碑只实现 pure request planning / result evaluation 和 V0004 durable run / checkpoint / full-set projection。它持久化 transport-neutral request，不创建 `WireMessage` 或 `wire_outbox`，不选择 active session。Gateway outbound adapter 只能绑定 route、写 wire outbox / delivery attempt 并报告 delivery outcome，不能拥有上述评估和 lifecycle 决策。

---

## 5. 服务间认证与最小安全边界

系统内所有服务间调用必须具备最低限度认证，防止任意进程伪造市场数据、执行事件或计算结果。

### 5.1 Execution Client → Gateway

Execution Client 连接 Gateway 的 Native TCP 或 Execution WebSocket endpoint 时必须进行 client auth secret 握手。

```text
Execution Client 建立 transport 连接
  → 发送 hello { client_id, platform, terminal_id, account_id, token, capabilities }
  → Gateway 校验 token
  → 通过后注册 session
  → 失败则断开连接并写 system.event: AUTHENTICATION_FAILED
```

安全规则：

```text
client auth secret 用于连接准入
command signing secret 用于 execution.command HMAC
两类 secret 必须分离
secret 必须按 client_id / account_id 维度配置，并支持轮换
```

session identity binding：

```text
Gateway 认证通过后生成 authenticated session context:
  { session_id, client_id, account_id, terminal_id, platform }

hello 之后的所有 inbound WireMessage:
  → session_id 必须等于当前 transport session 的 authenticated session_id
  → client_id 如果出现，必须等于 authenticated session context.client_id

所有带 account_id / client_id / terminal_id 的 payload:
  → 必须与 authenticated session context 一致
  → 不允许客户端在 payload 中声明另一个账户或终端

所有 outbound execution.command:
  → Gateway 只能投递到匹配 account_id / client_id / terminal_id 的 active session
  → Execution Client 必须拒绝与本地 account / terminal / client 不匹配的 command

不匹配处理:
  → 不发送 command.received
  → 不进入 command inbox
  → 写 system.event: SESSION_IDENTITY_MISMATCH
  → 严重或重复出现时关闭 session
```

### 5.2 Trading Core → Strategy & Decision Control Plane / 内部服务

Trading Core 对 Strategy & Decision Control Plane、UI、内部服务暴露的 HTTP / WS API 必须启用认证。最小可用配置可以使用固定 API Key；生产环境应升级为 mTLS 或短期 token。

```text
Authorization: Bearer <internal_service_token>
X-Request-Id: <request_id>
X-Correlation-Id: <correlation_id>
```

`X-Internal-Api-Key` 只允许作为本地开发兼容模式，不作为正式协议字段。

### 5.3 Strategy & Decision Control Plane → Compute Service

Python FastAPI 必须启用 bearer token 校验。

```text
Authorization: Bearer <compute_service_token>
```

### 5.4 安全边界

```text
未经认证的 Execution Client 不能发布 market.bar / market.tick
未经认证的内部服务不能写 execution.event
未经认证的调用不能访问 compute-service
所有认证失败写 system.event: AUTHENTICATION_FAILED
```

Execution Client transport 部署边界：

```text
同机部署或受控私有网络：
  → 只有 loopback 或具备明确网络隔离和访问控制的受控私网才允许明文 Native TCP / Execution WebSocket
  → 仍必须使用 client auth secret + command HMAC

跨主机、跨安全边界或云主机部署：
  → 必须在 Native TCP / Execution WebSocket 上启用 TLS，服务间优先使用 mTLS
  → WireGuard / Tailscale 等 overlay 只构成额外网络隔离，不能替代跨安全边界的 TLS 身份与传输保护
  → Gateway 不应暴露在公网
  → client auth secret 和 command signing secret 不得通过明文配置分发
```

`execution.command` 必须有 HMAC。`command.received`、`execution.event`、`account.snapshot`、`position.snapshot` 依赖连接认证与传输层保护；如果运行在非受控网络，也应增加消息级签名或强制 mTLS。

### 5.5 Secret Rotation

client auth secret 和 command signing secret 必须支持平滑轮换。

推荐 secret 状态：

```text
ACTIVE
NEXT
RETIRED
REVOKED
```

轮换流程：

```text
1. 为 client_id / account_id 生成 NEXT secret。
2. Trading Gateway / Trading Core 同时接受 ACTIVE 和 NEXT。
3. Execution Client reload 配置并开始使用 NEXT。
4. Gateway 观测到 client 使用 NEXT 成功连接和验签。
5. 将 NEXT 提升为 ACTIVE，旧 ACTIVE 标记为 RETIRED。
6. grace period 结束后旧 secret 标记为 REVOKED。
```

规则：

```text
auth secret 和 command signing secret 分开轮换
轮换必须写 audit.event
验签失败不得自动 fallback 到所有历史 secret，只能接受 ACTIVE / NEXT
client auth 握手只能接受该 client_id / account_id 配置的 ACTIVE / NEXT secret。当前 `ConfiguredClientAuthenticator` 的 credential selector 仅为 `(client_id, account_id)`；认证成功后 session context 固定 hello/transport 得到的 terminal_id / platform / remote identity，后续 payload 不得覆盖，但不能宣称 credential 本身已经限制这三个字段。若生产部署需要按 terminal/platform/remote allowlist 授权，必须扩展 credential schema 后再开放；日志和 Debug 输出不得包含 token 或配置 secret
REVOKED secret 命中必须写 system.event: AUTHENTICATION_FAILED
轮换失败必须支持回滚到旧 ACTIVE
```

---

## 6. 统一事件 Envelope

所有 Redis Streams 消息必须包统一 envelope。

```ts
export interface EventEnvelope<T> {
  event_id: string
  correlation_id: string
  causation_id?: string

  type: string
  source: string
  schema_version: string

  created_at: number
  received_at?: number
  observed_at?: number

  payload: T
}
```

### 字段语义

| 字段               | 含义                                          |
| ------------------ | --------------------------------------------- |
| `event_id`       | 当前事件唯一 ID                               |
| `correlation_id` | 一条交易链路的统一追踪 ID                     |
| `causation_id`   | 当前事件由哪个上游事件触发                    |
| `type`           | 事件类型，例如 `market.bar`                 |
| `source`         | 事件来源，例如 `mt5-adapter`                |
| `schema_version` | 消息 schema 版本                              |
| `created_at`     | server-time domain 的系统创建时间             |
| `received_at`    | Gateway / consumer 收到消息时的 server_now_ms |
| `observed_at`    | 外部状态被观测到时的 server-time domain 时间  |
| `payload`        | 业务载荷                                      |

### Dead Letter Event

无法解析、schema 不兼容、缺少必填字段或类型错误的消息必须写入 `deadletter.event`，不得静默丢弃。

```ts
export interface DeadLetterEvent {
  deadletter_id: string
  original_type?: string
  original_message_id?: string
  original_event_id?: string
  source: string
  reason:
    | "UNKNOWN_TYPE"
    | "SCHEMA_MAJOR_MISMATCH"
    | "MISSING_REQUIRED_FIELD"
    | "INVALID_FIELD_TYPE"
    | "INVALID_HMAC"
    | "DECODE_FAILED"
    | "WIRE_FRAME_TOO_LARGE"
    | "WIRE_PROTOCOL_VIOLATION"

  raw_payload?: string
  raw_payload_length?: number
  error_message: string
  created_at: number
}
```

`deadletter.event` 只能用于排查和人工处理，不得被业务流程直接消费为有效事件。

---

## 7. 核心数据模型

### 7.1 Common Types

```ts
export type SymbolCode = string
export type TimeframeCode = string

export type ErrorCode =
  | "ACCOUNT_SNAPSHOT_STALE"
  | "SYMBOL_METADATA_STALE"
  | "ORDER_SNAPSHOT_STALE"
  | "TRADE_INTENT_EXPIRED"
  | "TRADE_INTENT_TIME_INVALID"
  | "DUPLICATE_TRADE_INTENT"
  | "DUPLICATE_COMMAND"
  | "DUPLICATE_IDEMPOTENCY_CONFLICT"
  | "INVALID_HMAC"
  | "AUTHENTICATION_FAILED"
  | "SESSION_IDENTITY_MISMATCH"
  | "COMMAND_EXPIRED"
  | "COMMAND_DELIVERY_TIMEOUT"
  | "COMMAND_DELIVERY_UNCONFIRMED"
  | "COMMAND_DELIVERY_FAILED"
  | "COMMAND_DISPATCH_BACKPRESSURE"
  | "COMMAND_INFLIGHT_LIMIT_REACHED"
  | "BROKER_REJECTED"
  | "BROKER_TIMEOUT"
  | "INSUFFICIENT_MARGIN"
  | "INVALID_VOLUME"
  | "INVALID_PRICE"
  | "INVALID_STOPS"
  | "TRADE_MODE_DISABLED"
  | "RECONCILIATION_FAILED"
  | "MANUAL_RECONCILIATION_REQUIRED"
  | "SCHEMA_VALIDATION_FAILED"
  | "BAD_REQUEST"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "METHOD_NOT_ALLOWED"
  | "CONFLICT"
  | "IDEMPOTENCY_KEY_CONFLICT"
  | "RATE_LIMITED"
  | "INTERNAL_ERROR"
  | "SERVICE_UNAVAILABLE"
  | "MARKET_SNAPSHOT_STALE"
  | "RISK_INPUT_INVALID"
  | "RISK_LIMIT_EXCEEDED"
  | "EXPOSURE_LIMIT_EXCEEDED"
  | "POSITION_LIMIT_EXCEEDED"
  | "RISK_REDUCTION_NOT_PROVABLE"
  | "PENDING_EXPOSURE_CONFLICT"
  | "RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED"
  | "REDIS_UNAVAILABLE"
  | "STATE_STORE_UNAVAILABLE"
  | "CLOCK_SKEW_DETECTED"
  | "TIME_SYNC_UNHEALTHY"
  | "LONG_TERM_AUDIT_WRITE_FAILED"
  | "SECRET_ROTATION_FAILED"
  | "DEADLETTER_CREATED"
  | "UNKNOWN_TYPE"
  | "SCHEMA_MAJOR_MISMATCH"
  | "MISSING_REQUIRED_FIELD"
  | "INVALID_FIELD_TYPE"
  | "DECODE_FAILED"
  | "WIRE_FRAME_TOO_LARGE"
  | "WIRE_PROTOCOL_VIOLATION";

export const SUPPORTED_SYMBOLS = ["XAUUSD", "BTCUSD"] as const
export const SUPPORTED_TIMEFRAMES = ["H1", "H4"] as const
```

说明：

```text
SymbolCode 和 TimeframeCode 使用 string，避免类型层面硬编码扩展瓶颈。
SUPPORTED_SYMBOLS / SUPPORTED_TIMEFRAMES 用于运行时校验和配置约束。
新增品种或周期时只改集中配置，不改所有数据结构类型。
ErrorCode 必须集中维护。跨模块 reason / error_code 字段优先使用 ErrorCode，详细说明放入 message / metadata。
```

### 7.2 MarketBar

```ts
export interface MarketBar {
  symbol: SymbolCode
  timeframe: TimeframeCode
  timestamp: number

  open: number
  high: number
  low: number
  close: number
  volume: number
}
```

### 7.3 MarketSnapshot

```ts
export interface MarketSnapshot {
  symbol: SymbolCode
  broker_symbol?: string
  bid: number
  ask: number
  spread: number
  observed_at: number
}
```

### 7.4 SymbolMetadataSnapshot

```ts
export interface SymbolMetadataSnapshot {
  account_id: string

  symbol: SymbolCode
  broker_symbol: string

  digits: number
  point: number
  tick_size: number
  tick_value_loss: number
  contract_size: number

  volume_min: number
  volume_max: number
  volume_step: number

  stops_level_points: number
  freeze_level_points: number
  margin_initial?: number
  margin_maintenance?: number

  trade_mode: "FULL" | "LONG_ONLY" | "SHORT_ONLY" | "CLOSE_ONLY" | "DISABLED"

  observed_at: number
}
```

`tick_value_loss` 表示当前 `account_id` 的账户币种中，1 lot 每一个 `tick_size` 的保守亏损侧价值。Execution Client / broker adapter 必须基于终端或交易所实际合约口径提供它；如果无法给出与 `AccountSnapshot.currency` 一致的有效正值，Risk Layer 必须 fail closed，不得只用 `contract_size` 猜测。

`SymbolMetadataSnapshot` 是风控、下单格式化和 HMAC canonical number formatting 的共同依赖。Risk Layer 和 Execution Layer 不得依赖硬编码 digits / volume step / tick value。

### 7.5 IndicatorSnapshot

```ts
export interface IndicatorSnapshot {
  symbol: SymbolCode
  timeframe: TimeframeCode
  timestamp: number

  rsi?: number
  bbw?: number
  bb_upper?: number
  bb_lower?: number
  bb_mid?: number

  par?: number
  par_direction?: "UP" | "DOWN" | "FLAT"

  ema_21?: number
  ema_55?: number
  ema_200?: number
  ema_alignment?: "BULLISH" | "BEARISH" | "MIXED"

  adx?: number
  atr?: number
}
```

### 7.6 SignalRaw

```ts
export interface SignalRaw {
  symbol: SymbolCode
  timeframe: TimeframeCode
  timestamp: number

  rsi?: number
  bbw?: number
  par_direction?: "UP" | "DOWN" | "FLAT"
  ema_alignment?: "BULLISH" | "BEARISH" | "MIXED"
  adx?: number

  metadata?: Record<string, unknown>
}
```

### 7.7 SignalScored

```ts
export interface SignalScored {
  symbol: SymbolCode
  timeframe: TimeframeCode
  timestamp: number

  score: number
  confidence: number
  ic_score?: number
  regime: "TREND" | "RANGE" | "VOLATILE" | "UNKNOWN"

  reason: string
}
```

### 7.8 StrategyDecision

```ts
export interface StrategyDecision {
  decision_id: string
  strategy_id: string

  symbol: SymbolCode
  timeframe: TimeframeCode

  action: "BUY" | "SELL" | "CLOSE" | "HOLD"
  confidence: number
  reason: string

  proposed_risk_pct: number
  proposed_sl?: number
  proposed_tp?: number

  timestamp: number
  signal_expires_at: number
}
```

说明：

```text
timestamp         = strategy decision 生成时间，必须使用 Trading Core server-time domain
signal_expires_at = 策略信号失效时间，由 Strategy & Decision Control Plane / Strategy Runtime 负责，必须使用 Trading Core server-time domain
```

### 7.8.1 TradeIntent

```ts
export interface TradeIntent {
  intent_id: string
  decision_id: string
  strategy_id: string
  correlation_id: string
  idempotency_key: string

  account_id: string

  symbol: SymbolCode
  timeframe: TimeframeCode

  action: "BUY" | "SELL" | "CLOSE" | "HOLD"
  confidence: number
  reason: string

  proposed_risk_pct: number
  proposed_sl?: number
  proposed_tp?: number
  proposed_legs?: Array<{
    leg_id: string
    symbol: SymbolCode
    action: "BUY" | "SELL" | "CLOSE"
    ratio: number
    proposed_sl?: number
    proposed_tp?: number
  }>

  decision_timestamp: number
  signal_expires_at: number
  requested_at: number
}
```

`TradeIntent` 是 Strategy & Decision Control Plane 提交给 Trading Core 的交易意图，不是执行命令。它不能包含 broker_order_id、magic、filling_policy、最终 lots、最终 order_type 或 HMAC。Trading Core 必须基于最新状态、RiskResult 和 execution policy 生成最终 `execution.plan` / `execution.command`。

`TradeIntent.account_id` 标识目标交易账户，必须与调用方的授权账户范围一致。`TradeIntent.idempotency_key` 是 TradeIntent 业务幂等键，必须与 `POST /trade-intents` 的 `X-Idempotency-Key` 一致，并由 Trading Core 持久化后再进入 hard risk 与 execution flow。

`proposed_risk_pct` 使用 percentage-point 口径，`1.0` 表示 `1%`，不是 `100%` 或 `0.01%`。它是上游提议上限，不是最终风险批准，也不是 lots。BUY / SELL intent 的值必须 finite 且位于 `(0, 100]`；零、负数或超过 `100` 是畸形输入，必须返回 `RISK_INPUT_INVALID`，不得靠 policy min 静默修正。HOLD 固定为 `0` 并走 no-op shape，不进入 actionable sizing。

`TradeIntent.decision_timestamp` 必须原样复制产生该 intent 的 `StrategyDecision.timestamp`，且满足 `0 <= decision_timestamp <= requested_at < signal_expires_at`。`decision_timestamp`、`signal_expires_at` 与 `requested_at` 都必须使用 Trading Core server-time domain。Strategy & Decision Control Plane 不能用本地 wall clock 生成控制时间；需要通过 Trading Core `/state`、`/time` 或 WS heartbeat 维护 server time offset。Trading Core intake 和后续 trusted assembler 都不得用 `requested_at` 代替或补造 `decision_timestamp`。

### 7.9 AgentReview

```ts
export interface AgentReview {
  review_id: string

  score_adjustment: number
  recommendation: "none" | "skip" | "reduce_risk" | "manual_review"

  reason: string
}
```

`score_adjustment` 必须是 finite 且位于 `-1.0 ~ 1.0`；越界输入必须 fail closed，不得在 hard-risk path 中静默 clamp，因为 clamp 会改变已绑定到 `risk_request_hash` 的输入语义。第一版 pure evaluator 只接受缺省 review 或 `recommendation="none"`；`skip / reduce_risk / manual_review` 必须先在 Strategy & Decision Control Plane 完成有审计记录的决策，不能由 Risk evaluator 隐式改写 TradeIntent 风险或绕过人工流程。当前模型尚未携带该决策的结构化 resolution，因此这些 recommendation 直接进入 `RiskRequest` 时返回 `RISK_INPUT_INVALID`。

### 7.10 AccountSnapshot

```ts
export interface AccountSnapshot {
  account_id: string

  balance: number
  equity: number
  margin: number
  free_margin: number
  currency: string

  observed_at: number
}
```

### 7.11 PositionSnapshot

```ts
export interface PositionSnapshot {
  account_id: string

  symbol: SymbolCode
  position_id: string
  side: "BUY" | "SELL"

  lots: number
  open_price: number
  sl?: number
  tp?: number

  floating_pnl: number
  observed_at: number
}
```

### 7.11.1 OrderSnapshot

```ts
export interface OrderSnapshot {
  account_id: string
  terminal_id?: string
  client_id?: string

  symbol: SymbolCode
  broker_symbol?: string

  broker_order_id: string
  position_ticket?: string
  command_id?: string
  plan_id?: string
  leg_id?: string
  idempotency_key?: string

  side: "BUY" | "SELL"
  order_type: "MARKET" | "LIMIT" | "STOP" | "STOP_LIMIT"
  status:
    | "PLACED"
    | "PARTIALLY_FILLED"
    | "FILLED"
    | "CANCELLED"
    | "REJECTED"
    | "EXPIRED"
    | "UNKNOWN"

  requested_lots: number
  filled_lots: number
  remaining_lots: number

  price?: number
  sl?: number
  tp?: number

  created_at?: number
  updated_at?: number
  observed_at: number
}
```

`OrderSnapshot` 用于 `DELIVERY_UNCONFIRMED`、Saga 恢复和人工 reconciliation。它不是执行事实来源；执行事实仍然来自 `ExecutionEvent`。`OrderSnapshot` 只能产生差异 finding、刷新 full-set projection 或要求继续等待 / 人工核对，不能单独授权 retry / rollback，也不能推进 execution lifecycle。

### 7.12 RiskRequest

```ts
export interface PositionSizingCandidate {
  leg_id: string
  symbol: SymbolCode
  action: "BUY" | "SELL"
  ratio: number

  worst_entry_price: number
  stop_loss_price: number
  estimated_cost_per_lot: number
}

export interface RiskStateWatermarks {
  positions_observed_at: number
  orders_observed_at: number
  pending_commands_reconciled_at: number
}

export interface RiskCapacity {
  account_id: string
  strategy_id: string
  observed_at: number
  daily_realized_loss_pct: number
  equity_drawdown_pct: number
  remaining_account_risk_pct: number
  remaining_portfolio_risk_pct: number
  remaining_strategy_legs: number
}

export interface RiskMarketSnapshot {
  account_id: string
  snapshot: MarketSnapshot
}

export interface RiskRequest {
  request_id: string
  risk_id: string
  evaluated_at: number

  decision: StrategyDecision
  intent: TradeIntent
  agent_review?: AgentReview

  account: AccountSnapshot
  positions: PositionSnapshot[]
  orders: OrderSnapshot[]
  symbol_metadata: SymbolMetadataSnapshot[]
  pending_commands: ExecutionCommand[]
  pending_command_states: ExecutionCommandState[]

  policy: RiskPolicy
  strategy_policy: StrategyRiskPolicy
  markets: RiskMarketSnapshot[]
  sizing_candidates: PositionSizingCandidate[]
  state_watermarks: RiskStateWatermarks
  capacity: RiskCapacity
}
```

`RiskRequest` 是 Trading Core 内部不可变风控评估对象，不是 Strategy & Decision Control Plane 的外部提交 payload。外部只提交 `TradeIntent`；Trading Core 内可信 assembler 必须通过本地 State Store 的单一一致性读快照组装完整 `RiskRequest`，固定 `risk_id` 和服务器时间域的 `evaluated_at` 后再交给 pure evaluator。Risk Layer 不得在评估过程中补读状态、调用 Compute Service 或依赖网络服务。

`positions` 和 `orders` 必须分别表示目标账户的完整集合；`state_watermarks.positions_observed_at` 和 `orders_observed_at` 是账户级 full-set 水位，即使数组为空也必须存在。每个 position / order 行的 `observed_at` 必须精确等于对应 full-set 水位。`pending_commands_reconciled_at` 证明全账户、所有 session 的 command lifecycle 与 broker order 集合已经完成对账，因此只能由无 `terminal_id / client_id` route 限制且 scope 完整的账户级 reconciliation 推进；account、position full-set 和 order full-set 证据不得早于该水位，`capacity.observed_at` 不得早于这四项依赖证据中的任一项。逐行 latest observation 不能证明未出现的 position / order 已不存在，因此 assembler 在尚无新鲜且因果一致的 full-set / reconciliation 证据时必须 fail closed。`account`、capacity、所有 position / order / metadata / pending command 以及 `RiskMarketSnapshot.account_id` 必须与 `intent.account_id` 一致。

Trading Core 必须在 hard risk 前从 `TradeIntent`、同账户最新 market snapshot 和本地 execution policy 派生 `sizing_candidates`。`worst_entry_price` 必须包含对 spread 和允许滑点的保守估计；pending order 必须使用候选入场价再叠加不利执行 buffer。没有显式 `proposed_legs` 的单腿 BUY / SELL 必须使用稳定合成 ID `leg:{intent_id}:0` 且 ratio 必须为 `1`；多腿必须按唯一 `leg_id` 将 intent leg、candidate、metadata 和 account-scoped market 一一匹配，并同时校验 symbol / action 一致。任一缺失、重复或错配都必须 fail closed。

`pending_commands` 提供不可变 command 载荷，`pending_command_states` 提供 lifecycle 状态。两者必须按 `command_id` 一一对应，并精确匹配 `account_id / plan_id / leg_id`；state 的 lifecycle 时间不得晚于 `pending_commands_reconciled_at`，terminal status 必须携带合法 `completed_at`，非 terminal status 不得携带完成时间。Risk Layer 做敞口和重复下单检查时以 state 判断 command 是否仍然有效，以 command payload 读取 symbol / lots / action 等执行细节。合法 terminal command 不要求无关的 market / metadata；非 terminal BUY / SELL command 必须有合法的 order type、lots，并为非 MARKET order 提供 price。

Active order / BUY-SELL command 的 `broker_symbol` 保持 optional；但一旦存在，必须与同一 canonical `symbol` 的 `SymbolMetadataSnapshot.broker_symbol` 精确一致。不得把 broker symbol B 标成 canonical symbol A 后使用 A 的 tick value / margin 计算。position / order 的核心 identity 和 canonical symbol 也不得为空。

第一版 `RiskRequest` 没有携带 MODIFY 的 risk-reduction proof。任何 non-terminal MODIFY 都可能在当前 full-set snapshot 之后改变 pending order price / lots 或放宽 position stop，因此 Risk evaluator 必须以 `PENDING_EXPOSURE_CONFLICT` 阻断新的风险增加 intent；不得从 action 名称推断它只降低风险。CLOSE / CANCEL 继续按修改生效前的完整 position / order 暴露计数，是保守上界。未来只有模型能绑定 target、before/after 参数并证明风险不增加后，才能放行 pending MODIFY。

`capacity` 是与本次账户、策略和状态水位一致的日内亏损、回撤、剩余账户 / 组合风险容量及 `remaining_strategy_legs` 快照。`account_id / strategy_id` 必须与 intent 一致，新 legs 数不得超过 trusted assembler 给出的剩余策略容量。它必须参与 freshness、因果水位和 hard-limit 校验，不得由 Risk Layer 在评估期间从外部服务临时获取。

Rust evaluator 的边界为 `Result<RiskResult, RiskEvaluationError>`。策略、状态、行情或算术导致的业务拒绝必须返回 `Ok(RiskResult { approved: false, ... })` 以形成可持久化审计事实；只有空 `risk_id / request_id / intent_id / account_id / decision_id`、非法 `evaluated_at`，或连 fail-closed 结果都无法通过 `RiskResult::validate` 时，才返回 `Err(RiskEvaluationError)`，因为此时无法构造合法审计身份。

### 7.13 RiskResult

```ts
export interface RiskResult {
  risk_id: string
  request_id: string
  intent_id: string
  account_id: string
  risk_request_hash: string
  approved: boolean

  reason: ErrorCode | "OK"
  message?: string
  sizing_version?: string
  risk_base_amount?: number
  risk_budget_amount?: number
  adjusted_risk_pct?: number
  sizing_candidates?: PositionSizingCandidate[]
  adjusted_legs?: Array<{
    leg_id: string
    symbol: SymbolCode
    action: "BUY" | "SELL"
    lots: number
    risk_amount: number
    risk_pct: number
    sizing_entry_price: number
    approved_sl: number
    loss_per_lot: number
    reason?: ErrorCode | "OK"
  }>

  decision_id: string

  snapshot_age_ms: number
  market_snapshot_age_ms: number
  symbol_metadata_age_ms: number
  capacity_age_ms: number
  evaluated_at: number
  valid_until: number
}
```

说明：

```text
snapshot_age_ms        = 风控评估时使用的 account / position / order snapshot 距当前时间的最大年龄
market_snapshot_age_ms = 风控评估时使用的 account-scoped market snapshot 最大年龄
symbol_metadata_age_ms = 风控评估时使用的 symbol metadata 距当前时间的年龄
capacity_age_ms        = 风控评估时使用的 RiskCapacity 年龄
evaluated_at           = 风控完成评估的时间戳
valid_until            = 本次风控审批失效时间，由 Risk Layer 负责，必须不晚于相关 snapshot / order snapshot / metadata freshness 边界
```

`risk_request_hash` 是完整不可变 `RiskRequest` 确定性序列化后的 lowercase SHA-256，用于把审批结果绑定到唯一输入；`sizing_candidates` 在 actionable approval 中保留最终 sizing 所使用的完整 candidate provenance。对 `approved=true` 且包含任一风险增加 BUY / SELL leg 的结果，`sizing_version`、`risk_base_amount`、`risk_budget_amount`、`adjusted_risk_pct`、`sizing_candidates` 和 `adjusted_legs` 必填；candidate 与 `adjusted_legs` 必须按 `leg_id` 一一对应，不得缺失、重复或增加额外 leg。单腿策略使用只有一个元素的 candidate 和 `adjusted_legs`。

`approved=false` 的 `RiskResult` 不得携带可执行 lots。第一版动作规则固定如下：

```text
HOLD
  → approved=true 的 no-op RiskResult
  → sizing_version / risk base / budget / adjusted_risk_pct / adjusted_legs 全部为空
  → Execution 不得创建 plan / command

CLOSE
  → 当前 TradeIntent 没有目标 position 和 close lots，无法证明只降低风险
  → approved=false，reason=RISK_REDUCTION_NOT_PROVABLE
  → 不得创建 plan / command

BUY / SELL
  → 一律按风险增加处理并执行完整 sizing / hard-limit 校验
```

只有未来模型能够明确表达目标 position 和 close lots，并可由新鲜 full-set position 证据证明不会反向开仓时，`CLOSE` 才能改为风险降低路径。

#### Position Sizing 确定性换算

第一版使用保守的 fixed-risk-at-stop 模型。单腿 ratio 固定为 `1`；多腿 ratio 是相对 lots 系数，风险预算、敞口和保证金按所有腿的绝对风险贡献求和，不得使用多空方向、相关性、对冲或预期抵消减少 hard-risk budget。

```text
risk_base = min(max(account.balance, 0), max(account.equity, 0))

approved_risk_pct = min(
  valid TradeIntent.proposed_risk_pct,
  RiskPolicy.max_risk_per_trade_pct,
  StrategyRiskPolicy.max_risk_per_trade_pct,
  remaining account / portfolio risk cap
)

risk_budget = risk_base * approved_risk_pct / 100

loss_ticks_i = ceil(
  abs(candidate.worst_entry_price - candidate.stop_loss_price)
  / metadata.tick_size
)

loss_per_lot_i =
  loss_ticks_i * metadata.tick_value_loss
  + max(candidate.estimated_cost_per_lot, 0)

scale = risk_budget / sum(candidate.ratio * loss_per_lot_i)
raw_lots_i = scale * candidate.ratio
lots_i = floor_to_volume_step(raw_lots_i, metadata.volume_step)

actual_risk = sum(lots_i * loss_per_lot_i)
adjusted_risk_pct = actual_risk / risk_base * 100

notional_per_lot_i =
  ceil(abs(conservative_price_i) / metadata.tick_size)
  * metadata.tick_value_loss

new_margin = sum(lots_i * metadata.margin_initial_i)

pending_margin + new_margin <= account.free_margin

account.margin + pending_margin + new_margin
  <= risk_base * RiskPolicy.max_margin_usage_pct / 100
```

`notional_per_lot_i` 是第一版账户币种敞口的保守近似，`conservative_price_i` 使用 candidate 的不利价格。当前仓位、有效 pending order / command 和新批准 legs 的账户总敞口及逐品种敞口都必须按该口径累加后分别检查 `max_total_exposure_pct` 和 `max_symbol_exposure_pct`。Risk evaluator 不根据方向、broker order id 或推测的 command-order 对应关系抵消 pending order 与 pending command；二者同时存在时保守累加敞口和保证金，直到 reconciliation 提供 terminal / 去重后的可信完整输入。`margin_initial` 缺失、非 finite 或 `<= 0` 时无法证明新保证金上界，必须 fail closed。

在计算 lots 前后必须执行以下硬约束：

```text
risk_base、approved_risk_pct、ratio、worst_entry_price、stop_loss_price、tick_size、tick_value_loss 必须 finite 且符合各自的正值 / 方向约束。
BUY 必须 stop_loss_price < worst_entry_price；SELL 必须 stop_loss_price > worst_entry_price。
所有 sizing 输入通过有限值校验后，必须先按其 base-10 文本表示转换为 Decimal；Rust 实现使用 `f64::to_string()` 后解析 `rust_decimal::Decimal`。止损 tick 的 ceil、volume-step floor、risk budget、actual risk、敞口和保证金比较从此全部在 Decimal 域完成，不得用 binary float 做最终 step、预算或上限比较。
Decimal lots 写入共享 `f64` DTO 前必须转为 finite `f64`，再用该 `f64` 的 base-10 文本解析回 Decimal。round-trip 后的值不得大于原 Decimal lots，且必须仍然精确落在 `volume_step` 上；否则以 `INVALID_VOLUME` fail closed，不得把不可执行的审计值交给 Execution。
账户 / 品种敞口、volume_max 或保证金上限可以确定性缩小全局 scale，缩小后必须重新向下取整和校验。
任一必需 leg 向下取整后 lots < volume_min 时拒绝整个 intent，不得向上补到 volume_min，也不得静默删除该 leg。
lots 必须 <= volume_max 且 actual_risk <= risk_budget；无法确定损失或保证金上界时 fail closed。
同一完整 RiskRequest 与 sizing_version 必须产生字节级可重现的 sizing 结果。
```

`RiskResult.valid_until` 必须取以下时间边界的最小值，不得因缺项而延长：`evaluated_at + max_approval_ttl_ms`、`decision.signal_expires_at` / `intent.signal_expires_at`，以及本次依赖的 account、position full-set watermark、order full-set watermark、pending-command reconciliation watermark、risk capacity、market 和 symbol metadata 各自的 freshness 截止时间。任一 freshness 边界已经到期时拒绝审批。

Execution Layer 必须按 `leg_id` 把 `RiskResult.adjusted_legs[].lots` 原样映射到 `ExecutionLeg.lots` 和 `ExecutionCommand.lots`。Execution Layer 不得重算、round、clamp、放大或静默缩小 lots；`formatLots` 只能格式化 HMAC 字符串，不得改变数值。如果价格、SL、metadata、保证金或 execution policy 变化导致原审批无法原样执行，必须拒绝生成 / 投递 command 并用新输入重新进入 Risk Layer。

这些字段用于审计和排查风控是否基于足够新鲜的账户状态与交易品种约束。

### 7.14 ExecutionPlan

```ts
export interface ExecutionPlan {
  plan_id: string
  account_id: string
  strategy_id: string

  mode: "sequential" | "simultaneous" | "best_effort_atomic"
  legs: ExecutionLeg[]

  failure_policy: "cancel_all" | "partial_fill" | "retry"
  rollback_policy?: RollbackPolicy

  status:
    | "PENDING"
    | "RECONCILING"
    | "MANUAL_RECONCILIATION_REQUIRED"
    | "PARTIAL"
    | "COMPLETED"
    | "FAILED"
    | "EXPIRED"
    | "CANCELLED"
  filled_legs: string[]
  failed_legs: string[]
}
```

Plan 级别状态用于多腿策略的聚合执行控制。Execution Layer 根据 `filled_legs`、`failed_legs` 与 `failure_policy` 决定是否继续执行、触发 `cancel_all` 或执行 rollback。

### 7.15 ExecutionLeg

```ts
export interface ExecutionLeg {
  leg_id: string

  symbol: SymbolCode
  action: "BUY" | "SELL" | "CLOSE" | "MODIFY" | "CANCEL"

  lots?: number
  sl?: number
  tp?: number

  ratio: number
  dependency: string[]

  status:
    | "PENDING"
    | "SENT"
    | "DELIVERY_UNCONFIRMED"
    | "RECONCILING"
    | "MANUAL_RECONCILIATION_REQUIRED"
    | "COMMAND_RECEIVED"
    | "ACCEPTED"
    | "REJECTED"
    | "ORDER_SENT"
    | "PARTIALLY_FILLED"
    | "FILLED"
    | "FAILED"
    | "EXPIRED"
    | "CANCELLED"
}
```

对风险增加 BUY / SELL leg，`ExecutionLeg.lots` 必填，且必须精确等于同一 `risk_id` 下对应 `RiskResult.adjusted_legs[leg_id].lots`。

### 7.16 ExecutionCommand

```ts
export interface ExecutionCommand {
  command_id: string
  plan_id?: string
  leg_id?: string
  strategy_id: string

  account_id: string
  terminal_id?: string
  client_id?: string

  symbol: SymbolCode
  broker_symbol?: string
  action: "BUY" | "SELL" | "CLOSE" | "MODIFY" | "CANCEL"
  order_type?:
    | "MARKET"
    | "LIMIT"
    | "STOP"
    | "STOP_LIMIT"

  lots?: number
  price?: number
  sl?: number
  tp?: number
  deviation_points?: number

  magic: number
  comment?: string

  position_ticket?: string
  broker_order_id?: string

  filling_policy?: "FOK" | "IOC" | "RETURN"
  time_policy?: "GTC" | "DAY" | "SPECIFIED"
  expiration_time?: number

  expires_at: number
  idempotency_key: string

  hmac: string
}
```

`ExecutionCommand` 是不可变执行请求载荷，不承载全局生命周期状态。生命周期状态由 Execution Layer 维护。

字段说明：

```text
account_id / terminal_id / client_id
  → 多账户、多终端、多执行客户端路由和审计所需

plan_id / leg_id
  → 多腿执行投影所需；由 execution.plan 生成的 command 必须携带 leg_id

symbol / broker_symbol
  → symbol 是系统标准品种，broker_symbol 是券商实际交易品种名

action
  → 业务动作：BUY / SELL / CLOSE / MODIFY / CANCEL

order_type
  → 订单类型：市价 / 限价 / 停止 / stop-limit
  → BUY / SELL 创建订单时必填
  → CLOSE 默认为 MARKET，可为空
  → MODIFY / CANCEL 可为空

lots
  → BUY / SELL 必填
  → 值必须原样来自当前有效 RiskResult.adjusted_legs 的同 leg_id 审批结果
  → CLOSE 部分平仓时必填；全平可为空并由 Execution Client 按 position_ticket 解析
  → MODIFY / CANCEL 可为空

price
  → LIMIT / STOP / STOP_LIMIT 必填；MARKET 可为空

deviation_points
  → MT5 市价单允许滑点

magic / comment
  → MT5 订单标识和审计备注

position_ticket
  → CLOSE 在 hedging 账户下必须明确目标 position
  → MODIFY position 时必须明确目标 position

broker_order_id
  → MODIFY / CANCEL 时用于指定待修改或待取消订单

filling_policy / time_policy / expiration_time
  → MT5 order filling / time policy 映射

expires_at
  → command 最后可执行时间
  → 由 Execution Layer 根据 strategy_decision.signal_expires_at、risk_result.valid_until 和 execution_policy.max_command_ttl_ms 派生
  → Execution Layer 可以因为执行约束缩短它，但不得晚于任何上游有效期
```

action-specific validation：

```text
BUY / SELL
  → lots 必填
  → order_type 必填
  → LIMIT / STOP / STOP_LIMIT 需要 price

CLOSE
  → hedging 账户必须提供 position_ticket
  → lots 为空表示全平，lots 有值表示部分平仓

MODIFY
  → broker_order_id 或 position_ticket 至少一个必填
  → sl / tp / price / expiration_time 至少一个修改字段必填

CANCEL
  → broker_order_id 必填
  → lots / order_type / price 可为空
```

### 7.16.1 ExecutionCommandState

```ts
export interface ExecutionCommandState {
  command_id: string
  account_id: string
  plan_id?: string
  leg_id?: string

  status:
    | "CREATED"
    | "DISPATCHED"
    | "DELIVERY_UNCONFIRMED"
    | "DELIVERY_FAILED"
    | "RECONCILING"
    | "MANUAL_RECONCILIATION_REQUIRED"
    | "COMMAND_RECEIVED"
    | "ACCEPTED"
    | "REJECTED"
    | "ORDER_SENT"
    | "PARTIALLY_FILLED"
    | "FILLED"
    | "FAILED"
    | "EXPIRED"
    | "CANCELLED"

  delivery_attempts: number
  last_delivery_error?: string

  created_at: number
  dispatched_at?: number
  command_received_at?: number
  reconciling_at?: number
  completed_at?: number
  updated_at: number
}
```

状态所有权：

```text
Execution Layer owns ExecutionCommandState
Trading Gateway only reports delivery result / timeout as system.event
Execution Client only reports command.received and execution.event
```

`ORDER_SENT` 由 `execution.event(status=ORDER_SENT)` 投影得到，用于区分“客户端接受命令”和“订单已提交到 broker”。恢复时仍以 `ExecutionEvent` 重放为准。

#### Execution State Transition Rules

Command 状态必须单向推进，terminal state 不得回退。

```text
CREATED
  → DISPATCHED
  → EXPIRED
  → CANCELLED

DISPATCHED
  → COMMAND_RECEIVED
  → DELIVERY_UNCONFIRMED
  → DELIVERY_FAILED
  → EXPIRED
  → CANCELLED

DELIVERY_UNCONFIRMED
  → RECONCILING
  → COMMAND_RECEIVED
  → MANUAL_RECONCILIATION_REQUIRED
  → EXPIRED
  → FAILED

RECONCILING
  → COMMAND_RECEIVED
  → ORDER_SENT
  → PARTIALLY_FILLED
  → FILLED
  → MANUAL_RECONCILIATION_REQUIRED
  → FAILED
  → EXPIRED

MANUAL_RECONCILIATION_REQUIRED
  → COMMAND_RECEIVED
  → ORDER_SENT
  → PARTIALLY_FILLED
  → FILLED
  → FAILED
  → EXPIRED
  → CANCELLED

COMMAND_RECEIVED
  → ACCEPTED
  → REJECTED
  → CANCELLED
  → EXPIRED

ACCEPTED
  → ORDER_SENT
  → REJECTED
  → FAILED
  → CANCELLED
  → EXPIRED

ORDER_SENT
  → PARTIALLY_FILLED
  → FILLED
  → CANCELLED
  → FAILED

PARTIALLY_FILLED
  → FILLED
  → FAILED
  → CANCELLED

Terminal states:
  REJECTED / FILLED / FAILED / EXPIRED / CANCELLED
```

`MANUAL_RECONCILIATION_REQUIRED` 是自动流程阻塞状态，不是执行事实来源。人工处理可以提交可审计的显式 evidence 维持或解除人工工作流，但 `ORDER_SENT / PARTIALLY_FILLED / FILLED / FAILED` 等 broker 执行状态仍必须由 `ExecutionEvent` 推进；`OrderSnapshot` 单独不能恢复这些投影，也不能伪造执行事实。

Leg 与 Plan 状态不是事实来源，只能由 `ExecutionCommandState` 与 `ExecutionEvent` 投影：

```text
ExecutionLeg.status
  → 按 leg_id 聚合该 leg 的 command states / execution events

ExecutionPlan.status
  → 按 plan_id 聚合所有 leg states
  → 任一 leg 进入 PARTIALLY_FILLED 时 plan 不得回到 PENDING
  → 所有 leg 终态后 plan 才能进入 COMPLETED / FAILED / CANCELLED / EXPIRED
```

`execution.command` 必须带 HMAC 完整性保护。Execution Engine 使用 command signing secret 对 command 的 canonical signing string 签名，Execution Client（例如 MT5 Adapter）验签通过后才允许执行。

#### Execution Engine 签名与 Execution Client 验签约定

Execution Engine 与 Execution Client 必须使用同一套 signing string 构造规则、同一 command signing secret 和同一 HMAC 算法。不要使用 JSON canonicalization 作为跨语言签名格式；不同语言对字段顺序、null、浮点数、转义和编码的处理很容易不一致。

推荐签名格式：

```text
key=value&key=value&...
```

规则：

```text
1. 字段顺序固定，以 23.1 HMAC Signing String 的字段列表为准。
2. hmac 字段本身不参与签名。
3. 缺失的可选字段写为空字符串。
4. 所有字符串使用 UTF-8。
5. 所有字段值先做 RFC3986 percent-encoding。
6. number 字段必须先格式化为固定小数或十进制字符串。
7. bool / enum 使用大写枚举字符串。
```

数字格式建议：

```text
lots              → broker volume step 对齐后的字符串，保留 volume step 对应小数位
price / sl / tp   → broker symbol digits 对齐后的字符串，保留 trailing zeros
deviation_points  → 整数字符串
magic             → 整数字符串
time fields       → Unix milliseconds 整数字符串
```

canonical value 生成规则：

```text
undefined / missing optional field
  → ""

string
  → 原始字符串先转 UTF-8，再做 RFC3986 percent-encoding

number
  → 先按字段规则格式化为十进制字符串，再做 RFC3986 percent-encoding

enum
  → 使用协议枚举值本身，例如 BUY / MARKET / IOC

hmac
  → 不参与 signing string
```

`price / sl / tp` 的 digits 与 `lots` 的 volume step 必须来自同一套 symbol metadata。Execution Engine 生成 command 时应使用 Trading Core State Store 中最新有效的 broker symbol metadata；MT5 Adapter 验签时使用本地 terminal 的同一 `broker_symbol` metadata。两侧 metadata 不一致时必须拒绝执行，而不是放宽验签。

##### Signing String 伪代码

以下 TypeScript 片段只表达 canonical string 算法；生产实现由 Execution Engine 生成，并必须与 MT5 Adapter golden vector 一致。

```ts
function rfc3986Encode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (ch) =>
    `%${ch.charCodeAt(0).toString(16).toUpperCase()}`
  )
}

const fields: [string, string][] = [
  ["command_id", command.command_id],
  ["plan_id", command.plan_id ?? ""],
  ["leg_id", command.leg_id ?? ""],
  ["strategy_id", command.strategy_id],
  ["account_id", command.account_id],
  ["terminal_id", command.terminal_id ?? ""],
  ["client_id", command.client_id ?? ""],
  ["symbol", command.symbol],
  ["broker_symbol", command.broker_symbol ?? ""],
  ["action", command.action],
  ["order_type", command.order_type ?? ""],
  ["lots", formatLots(command.lots)],
  ["price", formatPrice(command.price)],
  ["sl", formatPrice(command.sl)],
  ["tp", formatPrice(command.tp)],
  ["deviation_points", formatInt(command.deviation_points)],
  ["magic", formatInt(command.magic)],
  ["comment", command.comment ?? ""],
  ["position_ticket", command.position_ticket ?? ""],
  ["broker_order_id", command.broker_order_id ?? ""],
  ["filling_policy", command.filling_policy ?? ""],
  ["time_policy", command.time_policy ?? ""],
  ["expiration_time", formatInt(command.expiration_time)],
  ["expires_at", formatInt(command.expires_at)],
  ["idempotency_key", command.idempotency_key],
]

const canonical = fields
  .map(([key, value]) => `${key}=${rfc3986Encode(value)}`)
  .join("&")

command.hmac = createHmac("sha256", COMMAND_SIGNING_SECRET)
  .update(canonical)
  .digest("hex")
  .toLowerCase()
```

##### MT5 Adapter 验签（MQL5）

```text
1. 确认 execution.command 的 account_id / client_id / terminal_id 匹配本地 terminal context。
2. 从收到的 execution.command 中取出除 hmac 外的签名字段。
3. 按与 Execution Engine 完全相同的字段顺序构造 key=value&... signing string。
4. 可选字段缺失时统一写为空字符串，不省略字段。
5. 使用相同 command signing secret 计算 HMAC-SHA256。
6. 将计算结果与收到的 hmac 字段比对。
7. 验签失败则拒绝执行，并写 system.event: AUTHENTICATION_FAILED。
8. 验签通过后先查本地 idempotency journal；已知 command 返回当前状态。
9. 本地未见过的新 command 才使用 effective_server_now_ms 检查 expires_at；已过期则不得进入 command inbox，不得发送 broker order。
```

验签、身份和过期检查的顺序必须固定：

```text
session identity check
  → 失败: SESSION_IDENTITY_MISMATCH

HMAC check
  → 失败: INVALID_HMAC / AUTHENTICATION_FAILED

local idempotency journal check
  → same command payload + already in inbox: 返回 command.received / 当前 execution.event，不继续后续检查
  → same command payload + rejection record: 返回当前 rejection execution.event，不发送 command.received
  → different command payload: DUPLICATE_IDEMPOTENCY_CONFLICT

expires_at check for new command with effective_server_now_ms
  → 失败: COMMAND_EXPIRED

persist command inbox
  → 成功后才允许发送 command.received
```

`expires_at` 判断必须使用 server-time domain。Execution Client 不得用本地 wall clock 判断过期，只能使用 time sync 维护的 `effective_server_now_ms`。如果 `clock_sync_status != SYNCED`，必须写 `TIME_SYNC_UNHEALTHY` 或 `CLOCK_SKEW_DETECTED` 并拒绝新 command，直到重新同步或人工处理。

MQL5 RFC3986 编码规则：

```text
1. 使用 StringToCharArray(value, bytes, 0, WHOLE_ARRAY, CP_UTF8) 取得 UTF-8 bytes。
2. StringToCharArray 会包含结尾 0 字节，编码时必须跳过最后的 null terminator。
3. unreserved 字节保持原样：A-Z a-z 0-9 - _ . ~
4. 其他字节编码为 %HH，HH 必须大写十六进制。
5. 空格必须编码为 %20，不能编码为 +。
6. 不使用表单 URL encoding，不依赖平台 UrlEncode。
```

MQL5 数字格式化规则：

```text
formatInt(value)
  → IntegerToString((long)value)

formatPrice(value, broker_symbol)
  → digits = (int)SymbolInfoInteger(broker_symbol, SYMBOL_DIGITS)
  → DoubleToString(NormalizeDouble(value, digits), digits)
  → optional missing 时返回 ""

formatLots(value, broker_symbol)
  → step = SymbolInfoDouble(broker_symbol, SYMBOL_VOLUME_STEP)
  → volume_digits = step 推导出的小数位
  → aligned = MathRound(value / step) * step
  → DoubleToString(NormalizeDouble(aligned, volume_digits), volume_digits)
  → optional missing 时返回 ""
```

`DoubleToString` 必须保留固定小数位，不能使用会丢失 trailing zeros 的普通 double 转 string。MQL5 和 TypeScript 都必须先得到同一 canonical number string，再参与 signing string。

MQL5 HMAC-SHA256 实现约束：

```text
1. 如果 command signing secret 的 UTF-8 bytes 长度 > 64，先 SHA256(secret_bytes)。
2. 将 key bytes 右侧补 0 到 64 bytes。
3. ipad = key XOR 0x36。
4. opad = key XOR 0x5c。
5. inner = SHA256(ipad || canonical_utf8_bytes)。
6. digest = SHA256(opad || inner)。
7. digest 输出 lowercase hex。
```

MQL5 可使用 `CryptEncode(CRYPT_HASH_SHA256, data, empty_key, out)` 作为 SHA256 primitive，再按 HMAC 标准拼接 ipad / opad。不要把 `command signing secret` 直接传给普通 SHA256 当作“keyed hash”，那不是 HMAC。

MQL5 侧推荐工具函数边界：

```text
BuildExecutionCommandSigningString(command, broker_symbol_metadata)
Rfc3986EncodeUtf8(value)
FormatCommandString(value)
FormatCommandInt(value)
FormatCommandPrice(value, digits)
FormatCommandLots(value, volume_step)
HmacSha256HexLower(canonical, command_signing_secret)
VerifyCommandHmac(command)
```

字段顺序固定为：

```text
command_id
plan_id
leg_id
strategy_id
account_id
terminal_id
client_id
symbol
broker_symbol
action
order_type
lots
price
sl
tp
deviation_points
magic
comment
position_ticket
broker_order_id
filling_policy
time_policy
expiration_time
expires_at
idempotency_key
```

该顺序与 23.1 HMAC Signing String 及 golden vector 完全一致，不得在不同语言实现中自行调整。`hmac` 字段本身不得参与签名。

### 7.17 ExecutionEvent

```ts
export interface ExecutionEvent {
  execution_id: string
  command_id: string
  plan_id?: string
  leg_id?: string

  account_id: string
  terminal_id?: string
  client_id?: string

  symbol: SymbolCode
  broker_symbol?: string

  status:
    | "ACCEPTED"
    | "ORDER_SENT"
    | "REJECTED"
    | "FILLED"
    | "PARTIALLY_FILLED"
    | "FAILED"
    | "EXPIRED"
    | "CANCELLED"

  broker_order_id?: string
  broker_deal_id?: string
  position_ticket?: string
  idempotency_key?: string

  requested_lots?: number
  fill_price?: number
  filled_lots?: number
  remaining_lots?: number

  event_at: number
  filled_at?: number
  broker_filled_at?: number

  error_code?: ErrorCode | string
  message?: string
}
```

说明：

```text
command.received = command delivery ack，不属于 ExecutionEvent
event_at         = Execution Client 生成该执行回报时的 effective_server_now_ms
filled_at        = Execution Client 观测到成交时的 effective_server_now_ms；仅在成交或部分成交时出现
broker_filled_at = broker 原始成交时间戳；仅用于审计 / 对账，不参与控制判断
```

`ExecutionEvent` 必须尽量自描述，不应只能依赖 join `execution.command` 才能恢复关键上下文。`event_at` / `filled_at` 属于 server-time domain，用于系统审计和交易执行分析。

### 7.18 Partial Fill 恢复语义

当系统重启时，如果存在多腿策略且某些 leg 处于 `PARTIALLY_FILLED`，Execution Layer 必须根据 `RollbackPolicy` 决定恢复动作。

```text
rollback_policy.mode = close_filled
  → 对已成交或部分成交的 leg 生成 CLOSE command
  → plan.status = CANCELLED 或 FAILED

rollback_policy.mode = none
  → 不主动回滚
  → plan.status = PARTIAL
  → 写 audit.event，等待人工或上层策略处理
```

`ExecutionEvent` 是执行事实来源，`ExecutionCommandState` / `ExecutionLeg.status` / `ExecutionPlan.status` 是由事件流投影出的 materialized state。恢复时读取投影状态是为了快速判断当前计划状态；如果投影状态缺失或疑似损坏，必须从 `ExecutionEvent` 事件流重放生成投影，而不是让状态覆盖事实。

### 7.19 AuditEntry

```ts
export interface AuditEntry {
  entry_id: string
  correlation_id: string
  node: string
  event_type: string
  timestamp: number // server_now_ms
  summary: string
  metadata?: Record<string, unknown>
}
```

### 7.20 RollbackPolicy

```ts
export interface RollbackPolicy {
  mode: "close_filled" | "none"
  max_retry_attempts?: number
}
```

说明：

```text
close_filled = 若部分腿成交后整体失败，关闭已成交的腿
none         = 不主动回滚，留给人工处理
```

### 7.21 StrategyRiskPolicy

```ts
export interface StrategyRiskPolicy {
  max_risk_per_trade_pct: number
  max_concurrent_legs: number
  require_stop_loss: boolean
  signal_expiry_bars: number
}
```

`StrategyRiskPolicy` 是策略级风险约束，可视为全局 `RiskPolicy` 的子集或补充。最终批准仍由 Risk Layer 使用全局账户、持仓和策略上下文统一评估。

---

## 8. TradingState 设计

Strategy & Decision Control Plane 统一操作 `TradingState`，不要让每个 decision module / workflow node 创建彼此割裂的状态对象。

```ts
export interface TradingState {
  correlation_id: string

  market?: MarketSnapshot
  symbolMetadata?: SymbolMetadataSnapshot[]
  indicators?: IndicatorSnapshot

  rawSignal?: SignalRaw
  scoredSignal?: SignalScored

  strategyDecision?: StrategyDecision
  agentReview?: AgentReview

  tradeIntent?: TradeIntent
  softRiskResult?: RiskResult

  tradingCoreIntentResponse?: {
    intent_id: string
    status: "ACCEPTED" | "RISK_BLOCKED" | "REJECTED" | "DUPLICATE"
    reason?: string
    correlation_id: string
    received_at: number
  }

  tradingCoreExecutionSummary?: {
    plan_id?: string
    commandStates: ExecutionCommandState[]
    executionEvents: ExecutionEvent[]
    updated_at: number
  }

  accountSnapshot?: AccountSnapshot
  positionSnapshot?: PositionSnapshot[]
  orderSnapshot?: OrderSnapshot[]

  audit: AuditEntry[]
}
```

`TradingState` 可以缓存 Trading Core 返回的 execution summary，用于解释、审计和 workflow 恢复；但它不是 execution state source of truth，不能写回或覆盖 Trading Core State Store。

---

## 9. TradingState 持久化与 Saga 恢复

`TradingState` 不应只存在于单个编排 runtime 的内存中。每条交易链路以 `correlation_id` 作为 Saga ID，并在关键节点完成后持久化 checkpoint。

### Checkpoint 结构

```ts
export interface TradingStateCheckpoint {
  correlation_id: string
  current_node: string
  status: "RUNNING" | "COMPLETED" | "FAILED" | "EXPIRED"
  updated_at: number
  expires_at?: number
  state: TradingState
}
```

### Redis Key

```text
trading:saga:{correlation_id}
```

### TTL 与过期策略

Saga checkpoint 必须定义生命周期，避免 Redis 中长期堆积无法恢复或无审计价值的状态。

```text
COMPLETED / FAILED 的 Saga：
  TTL = 7 天
  用于短期审计、排查和链路回放

RUNNING 的 Saga：
  如果超过 1 小时未更新
  → 标记为 EXPIRED
  → 写入 system.event: SAGA_EXPIRED

EXPIRED 的 Saga：
  TTL = 24 小时
  用于排查后自动清理
```

过期判断基于：

```text
now - checkpoint.updated_at > 1 hour
```

Strategy & Decision Control Plane 启动和定时巡检时都应扫描超时的 RUNNING Saga。

### 推荐 Checkpoint 节点

```text
receiveMarket.done
computeIndicators.done
scoreSignal.done
signalFiltered.done
strategyDecision.done
agentReview.done
tradeIntentBuilt.done
tradeIntentSubmitted.done
tradeIntentAccepted.done
executionSummaryReceived.done
audit.done
```

### 恢复规则

```text
Strategy & Decision Control Plane 重启时扫描未完成 Saga
根据 current_node 找到最后一个有效 checkpoint
从最后一个有效 event 节点恢复
已完成节点不重复执行，或通过 idempotency_key 防重复
```

典型中断场景：

```text
TradeIntent 已提交 Trading Core
Trading Core 返回 ACCEPTED
但 Strategy & Decision Control Plane 尚未收到 execution.summary / terminal event
```

恢复后 Strategy & Decision Control Plane 不得重新生成 execution.command，也不得再次提交新的 intent_id。它必须用原 `intent_id / idempotency_key / decision_id / correlation_id` 查询 Trading Core `/state` 或 intent summary；如果 Trading Core 已记录该 intent，则按 Trading Core 返回的状态继续 workflow。如果 Trading Core 没有记录该 intent，才允许用原 `intent_id` 与原 `idempotency_key` 幂等重放 `POST /trade-intents`。

### Saga 恢复时的 Snapshot / Order / Symbol Metadata Freshness

Saga checkpoint 中的 `accountSnapshot` / `positionSnapshot` / `orderSnapshot` / `symbolMetadata` 可能已经过期。恢复时不得直接使用旧 snapshot、order snapshot 或 metadata 形成新的 TradeIntent。

恢复规则：

```text
如果 checkpoint 中的 accountSnapshot / positionSnapshot / orderSnapshot / symbolMetadata 超过 max_snapshot_age_ms
  → Strategy & Decision Control Plane 必须通过 Trading Core 请求刷新或等待 Trading Core 发布最新 summary
  → 使用最新 summary 重新评估 strategy.decision / TradeIntent
  → 如果刷新失败，则 Saga 标记 FAILED，并写 audit.event
```

典型场景：

```text
Saga 停在 tradeIntentBuilt.done
系统重启后恢复
如果 snapshot、order snapshot 或 symbol metadata 过期
  → 不得直接 submitTradeIntentToTradingCore
  → 必须先刷新 Trading Core state summary，并重新确认 TradeIntent 是否仍有效
```

---

## 10. Strategy 抽象设计

### 10.1 Strategy Definition

策略定义描述策略本身，不携带运行状态。

```ts
export interface StrategyDefinition {
  id: string
  type: "trend" | "hedge" | "arbitrage" | "options"

  legs: Leg[]
  executionPolicy: ExecutionPolicy
  riskPolicy: StrategyRiskPolicy
}
```

### 10.2 Strategy Runtime

策略运行时负责基于输入生成决策。

```ts
export interface StrategyRuntime {
  definition: StrategyDefinition

  evaluate(input: StrategyInput): Promise<StrategyDecision>
}
```

### 10.3 StrategyInput

```ts
export interface StrategyInput {
  market: MarketSnapshot
  symbolMetadata: SymbolMetadataSnapshot[]
  indicators: IndicatorSnapshot
  scoredSignal: SignalScored
  accountSnapshot: AccountSnapshot
  positionSnapshot: PositionSnapshot[]
  orderSnapshot: OrderSnapshot[]
}
```

`StrategyInput` 是策略运行时的核心入口。策略只能基于明确传入的快照和信号评分生成 `StrategyDecision`，不直接读取 Redis、Gateway、MT5 或全局状态。

### 10.4 Leg

```ts
export interface Leg {
  leg_id: string
  symbol: SymbolCode
  direction: "BUY" | "SELL"
  ratio: number
  dependency: string[]
}
```

### 10.5 ExecutionPolicy

```ts
export interface ExecutionPolicy {
  mode: "sequential" | "simultaneous" | "best_effort_atomic"
  failurePolicy: "cancel_all" | "partial_fill" | "retry"
  timeout: number
  max_command_ttl_ms: number
  rollbackPolicy?: RollbackPolicy
}
```

`best_effort_atomic` 不表示 broker 级真实原子提交。它表示 Execution Layer 尽量同时或按依赖提交多腿命令，并在部分失败或部分成交时按 `rollbackPolicy` 执行补偿。

`max_command_ttl_ms` 是 execution policy 对投递窗口的上限，用于缩短 `execution.command.expires_at`，不能延长 `signal_expires_at` 或 `risk_result.valid_until`。

---

## 11. Signal Early Exit 与过期信号处理

信号评分后必须经过条件路由，不满足阈值的信号不进入策略选择和风控。

### 推荐流程

```text
signal.raw
  → scoreSignal
  → signal.scored
  → IC / confidence threshold filter
      ├── pass → selectStrategies
      └── fail → audit.event + stop
```

### 推荐规则

```ts
if ((signal.ic_score ?? 0) < 0.05) {
  return {
    status: "SKIPPED",
    reason: "IC score below threshold",
    next: "audit",
  }
}
```

### 过期信号处理

对于趋势策略，超过 2 根 bar 的信号视为过期。

```text
H1 策略：超过 2 小时丢弃
H4 策略：超过 8 小时丢弃
```

过期信号处理结果：

```text
写 audit.event
必要时写 system.event: SIGNAL_BACKPRESSURE_WARNING
不形成 TradeIntent
不提交 Trading Core
```

未过期并进入策略选择的信号，Strategy Runtime 必须在 `StrategyDecision.signal_expires_at` 中写入信号失效时间。Risk Layer 可以拒绝过期信号或 intent，但不能延长 `signal_expires_at`。

---

## 12. Account / Position / Order / Symbol Metadata 更新策略

`account.snapshot`、`position.snapshot`、`order.snapshot` 和 `symbol.metadata` 由 Execution Client（例如 MT5 Adapter）负责发布。

### 更新时机

```text
1. 每次 execution.event 之后立即发布
2. 每 30 秒定时发布一次 heartbeat snapshot
3. 连接恢复后立即发布 account.snapshot / position.snapshot / order.snapshot / symbol.metadata
4. Risk Layer 需要时可通过 Trading Gateway 请求对应 Execution Client 按需刷新
```

### Risk Layer 使用规则

Risk Layer 必须使用最新有效 snapshot、order snapshot 与 symbol metadata。

```text
如果 snapshot、order snapshot 或 symbol metadata 过期，则不得批准 TradeIntent，也不得生成 execution.plan / execution.command
```

推荐有效期：

```text
max_snapshot_age_ms = 30000 或 60000
```

如果超过有效期：

```text
risk.rejected reason = ACCOUNT_SNAPSHOT_STALE / ORDER_SNAPSHOT_STALE / SYMBOL_METADATA_STALE
```

---

## 13. Agent Review 触发条件

`agentReview` 不应无条件触发。Agent 调用存在延迟和成本，只有在信号价值较高、信号模糊或系统检测到异常时才进入 Agent Review。

### 条件路由

```text
strategyDecision
  ├── confidence >= 0.8 且无异常
  │     → 跳过 agentReview
  │     → 形成 TradeIntent 并提交 Trading Core
  │
  └── confidence < 0.8 或存在异常
        → 进入 agentReview
        → 按 AgentReview 结果形成 / 放弃 / 降风险 TradeIntent
        → 提交 Trading Core
```

### 建议触发条件

```text
confidence < 0.8
signal.scored 与 strategy.decision 出现冲突
market regime = VOLATILE 或 UNKNOWN
snapshot 接近过期但未超过硬阈值
近期系统出现 STRATEGY_DEGRADED
外部研究 / 回测服务返回高风险提示
```

### 不触发条件

```text
confidence >= 0.8
signal.scored 清晰
market regime = TREND
无系统异常
无风险预警
```

Agent Review 的输出只能影响解释、置信度修正、skip / reduce / manual_review 建议，不能直接生成交易命令。

### 超时与 Fallback

`agentReview` 节点必须设置硬超时，避免 LLM 调用阻塞主流程。

推荐超时：

```text
agent_review_timeout_ms = 15000
```

超时 fallback：

```ts
const fallbackAgentReview: AgentReview = {
  review_id: "agent_timeout",
  score_adjustment: 0,
  recommendation: "none",
  reason: "AGENT_TIMEOUT",
}
```

处理规则：

```text
Agent 超时不阻塞主流程
写 system.event: AGENT_TIMEOUT
使用 fallbackAgentReview 继续形成 TradeIntent 或按策略规则跳过
Agent 返回格式错误时按超时等价处理
```

---

## 14. Agent / LLM 边界

Agent 可以接入，但权限必须固定。

### 推荐 Agent 拓扑

当前方案推荐少量、职责固定的 Agent 子图，不推荐 autonomous multi-agent debate 或 supervisor 自由派单来驱动 live trading。

```text
Deterministic Decision Workflow
  ├── StrategyReviewAgent
  │     → live path 可选节点
  │     → 输入 signal / indicators / strategy.decision / snapshots
  │     → 输出 AgentReview
  │
  ├── RiskExplanationAgent
  │     → 非交易决策节点
  │     → 输入 RiskResult / rejected reason / policy
  │     → 输出 audit summary / operator explanation
  │
  ├── ReconciliationAssistant
  │     → 仅用于 MANUAL_RECONCILIATION_REQUIRED
  │     → 输入 execution events / order snapshots / spool records
  │     → 输出 suggested reconciliation note
  │
  ├── IncidentAuditAgent
  │     → post-trade / incident path
  │     → 输入 audit.event / system.event / deadletter.event
  │     → 输出 incident report
  │
  └── ResearchAgent
        → async sidecar
        → 输入 historical data / backtest result / external research
        → 输出 external.research.result / external.strategy.candidate
```

live trading path 只允许 `StrategyReviewAgent` 参与，并且必须经过 `agentReviewGate` 条件触发。其他 Agent 不能阻塞或改变 live execution path。

Agent 权限矩阵：

| Agent                   | Live Path | 可写事件                  | 禁止                               |
| ----------------------- | --------: | ------------------------- | ---------------------------------- |
| StrategyReviewAgent     |      可选 | agent.review              | execution.command / risk.approved  |
| RiskExplanationAgent    |        否 | audit.event               | 修改 RiskResult                    |
| ReconciliationAssistant |        否 | audit.event / manual note | 修改 ExecutionEvent / 生成 command |
| IncidentAuditAgent      |        否 | audit.event / report      | 修改业务状态                       |
| ResearchAgent           |        否 | external.*                | 修改 live registry / command       |

所有 Agent 输出必须是结构化 JSON，并通过 schema validation；失败则写 `deadletter.event` 或按对应 fallback 处理。

### Agent 可以做

```text
分析市场 regime
总结信号理由
给 confidence adjustment
识别异常行情
建议 skip / reduce risk / manual review
生成审计说明
做研究和回测辅助
```

### Agent 不可以做

```text
直接下单
直接写 execution.command
绕过 Risk Engine
放大仓位
移除止损
覆盖硬风控
强制 BUY / SELL
```

### Agent 输出

```ts
export interface AgentReview {
  review_id: string
  score_adjustment: number
  recommendation: "none" | "skip" | "reduce_risk" | "manual_review"
  reason: string
}
```

Agent 输出不得直接交易；任何由 Agent 影响的 TradeIntent 必须再经过 Risk Layer。

### Manual Review / Manual Reconciliation 边界

人工处理只能解除阻塞、确认外部事实或终止流程，不能绕过 Risk Layer 直接下单。

允许人工操作：

```text
标记 signal / strategy.decision 为 rejected
确认 manual_review 后允许继续形成 / 提交 TradeIntent
为 MANUAL_RECONCILIATION_REQUIRED 填写 broker 侧核对结论
请求 Trading Core 根据 broker 证据恢复 command / plan 投影状态；人工流程不能直接写 execution state
请求刷新 account / position / order snapshot
```

禁止人工操作：

```text
直接生成 execution.command
绕过 RiskResult 批准交易
修改 ExecutionEvent 事实记录
删除 Redis / spool 中的执行事实
在 reconciliation 前后仅凭 snapshot / result 强制 retry command
```

人工处理结果必须写 `audit.event`，并保留 operator、reason、evidence、timestamp。

---

## 15. Risk Policy 设计

```ts
export interface RiskPolicy {
  position_sizing_version: string
  max_risk_per_trade_pct: number
  max_daily_loss_pct: number
  max_drawdown_pct: number

  max_symbol_exposure_pct: number
  max_total_exposure_pct: number
  max_margin_usage_pct: number

  require_stop_loss: boolean
  reject_expired_signal: boolean
  max_approval_ttl_ms: number
  max_snapshot_age_ms: number
  max_order_snapshot_age_ms: number
  max_market_snapshot_age_ms: number
  max_symbol_metadata_age_ms: number
  max_capacity_age_ms: number

  max_concurrent_positions: number
  require_valid_symbol_metadata: boolean
  reject_trade_mode_disabled: boolean
}
```

`max_approval_ttl_ms` 是 Risk Layer 对 `risk.approved` 的最长有效期。`RiskResult.valid_until` 必须不晚于 `evaluated_at + max_approval_ttl_ms`、signal expiry，以及本次评估依赖的 account / position full-set / order full-set / pending-command reconciliation / risk capacity / market / symbol metadata freshness 边界。

`position_sizing_version` 标识确定性换算算法和参数版本，必须原样写入 `RiskResult.sizing_version` 和审计 payload。第一版唯一支持值为 `fixed-risk-at-stop.v1`；未知值必须返回 `RISK_INPUT_INVALID`，不得继续运行 v1 算法却标记成其他版本。未来新增版本必须显式 dispatch 到对应实现。同一版本不得在不修改版本号的情况下改变取整、价值换算或成本 buffer 规则。

### 初始硬规则

```text
单笔风险 ≤ 账户 1%
日最大亏损 ≤ 账户 3%
最大回撤熔断 ≤ 账户 10%
禁止无 SL 开新仓；CLOSE / CANCEL / 降风险 MODIFY 不受该规则阻塞
禁止过期信号下单
禁止重复 execution.command
禁止使用过期 account / position snapshot 批准交易
禁止使用过期 order snapshot 批准交易
禁止以逐行 latest observation 或空数组替代账户级 position / order full-set 水位
禁止使用跨账户或过期 market snapshot 批准交易
禁止使用过期 symbol metadata 批准交易
禁止缺失或使用非法 tick_value_loss 做 position sizing
禁止缺失或使用非法 margin_initial 证明新保证金上界
禁止违反 volume_min / volume_max / volume_step / stops_level / trade_mode
禁止向上取整到 volume_min，或产生 actual_risk > risk_budget 的 lots
```

---

## 16. Backpressure 设计

Redis Streams 的积压必须可观测，并影响信号处理路径。

### 监控指标

```text
stream length
consumer group lag
oldest pending message age
compute service latency
orchestrator processing latency
```

### 处理策略

当积压超过阈值时：

```text
Option A：Strategy & Decision Control Plane 暂停消费，等待 Compute Service 恢复
Option B：丢弃过期 signal
Option C：发 system.event 告警，人工介入
```

趋势策略默认采用 Option B：

```text
signal 超过 2 根 bar 的时间窗口就直接丢弃
```

### 处理结果

```text
写 audit.event
写 system.event: SIGNAL_BACKPRESSURE_WARNING
不形成 TradeIntent
```

### Execution Path Metrics / SLO

执行链路必须单独监控，不能只看 Redis lag。

推荐指标：

```text
gateway_active_sessions
execution_client_heartbeat_lag_ms
time_sync_rtt_ms
time_sync_offset_ms
time_sync_status
wire_decode_error_count
schema_deadletter_count
hmac_failure_count
command_dispatch_latency_ms
command_received_latency_ms
command_delivery_timeout_count
delivery_unconfirmed_count
reconciliation_duration_ms
reconciliation_failed_count
spool_depth
spool_oldest_unflushed_age_ms
spool_flush_failed_count
snapshot_stale_count
order_snapshot_stale_count
symbol_metadata_stale_count
clock_skew_detected_count
time_sync_unhealthy_count
```

推荐默认阈值：

```text
command_received_latency_ms p95 > 1000
  → WARNING

delivery_ack_timeout_ms = 5000
  → 超时进入 DELIVERY_UNCONFIRMED

reconciliation_duration_ms > 10000
  → WARNING

spool_oldest_unflushed_age_ms > 30000
  → ERROR

spool_flush_failed_count > 0
  → ERROR

hmac_failure_count > 0
  → WARNING；短时间连续出现则 CRITICAL

time_sync_rtt_ms > max_time_sync_rtt_ms
  → 丢弃该 sample

time_sync_status != SYNCED
  → TIME_SYNC_UNHEALTHY；拒绝新 execution.command

time_sync_status 从 DEGRADED / UNSYNCED 恢复到 SYNCED
  → TIME_SYNC_RESTORED

abs(time_sync_offset_ms previous - current) > max_clock_offset_ms
  → CLOCK_SKEW_DETECTED
```

这些指标用于 system.event 和 dashboard，不直接替代 Risk Layer 判断。

### State Store / Redis 不可用时的降级行为

Trading Core State Store 是执行状态强一致存储。Redis 是跨服务 fanout，不是执行状态唯一来源。

```text
Trading Core State Store 不可用：
  → Trading Core 拒绝新的 trade.intent，返回 STATE_STORE_UNAVAILABLE
  → 新 intent 不得进入 accepted 状态，也不得写入 idempotency journal
  → Trading Core 不生成新的 execution.command
  → 已 accepted 但尚未生成 command 的 intent 冻结，恢复后必须重新校验时间、snapshot freshness 和 Risk
  → 已在途 broker 回报写本地 emergency append-only spool
  → 触发 system.event: STATE_STORE_UNAVAILABLE；若主 store 无法写入，则先写 emergency spool
  → 进入 MANUAL_RECONCILIATION_REQUIRED 或只读恢复模式

Redis 不可用：
  → Trading Core 继续维护本地 execution state
  → Trading Core 继续处理已接受 intent 和已在途订单
  → Redis fanout 写入 Trading Core local spool
  → Strategy & Decision Control Plane 暂停依赖 Redis 的新 workflow
  → UI / audit 可能延迟，但不得反向影响 Trading Core 已批准执行
  → 触发 system.event: REDIS_UNAVAILABLE（恢复后补写）
```

在 Redis 恢复前：

```text
Execution Client 可以继续保持 TCP 连接
Trading Gateway 可以继续维护 session
Trading Gateway 必须继续接收已在途订单的 execution.event
Trading Core 必须将 execution.event / command.received 写入 Trading Core State Store
Trading Core 将 Redis fanout 记录写入本地 append-only spool
Strategy & Decision Control Plane 不依赖 Redis lag 来判断执行事实
```

Redis 恢复后，Trading Core 必须先 flush fanout spool；Strategy & Decision Control Plane 再恢复消费，并以 Trading Core `/state` 或 execution summary 校准 workflow state。

Trading Core State Store 恢复后：

```text
1. 先停止接收新的 trade.intent。
2. replay emergency append-only spool 到 Trading Core State Store。
3. 对所有 in-flight command / accepted intent 执行 reconciliation。
4. 对已 accepted 但尚未生成 command 的 intent 重新校验 requested_at / signal_expires_at / snapshot freshness / Risk。
5. 写 system.event: STATE_STORE_RESTORED。
6. 只有 replay 与 reconciliation 完成后才恢复接收新的 trade.intent。
```

spool owner：

```text
Trading Core owns:
  command.received
  execution.event
  account.snapshot
  position.snapshot
  order.snapshot
  symbol.metadata
  gateway-originated system.event

Strategy & Decision Control Plane owns:
  workflow checkpoint fallback
  audit.event fallback
  decision-control-plane-originated system.event
```

Trading Core 和 Strategy & Decision Control Plane 不得 flush 对方拥有的 spool。补写 Redis 时必须保留原始 `message_id` / `event_id`，并按 `message_id` / `event_id` 幂等去重。

本地 spool 规则：

```text
append-only
fsync or flush policy 明确配置
每条记录包含 message_id / event_id / type / payload / received_at
received_at 必须是 server-time domain
补写 Redis 成功后标记 flushed
重复补写必须按 message_id / event_id 幂等去重
spool 损坏或缺口必须触发 MANUAL_RECONCILIATION_REQUIRED
```

推荐 spool record：

```ts
export interface LocalSpoolRecord<T> {
  spool_id: string
  message_id?: string
  event_id?: string
  type: string
  payload: T
  received_at: number
  flushed_at?: number
  flush_attempts: number
  last_error?: string
}
```

---

## 17. system.event 设计

`system.event` 是系统可观测性基础。

### 类型

```ts
export type SystemEventType =
  | "DECISION_CONTROL_PLANE_STARTED"
  | "DECISION_CONTROL_PLANE_STOPPED"
  | "GATEWAY_STARTED"
  | "GATEWAY_STOPPED"
  | "COMPUTE_SERVICE_UNHEALTHY"
  | "COMPUTE_SERVICE_RESTORED"
  | "EXECUTION_CLIENT_CONNECTION_LOST"
  | "EXECUTION_CLIENT_CONNECTION_RESTORED"
  | "SIGNAL_BACKPRESSURE_WARNING"
  | "REDIS_STREAM_LAG_WARNING"
  | "REDIS_UNAVAILABLE"
  | "REDIS_RESTORED"
  | "STATE_STORE_UNAVAILABLE"
  | "STATE_STORE_RESTORED"
  | "RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED"
  | "STRATEGY_DEGRADED"
  | "SAGA_EXPIRED"
  | "COMMAND_DELIVERY_TIMEOUT"
  | "COMMAND_DELIVERY_UNCONFIRMED"
  | "COMMAND_DELIVERY_FAILED"
  | "COMMAND_DISPATCH_BACKPRESSURE"
  | "COMMAND_EXPIRED"
  | "MANUAL_RECONCILIATION_REQUIRED"
  | "AGENT_TIMEOUT"
  | "AUTHENTICATION_FAILED"
  | "SESSION_IDENTITY_MISMATCH"
  | "CLOCK_SKEW_DETECTED"
  | "TIME_SYNC_UNHEALTHY"
  | "TIME_SYNC_RESTORED"
  | "WIRE_FRAME_TOO_LARGE"
  | "WIRE_PROTOCOL_VIOLATION"
  | "LONG_TERM_AUDIT_WRITE_FAILED"
  | "SECRET_ROTATION_STARTED"
  | "SECRET_ROTATION_COMPLETED"
  | "SECRET_ROTATION_FAILED"
  | "DEADLETTER_CREATED"
```

### Payload

```ts
export interface SystemEvent {
  type: SystemEventType
  severity: "INFO" | "WARNING" | "ERROR" | "CRITICAL"
  component: string
  message: string
  metadata?: Record<string, unknown>
  timestamp: number // server_now_ms
}
```

---

## 18. 目标数据流

```text
MT5 Adapter
  → Trading Core
  → market.tick / market.bar / account.snapshot / position.snapshot / order.snapshot / symbol.metadata
  → Trading Core State Store
  → optional Redis / WS fanout

MarketEventGraph
  → consume market.bar / market.snapshot from Trading Core
  → computeIndicators
  → scoreSignal

StrategyDecisionGraph
  → filterSignal
      ├── fail → audit.event + stop
      └── pass → selectStrategies
  → strategy.decision
  → trade.intent

AgentReviewSubgraph
  → agentReview 条件路由
      ├── skip → trade.intent
      └── review → agent.review → trade.intent

Trading Core
  → receive trade.intent
  → idempotency check
  → load latest account / position / order / symbol / market state
  → derive deterministic position sizing candidates
  → hard risk gate
      ├── rejected → risk.rejected / event fanout
      └── approved → risk.approved with final RiskResult.adjusted_legs lots

Execution Engine
  → exact-map approved lots by leg_id; no sizing recalculation
  → buildExecutionPlan
  → execution.plan
  → execution.command
  → Trading Gateway / adapter delivery request
      ├── rejected before socket write → EXPIRED / DELIVERY_FAILED / COMMAND_DISPATCH_BACKPRESSURE
      └── accepted for socket write → execution.command.state(DISPATCHED)
  → Execution Client
  → command.received
      └── timeout / disconnect → DELIVERY_UNCONFIRMED → reconciliation

Execution Projection
  → execution.command.state(COMMAND_RECEIVED)
  → execution.event
  → execution.command.state / execution.plan projection
  → account.snapshot / position.snapshot / order.snapshot / symbol.metadata
  → audit.event
  → WS / Redis fanout to Strategy & Decision Control Plane / UI

Reconciliation Engine
  → reconciliation.request
  → reconciliation.result
  → order.snapshot / position.snapshot / account.snapshot / symbol.metadata
  → evaluate Completed / PendingEvidence / ManualRequired
  → only typed dispatch / delivery / reconciliation evidence, command.received, ExecutionEvent, or explicit time/manual evidence may drive Execution state machine
```

---

## 19. 典型趋势策略逻辑

### 19.1 市场状态判断

```text
BBW 扩张 → 趋势模式
BBW 收缩 → 震荡模式
```

趋势模式：

```text
PAR 方向
EMA 排列
ADX 强度
```

震荡模式：

```text
RSI 均值回归
PAR 暂停
```

### 19.2 进场条件

```text
BBW 扩张确认
EMA 21 / 55 / 200 多头或空头排列
ADX > 25
PAR 翻转方向一致
```

### 19.3 出场条件

```text
止损：PAR 反转 或 ATR × 2
止盈：分批出场
  50% 仓位 → 盈亏比 1:2 出场
  50% 仓位 → PAR 跟踪止损
强制出场：BBW 快速收缩
```

### 19.4 起始参数

| 指标 | XAUUSD H4          | BTCUSD H4          |
| ---- | ------------------ | ------------------ |
| PAR  | step=0.02, max=0.2 | step=0.01, max=0.1 |
| RSI  | period=14, 70/30   | period=21, 80/20   |
| BB   | period=20, std=2.0 | period=20, std=2.5 |
| EMA  | 21/55/200          | 21/55/200          |
| ADX  | >25                | >25                |

---

## 20. 外部研究 / 回测服务接入边界

外部研究 / 回测服务可以作为后期接入的研究、回测和策略候选 sidecar。

### 建议定位

```text
research-agent
strategy-lab
backtest-service
shadow-account-service
strategy-candidate-generator
```

### 可接入层

| 层                                | 角色                                            |
| --------------------------------- | ----------------------------------------------- |
| Event Backbone                    | 读写 external.research / external.backtest 事件 |
| Compute Layer                     | 使用其分析和回测能力                            |
| Strategy & Decision Control Plane | 作为 research / backtest tool                   |
| Agent Layer                       | 提供策略研究、报告和辅助审查                    |

### 不应承担

```text
实时 execution engine
最终 risk-engine
Execution Client adapter
execution.command 生成者
```

### 推荐 Streams

```text
external.research.request
external.research.result

external.backtest.request
external.backtest.result

external.strategy.candidate
external.strategy.export
external.strategy.rejected

external.shadow.report
```

### 接入原则

```text
外部服务可以影响 strategy.decision 的解释和置信度
外部服务可以产生 backtest.result / strategy.candidate
外部服务可以产生 agent.review 的研究输入
外部服务不能绕过 risk-engine
外部服务不能直接写 execution.command
```

### Strategy Candidate Promotion Gate

`external.strategy.candidate` 不能直接进入 live strategy registry。候选策略必须经过 promotion gate。

推荐流程：

```text
external.strategy.candidate
  → offline backtest passed
  → walk-forward / out-of-sample validation passed
  → paper trading observation passed
  → risk policy review passed
  → manual approval
  → versioned strategy config
  → strategy.registry activation
```

上线约束：

```text
每个策略必须有 strategy_id / version / config_hash
策略配置变更必须写 audit.event
live activation 必须人工批准
external service 不得直接修改 live registry
promotion 失败必须写 external.strategy.rejected 或 audit.event
```

---

## 21. 服务边界总结

### mt5-adapter

```text
MQL5 EA
TCP execution client
market publisher
symbol metadata publisher
execution command receiver
execution event reporter
account / position / order snapshot publisher
time sync client
local terminal guard
```

### Trading Core (`sinan-core`)

```text
Rust implementation
WS / HTTP API for UI and Strategy & Decision Control Plane
Execution Client Protocol server
Native TCP / Execution WebSocket bindings
TransportAdapter abstraction
GatewayInboundRouter / GatewayOutboundRouter
Execution Client session registry
TradeIntent receiver
hard risk gate
execution plan / command builder
command lifecycle owner
execution event projection
idempotency journal
reconciliation owner
command / broker adapter routing
heartbeat detection
time sync authority
transport framing / message boundary enforcement
connection health
SQLite / append-only State Store
local append-only event spool / audit writer
```

### strategy-decision-control-plane

```text
TypeScript / Bun implementation
LangGraph optional
Cursor SDK optional
Agent SDK optional
Rule engine optional
workflow state owner
decision workflow checkpoint owner
workflow owner
strategy routing
slow decision generation
TradeIntent producer
Trading Core HTTP / WS client
execution summary consumer
event cursor / resume consumer
audit
external service integration
```

### compute-service

```text
Python / FastAPI implementation
technical indicators
statistical models
advisory / research position sizing math
hedge ratio
basis
regime detection
```

### risk-engine

```text
Trading Core 内部 domain module
portfolio risk
account risk
strategy risk
snapshot freshness validation
symbol metadata validation
broker trading constraint validation
local deterministic final lots calculation
hard execution approval
```

### external-research-service

```text
external sidecar
backtest
strategy candidate
shadow account
research report
agent support
```

### storage / audit

```text
SQLite / append-only log = authoritative execution state store
Redis Streams = operational event fanout / replay aid
Decision workflow checkpoint store = workflow checkpoints, not execution facts
SQLite / Postgres / ClickHouse = long-term audit, research, analysis
```

### Long-term Audit Sink

Trading Core State Store / Redis Streams 都不是长期审计唯一存储。以下事件必须异步落入长期审计库：

```text
execution.command
command.received
execution.event
account.snapshot
position.snapshot
order.snapshot
symbol.metadata
reconciliation.request
reconciliation.result
audit.event
deadletter.event
manual review / manual reconciliation action
spool flush result
strategy activation / deactivation
```

长期审计规则：

```text
保留原始 event_id / message_id / correlation_id / causation_id
保留原始 payload 和 schema_version
写入失败不得阻塞已批准的执行流程，但必须写 system.event
审计库不可反向驱动交易流程
执行相关审计建议至少保留 1 年
```

---

## 22. 设计决策结论

最终目标架构定为：

```text
MT5 Adapter
↔ Trading Core
  使用 Execution Client Protocol
  Native TCP / Execution WebSocket transport binding
  WS / HTTP API + hard risk + execution engine + state store
↔ Strategy & Decision Control Plane
↔ Compute & Research Services
↔ Redis Streams optional fanout
↔ Audit / Replay / Research
```

核心设计边界：

```text
MQL5 只做 MT5 Adapter
MT5 Adapter 是 Execution Client Protocol 的一种实现
Trading Core 是交易正确性边界
Trading Gateway 是 TCP / WS / HTTP gateway，不属于 Strategy & Decision Control Plane
Execution Client Protocol 承担执行链路的低延迟双向通信
Native TCP 与 Execution WebSocket 都可以作为 Execution Client Protocol transport binding
Event WebSocket 可用于 dashboard / browser / decision event stream，不作为执行客户端 command 链路
SQLite / append-only log 是执行状态强一致存储
Redis Streams 做跨服务 fanout / replay aid，不是执行事实唯一来源
Compute & Research Services 做无状态研究 / 决策建议性计算，不生成最终 lots
Strategy & Decision Control Plane 做慢决策、AI 编排、研究和人工流程
Risk Engine 当前作为 Trading Core 内部 domain module
Execution Event 是执行事实来源，ExecutionCommandState / ExecutionLeg.status / ExecutionPlan.status 是事实流投影
Agent / 外部研究服务只做研究、分析、审查、候选生成
```

关键原则：

```text
strategy.decision 不是交易命令
trade.intent 不是执行命令
risk.approved 不是最终事实
execution.command 是执行请求
execution.event 是执行事实
audit.event 是追踪与复盘基础
Trading Gateway / Trading Core server time 是协议权威时间源
所有控制时间必须使用 server-time domain，客户端本地 wall clock 不参与交易判断
Execution Client 必须通过 time.sync 维护 effective_server_now_ms
Trading Core State Store 必须持久化 execution state / idempotency journal / reconciliation checkpoint
Decision workflow state 必须按 correlation_id 做 checkpoint，但不能覆盖 Trading Core execution state
Saga 必须定义 TTL 与 EXPIRED 处理
Risk Layer 必须使用最新有效 account / position / order snapshot 和 symbol metadata
StrategyDecision 必须记录 signal_expires_at
RiskResult 必须记录 snapshot_age_ms / symbol_metadata_age_ms / sizing_version / risk budget / adjusted_legs / evaluated_at / valid_until
Risk Layer 本地、确定性地拥有最终 lots 批准；live hard-risk path 不依赖 Compute Service
Execution Layer 必须原样映射 RiskResult.adjusted_legs lots，不得重算、round、clamp 或放大
ExecutionCommand.expires_at 是派生字段，不是 Execution Layer 拥有的业务有效期
Execution Layer 拥有拒绝执行权，不得延长 signal_expires_at 或 risk_result.valid_until
ExecutionEvent 必须记录 server-time domain 的 event_at，并在成交时记录 server-time domain 的 filled_at
ExecutionEvent 必须携带 account / client / plan / leg / broker order 等恢复上下文
OrderSnapshot 是 broker 订单状态观测，用于 reconciliation，不是执行事实来源
过期 signal 不形成 TradeIntent
agentReview 必须通过条件路由触发，不无条件执行
agentReview 必须有超时 fallback
execution.command 必须有 command.received ACK 回路
未收到 command.received 时必须进入 DELIVERY_UNCONFIRMED / RECONCILING，不得盲目重试
服务间调用必须具备最低限度认证
Trading Gateway 必须绑定 session_id / client_id / account_id / terminal_id，不允许 payload 覆盖认证身份
已过期 execution.command 不得进入 command inbox，不得发送 broker order
max_inflight_commands 超限时不得推进到 DISPATCHED
Saga 恢复后必须重新校验 snapshot / order snapshot / symbol metadata freshness
PARTIALLY_FILLED 的多腿计划必须按 RollbackPolicy 恢复
MANUAL_RECONCILIATION_REQUIRED 是自动流程阻塞状态，不是执行事实来源
CANCEL 是显式 execution.command action，不能塞进 MODIFY 语义
best_effort_atomic 只是补偿式多腿执行，不代表 broker 级真实原子提交
execution.command 必须包含 account / terminal / broker / order policy 等实盘执行字段
execution.command 必须具备 HMAC 完整性保护，签名使用固定 key=value 字符串，不使用 JSON.stringify canonical
hello 之后的 Execution Client WireMessage 必须包含 message_id / session_id / schema_version / sequence
每种 transport binding 必须约束 max_message_bytes；Native TCP frame 必须受 max_frame_bytes 约束，非法 frame、无法解析或未通过最小 envelope 校验的 message 不得 ack
sequence 只在单个 Execution Client session 内有效，不是全局事实序号
Execution Client 重连恢复必须依赖 command journal / idempotency_key / execution.event
MQL5 验签必须使用 UTF-8 RFC3986 编码、固定数字格式和标准 HMAC-SHA256
Trading Core State Store 不可用时不得推进新交易流程
Redis 不可用时 Trading Core 可以继续维护本地 execution state，但必须 spool Redis fanout
Trading Core owns inbound execution spool，Strategy & Decision Control Plane owns workflow / audit spool
external.strategy.candidate 必须经过 promotion gate 才能进入 live strategy registry
所有跨服务时间必须使用 Unix milliseconds UTC server-time domain，并检测 clock skew / time sync health
schema 不兼容或无法解析的消息必须进入 deadletter.event
执行相关事件必须异步进入长期审计 sink
secret rotation 必须支持 ACTIVE / NEXT / RETIRED / REVOKED
market.tick 用于 MarketSnapshot 实时更新，market.bar 用于信号主流程
Execution Client 必须负责 connect / send / receive / heartbeat / reconnect / framing / ack 的基础可靠通信
MQL5 Execution Client 每个 EA 实例使用串行事件回调且无后台 worker；OnTimer 是 bounded network pump 的唯一活性 owner，OnTick 只采集并合并行情，OnTradeTransaction 先 journal broker 状态变化再入队上报
```

系统以事件为中心，以 Trading Core 为正确性边界，以 Execution Client 为 broker 执行端，以 Strategy & Decision Control Plane 为慢决策、AI 编排和状态工作流中心。

---

## 23. 实现规格拆解

实现按以下依赖顺序拆：

```text
Execution Client message protocol
  → 定义 Execution Client 与 Trading Core 能说什么
  → 独立于 TCP / WebSocket 等 transport binding

SQLite State Store
  → 定义哪些消息必须变成事实事件、哪些只是 projection

Rust crate boundary
  → 按消息处理职责和 State Store 边界拆模块

HTTP API schema
  → 只暴露 State Store projection，不暴露内部 transport 细节
```

### 23.1 Execution Client Message Protocol Registry

Execution Client message protocol 只服务 Execution Client 与 Trading Core 的双向执行链路。它独立于 transport；Native TCP 和 Execution WebSocket 都可以承载同一套 `WireMessage`。

Transport binding 规则：

```text
Native TCP
  → 4-byte unsigned big-endian length prefix
  → UTF-8 JSON WireMessage payload

Execution WebSocket
  → one WebSocket message = one UTF-8 JSON WireMessage payload
  → 不使用 TCP length prefix
  → 必须限制 max_message_bytes

HTTP
  → 不作为 Execution Client full-duplex command transport

Event WebSocket
  → 不作为 Execution Client command transport
```

所有 `type` 必须进入 registry。未知 `type` 一律写 `deadletter.event`，不得进入业务流程。

#### WireMessage Envelope

```ts
export interface WireMessage<T> {
  message_id: string
  type: ExecutionClientMessageType
  schema_version: string

  client_id?: string
  session_id?: string
  correlation_id?: string
  causation_id?: string

  sent_at?: number
  sequence?: number

  payload: T
}
```

#### Message Type 命名规则

```text
连接控制：session.*
时间同步：time.*
连接健康：heartbeat
传输确认：transport.*
市场数据：market.*
账户状态：account.* / position.* / order.* / symbol.*
执行请求：execution.command
执行回报：command.received / execution.event
对账：reconciliation.*
协议错误：protocol.error
```

固定类型集合：

```ts
export type ExecutionClientMessageType =
  | "session.hello"
  | "session.accepted"
  | "session.rejected"
  | "time.sync.request"
  | "time.sync.response"
  | "heartbeat"
  | "transport.ack"
  | "market.tick"
  | "market.bar"
  | "symbol.metadata"
  | "account.snapshot"
  | "position.snapshot"
  | "order.snapshot"
  | "execution.command"
  | "command.received"
  | "execution.event"
  | "reconciliation.request"
  | "reconciliation.result"
  | "protocol.error"
```

#### Schema Version Format

`schema_version` 使用固定格式：

```text
ecp.v<major>.<minor>
```

规则：

```text
示例：ecp.v1.0
major 不同表示不兼容，必须拒绝并写 deadletter.event(reason=SCHEMA_MAJOR_MISMATCH)。
minor 增加表示向后兼容，只允许新增 optional 字段。
字段删除、必填字段新增、枚举值语义改变必须提升 major。
同一 Trading Core 进程必须声明 supported_schema_versions。
收到高于自身支持 minor 的消息时，可以按已知字段处理，但必须忽略未知 optional 字段。
```

#### Message Registry

| type | direction | durable | ack | owner | purpose |
|---|---|---:|---|---|---|
| `session.hello` | client → core | no | `session.accepted/rejected` | gateway-session | 建立连接、身份、capabilities、resume cursor |
| `session.accepted` | core → client | no | transport ack | gateway-session | 返回 session_id、协议参数、server_time |
| `session.rejected` | core → client | no | none | gateway-session | 拒绝连接，随后关闭 socket |
| `time.sync.request` | client → core | no | `time.sync.response` | gateway-time | 采样 server time |
| `time.sync.response` | core → client | no | transport ack | gateway-time | 返回 server_receive_at / server_send_at |
| `heartbeat` | client → core | no | transport ack | gateway-session | 连接健康、clock_sync_status、queue depth |
| `transport.ack` | both | no | none | gateway-transport | 回报 wire message 的协议接纳状态，不表示业务处理完成 |
| `market.tick` | client → core | latest-only | transport ack | market-ingest | 更新实时 market snapshot，不逐条长期持久化 |
| `market.bar` | client → core | yes | transport ack | market-ingest | 已关闭 K 线，按 symbol/timeframe/timestamp 幂等 |
| `symbol.metadata` | client → core | latest-state | transport ack | state-ingest | broker symbol 交易约束 |
| `account.snapshot` | client → core | latest-state | transport ack | state-ingest | 账户权益 / 保证金快照 |
| `position.snapshot` | client → core | latest-state | transport ack | state-ingest | 持仓快照 |
| `order.snapshot` | client → core | latest-state | transport ack | state-ingest | broker 订单状态观测 |
| `execution.command` | core → client | yes | `command.received` | execution-engine | 下单 / 撤单 / 改单 / 平仓请求 |
| `command.received` | client → core | yes | transport ack | execution-engine | 客户端已持久化 command journal |
| `execution.event` | client → core | yes | transport ack | execution-projection | broker 执行事实 |
| `reconciliation.request` | core → client | yes | transport ack | reconciliation | 请求客户端回传 broker 当前状态 |
| `reconciliation.result` | client → core | yes | transport ack | reconciliation | 对账结果与 snapshot 汇总 |
| `protocol.error` | core → client | no | none | gateway-transport | 可解析 envelope 后的协议错误提示 |

#### Payload Schema 边界

```text
session.hello
  → HelloPayload
  → 不要求 session_id
  → 不要求 sent_at

session.accepted
  → HelloAcceptedPayload
  → 返回 session_id / heartbeat_interval_ms / max_frame_bytes / max_message_bytes / time sync policy

session.rejected
  → SessionRejected
  → 返回拒绝原因，发送后关闭 transport session

time.sync.request
  → TimeSyncRequest
  → 不要求 sent_at

time.sync.response
  → TimeSyncResponse
  → 必须使用 Trading Core server-time domain

heartbeat
  → HeartbeatPayload
  → 必须包含 effective_server_now / clock_sync_status

transport.ack
  → TransportAck
  → ACCEPTED: envelope / schema / identity 已通过，且消息已进入 crash-recoverable、幂等的 durable handler path
  → DUPLICATE: 相同 message identity 与 payload 已被 durable handler path 接纳或处理，不重复执行
  → REJECTED: 可关联消息的 typed rejection 与稳定 reason 已持久化为可幂等重放的 durable decision，reason 必填

market.tick
  → MarketTick
  → latest-only，不作为 strategy 主信号事实

market.bar
  → MarketBar
  → 已关闭 bar，主信号输入

symbol.metadata
  → SymbolMetadataSnapshot

account.snapshot
  → AccountSnapshot

position.snapshot
  → PositionSnapshot

order.snapshot
  → OrderSnapshot

execution.command
  → ExecutionCommand
  → 必须 HMAC
  → 必须 expires_at

command.received
  → CommandReceived
  → 表示 Execution Client 已写入本地 command journal

execution.event
  → ExecutionEvent
  → 执行事实来源

reconciliation.request
  → ReconciliationRequest

reconciliation.result
  → ReconciliationResult

protocol.error
  → ProtocolError
  → 仅用于可解析 envelope 后的协议错误提示
```

#### 新增 Payload 类型

```ts
export interface SessionRejected {
  reason: ErrorCode | "AUTHENTICATION_FAILED" | "SESSION_IDENTITY_MISMATCH"
  message?: string
  server_time: number
}

export interface TransportAck {
  acked_message_id: string
  acked_message_type: ExecutionClientMessageType
  status: "ACCEPTED" | "DUPLICATE" | "REJECTED"
  reason?: ErrorCode | "OK"
  received_at: number
}

export interface ProtocolError {
  related_message_id?: string
  related_message_type?: ExecutionClientMessageType
  reason: ErrorCode | "WIRE_PROTOCOL_VIOLATION" | "WIRE_FRAME_TOO_LARGE" | "DECODE_FAILED"
  message?: string
  server_time: number
}

export interface MarketTick {
  account_id: string
  symbol: SymbolCode
  broker_symbol?: string
  bid: number
  ask: number
  last?: number
  volume?: number
  observed_at: number
}

export interface CommandReceived {
  command_id: string
  idempotency_key: string
  account_id: string
  terminal_id?: string
  client_id?: string
  received_at: number
  inbox_status: "RECORDED" | "DUPLICATE" | "EXPIRED" | "REJECTED"
  reason?: ErrorCode | "OK"
}

export interface ReconciliationRequest {
  request_id: string
  account_id: string
  terminal_id?: string
  client_id?: string
  reason:
    | "DELIVERY_UNCONFIRMED"
    | "CONNECTION_RESTORED"
    | "MANUAL_REQUEST"
    | "STATE_STORE_RESTORED"
  command_ids?: string[]
  since_server_time?: number
}

export interface ReconciliationResult {
  request_id: string
  account_id: string
  terminal_id?: string
  client_id?: string
  observed_at: number
  account?: AccountSnapshot
  positions: PositionSnapshot[]
  orders: OrderSnapshot[]
  symbol_metadata: SymbolMetadataSnapshot[]
  unresolved_command_ids: string[]
}
```

这是 `transport.ack` payload 的权威定义，文档其他章节的 ACK 示例必须与此保持一致。

#### ACK 语义

```text
transport.ack
  → status=ACCEPTED 表示 WireMessage envelope / schema / identity 已通过，且消息已进入 crash-recoverable、幂等的 durable handler path
  → status=DUPLICATE 表示同一 message identity 和 payload 已被 durable handler path 处理或接纳，不重复执行
  → status=REJECTED 表示可关联消息的 typed rejection 与稳定 reason 已持久化为可幂等重放的 durable handler decision，reason 必填
  → 不表示 command 已执行
  → 不表示事件已改变 execution state

command.received
  → execution.command 的业务 ACK
  → 表示 Execution Client 已持久化 command_id / idempotency_key
  → 是 command lifecycle 从 DISPATCHED 推进的必要条件

execution.event
  → broker 执行事实
  → 是 ACCEPTED / ORDER_SENT / FILLED / REJECTED / FAILED 等状态投影来源
```

#### HMAC Signing String

`execution.command` 的 HMAC 签名字符串必须固定字段顺序，不能使用 JSON stringify / canonical JSON。本节的字段顺序与 golden vector 是所有语言实现的权威规格；文档其他章节不得定义不同顺序。

签名 secret：

```text
command signing secret
  → UTF-8 bytes
  → HMAC-SHA256
  → lowercase hex digest
```

固定字段顺序：

```text
command_id
plan_id
leg_id
strategy_id
account_id
terminal_id
client_id
symbol
broker_symbol
action
order_type
lots
price
sl
tp
deviation_points
magic
comment
position_ticket
broker_order_id
filling_policy
time_policy
expiration_time
expires_at
idempotency_key
```

生成规则：

```text
1. hmac 字段不参与签名。
2. 缺失 optional 字段写为空字符串。
3. 字段格式为 key=value。
4. 字段之间用 & 连接。
5. string / enum 使用 UTF-8 + RFC3986 percent-encoding。
6. number 必须先按 symbol metadata / 字段规则格式化成确定字符串。
7. digest = HMAC_SHA256(secret_utf8, signing_string_utf8)，输出 lowercase hex。
```

Golden signing vector：

```text
secret = test_command_secret_v1

signing_string =
command_id=cmd_20260526_000001&plan_id=plan_20260526_000001&leg_id=leg_1&strategy_id=trend_xau_h4_v1&account_id=acct_mt5_001&terminal_id=mt5_terminal_001&client_id=mt5_client_001&symbol=XAUUSD&broker_symbol=XAUUSD&action=BUY&order_type=MARKET&lots=0.10&price=&sl=2320.50&tp=2365.50&deviation_points=20&magic=26052601&comment=trend_xau_h4&position_ticket=&broker_order_id=&filling_policy=IOC&time_policy=GTC&expiration_time=&expires_at=1779800123000&idempotency_key=idem_cmd_20260526_000001

hmac =
044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e
```

#### Golden Sample JSON

Golden samples 必须进入代码仓库测试目录，例如：

```text
tests/golden/execution_client_protocol/session_hello.json
tests/golden/execution_client_protocol/execution_command_buy_market.json
tests/golden/execution_client_protocol/command_received.json
```

`session_hello.json`：

```json
{
  "message_id": "msg_hello_20260526_000001",
  "type": "session.hello",
  "schema_version": "ecp.v1.0",
  "correlation_id": "corr_boot_20260526_000001",
  "sequence": 1,
  "payload": {
    "client_id": "mt5_client_001",
    "platform": "MT5",
    "terminal_id": "mt5_terminal_001",
    "account_id": "acct_mt5_001",
    "token": "test_client_token",
    "capabilities": [
      "MARKET_ORDER",
      "CANCEL_ORDER",
      "RECONCILIATION_REQUEST"
    ],
    "resume": {
      "previous_session_id": "sess_previous",
      "last_gateway_message_id": "msg_cmd_previous",
      "last_gateway_sequence": 41,
      "pending_command_ids": []
    }
  }
}
```

`execution_command_buy_market.json`：

```json
{
  "message_id": "msg_cmd_20260526_000001",
  "type": "execution.command",
  "schema_version": "ecp.v1.0",
  "session_id": "sess_20260526_000001",
  "correlation_id": "corr_trade_20260526_000001",
  "causation_id": "intent_20260526_000001",
  "sent_at": 1779800000123,
  "sequence": 42,
  "payload": {
    "command_id": "cmd_20260526_000001",
    "plan_id": "plan_20260526_000001",
    "leg_id": "leg_1",
    "strategy_id": "trend_xau_h4_v1",
    "account_id": "acct_mt5_001",
    "terminal_id": "mt5_terminal_001",
    "client_id": "mt5_client_001",
    "symbol": "XAUUSD",
    "broker_symbol": "XAUUSD",
    "action": "BUY",
    "order_type": "MARKET",
    "lots": 0.1,
    "sl": 2320.5,
    "tp": 2365.5,
    "deviation_points": 20,
    "magic": 26052601,
    "comment": "trend_xau_h4",
    "filling_policy": "IOC",
    "time_policy": "GTC",
    "expires_at": 1779800123000,
    "idempotency_key": "idem_cmd_20260526_000001",
    "hmac": "044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e"
  }
}
```

`command_received.json`：

```json
{
  "message_id": "msg_received_20260526_000001",
  "type": "command.received",
  "schema_version": "ecp.v1.0",
  "session_id": "sess_20260526_000001",
  "correlation_id": "corr_trade_20260526_000001",
  "causation_id": "msg_cmd_20260526_000001",
  "sent_at": 1779800000188,
  "sequence": 17,
  "payload": {
    "command_id": "cmd_20260526_000001",
    "idempotency_key": "idem_cmd_20260526_000001",
    "account_id": "acct_mt5_001",
    "terminal_id": "mt5_terminal_001",
    "client_id": "mt5_client_001",
    "received_at": 1779800000180,
    "inbox_status": "RECORDED",
    "reason": "OK"
  }
}
```

### 23.2 SQLite State Store Schema

SQLite State Store 分两层：

```text
append-only facts
  → 不可变事实事件，用于恢复、审计、重放

projection tables
  → 为 HTTP query / risk check / execution scheduling 优化的当前状态
  → 可由 append-only facts 重建
```

State Store 启用：

```text
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
所有写入必须包在 transaction 中。
所有 server time 字段使用 INTEGER Unix milliseconds UTC。
所有名为 payload_json 的列都存 canonical JSON text。
每个 payload_json 都必须配套 payload_hash；payload_hash 存 canonical JSON UTF-8 bytes 的小写 SHA-256 hex，用于审计和重复检测。
State Store 只能通过统一配置并执行 migration 校验的连接构造路径对外创建，不得公开绕过 WAL、foreign_keys、busy_timeout 或 migration 的 unchecked pool 构造器。
```

#### SQLite Migration Strategy

Migration 必须 forward-only、可审计、可重复校验。

```sql
CREATE TABLE schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
```

规则：

```text
migration 文件命名：V0001__init.sql、V0002__state_store_schema.sql、V0003__execution_durability.sql、V0004__reconciliation_durability.sql、V0005__gateway_delivery_durability.sql、V0006__event_stream_sequence.sql、V0007__inbound_durability.sql、V0008__risk_workflow_inputs.sql、V0009__outbound_delivery_work.sql、V0010__inbound_raw_payload_length.sql
version 必须单调递增，不允许跳号复用。
checksum 使用 migration 文件内容 SHA-256 hex。
启动时必须校验已应用 migration 的 checksum；不一致则拒绝启动。
生产环境不做自动破坏性 DDL；需要 backfill / rebuild projection 时必须显式 maintenance mode。
append-only facts 表不得通过 migration 删除历史列；废弃字段只能停止写入。
durable projection 表可以 drop/rebuild，但必须能从 core_events / execution_events 重建。
仅由可选 sampled market.tick 驱动的 market_snapshots 不属于 durable rebuild 保证；不得用缺少 bid / ask / spread 的 market.bar 伪造 MarketSnapshot。
projection rebuild 必须在单一 transaction 内完成；任何解析、业务键校验或写入失败都必须整体回滚，并保留 rebuild 前的 projection。
SQLite `PRAGMA user_version` 必须同步写入并校验当前最高 migration version，`schema_migrations` 是权威记录；当前最高版本为 V0010，因此完整迁移后的 `user_version = 10`，两者不一致时拒绝启动。
```

`core_events`、`deadletter_events`、`system_events`、`risk_results`、`execution_commands` 和 `execution_events` 是不可变事实；migration 必须通过 trigger 阻止 `UPDATE` / `DELETE`。`market_bars` 是由 `market.bar` core fact 重建的查询 projection，不属于不可变事实表。状态变化只能追加新事实，或写入明确的 projection / lifecycle table。

`event_stream_log` 是 bounded summary replay log，不是执行事实来源。V0006 为每次成功 append 分配数据库内单调递增的 `stream_sequence`；外部 cursor 仍使用不可猜测的 `event_id`。单条 entry 插入后禁止 `UPDATE`，但允许 Event Stream Manager 按 retention policy 批量 `DELETE`；删除时 `outbound_spool.event_id` 按外键规则置空。durable execution facts 的保留不受 event cursor window 影响。`account_id = NULL` 只允许 `system.event` / `deadletter.summary`；其他 topic 必须绑定非空账户。

V0006 升级时重建 `event_stream_log`，按旧表 `rowid` 升序把 `stream_sequence` 回填为既有 row identity，因而保留升级前的 append 顺序；同时重建 `outbound_spool` 外键，并创建 `(topic, created_at, stream_sequence)`、`(topic, stream_sequence)`、`(account_id, stream_sequence)`、`(created_at, stream_sequence)` 四个 replay/retention 索引。

Projection rebuild 分为有明确 owner 的阶段：

```text
state-ingest rebuild（State Store owner）
  → 从 core_events 重放 account / symbol / position / order / market.bar
  → 重建 latest-state tables 与 market_bars
  → V0004 后公开的 rebuild_ingest_projections 在同一 transaction 中继续执行下述 reconciliation rebuild；返回 report 仍只统计 ingest 阶段，避免 standalone 调用丢失 full-set-only 成员
  → 不重建 tick-only market_snapshots

reconciliation projection rebuild（State Store owner，V0004）
  → 从 core_events 按 received_at / created_at / event_id 重放 account / symbol / position / order / reconciliation.result
  → reconciliation.result 使用与在线写入相同的账户级 full-set replacement / watermark 规则
  → 重建 account / symbol / position / order latest tables、set-membership 和 account checkpoint
  → 不修改 market tables 或 execution lifecycle tables

execution lifecycle rebuild（Execution owner）
  → 保留 execution_commands、plan definition 和 leg definition
  → 重置 command / leg / plan 的 materialized status
  → 按确定性顺序重放 command.received 与 execution_events
  → 状态转换规则由 Execution 里程碑实现，不得在 store 中复制一套状态机
```

`execution_plans` / `execution_legs` 的 definition 和 payload 属于 workflow journal，不通过删除 definition row 来重建；可重建的是其中的 materialized status 以及 `execution_command_states`。definition 缺失或损坏时必须 fail closed，不能从不完整的 execution event 猜测计划结构。

`reconciliation_runs` 的 request definition 也属于 workflow journal，不通过 projection rebuild 删除或重造。可重建的是由 durable `reconciliation.result` facts 派生的 account checkpoint 和 position / order full sets；rebuild 必须保持 empty-set deletion、集合 hash 和旧逐行事实防复活语义。

#### SQLite Status Enum Registry

所有 status / enum TEXT 字段必须在 migration 中使用 `CHECK (...)` 或由集中 enum table 约束。第一选择是 `CHECK`，因为值集合属于协议和状态机的一部分。

```text
execution_client_sessions.status
  → ACTIVE / STALE / DISCONNECTED / REJECTED

execution_client_sessions.clock_sync_status
  → SYNCED / DEGRADED / UNSYNCED

wire_inbox.status
  → RECEIVED / ACKED / HANDLED / DUPLICATE / DEADLETTER / FAILED

wire_outbox.status
  → PENDING / WRITE_STARTED / SENT / ACKED / FAILED / CANCELLED

trade_intents.status
  → ACCEPTED / RISK_BLOCKED / REJECTED / DUPLICATE / EXPIRED / CANCELLED

trade_intents.action
  → BUY / SELL / CLOSE / HOLD

execution_plans.status
  → PENDING / RECONCILING / MANUAL_RECONCILIATION_REQUIRED / PARTIAL / COMPLETED / FAILED / EXPIRED / CANCELLED

execution_legs.status
  → PENDING / SENT / DELIVERY_UNCONFIRMED / RECONCILING / MANUAL_RECONCILIATION_REQUIRED / COMMAND_RECEIVED / ACCEPTED / REJECTED / ORDER_SENT / PARTIALLY_FILLED / FILLED / FAILED / EXPIRED / CANCELLED

execution_commands.action
  → BUY / SELL / CLOSE / MODIFY / CANCEL

execution_command_states.status
  → CREATED / DISPATCHED / DELIVERY_UNCONFIRMED / DELIVERY_FAILED / RECONCILING / MANUAL_RECONCILIATION_REQUIRED / COMMAND_RECEIVED / ACCEPTED / REJECTED / ORDER_SENT / PARTIALLY_FILLED / FILLED / FAILED / EXPIRED / CANCELLED

command_delivery_attempts.status
  → PENDING / SENT / ACKED / BACKPRESSURE / NO_ACTIVE_SESSION / FAILED / UNCONFIRMED / CANCELLED

execution_events.status
  → ACCEPTED / ORDER_SENT / REJECTED / FILLED / PARTIALLY_FILLED / FAILED / EXPIRED / CANCELLED

reconciliation_runs.status
  → REQUESTED / PENDING_EVIDENCE / COMPLETED / MANUAL_RECONCILIATION_REQUIRED

system_events.severity
  → INFO / WARNING / ERROR / CRITICAL

event_stream_log.topic
  → market.snapshot / risk.summary / execution.summary / system.event / deadletter.summary

outbound_spool.status
  → PENDING / SENT / ACKED / FAILED / RETRYING / DEADLETTER
```

核心事实事件的 `event_type` 直接复用 durable message type，必要时加 domain suffix：

```text
market.bar
symbol.metadata
account.snapshot
position.snapshot
order.snapshot
execution.command.created
command.received
execution.event
reconciliation.request
reconciliation.result
trade.intent.accepted
trade.intent.rejected
risk.approved
risk.rejected
system.event
deadletter.event
```

#### Core Append-only Tables

```sql
CREATE TABLE core_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,

  message_id TEXT,
  schema_version TEXT NOT NULL,
  correlation_id TEXT,
  causation_id TEXT,

  account_id TEXT,
  client_id TEXT,
  terminal_id TEXT,
  strategy_id TEXT,
  intent_id TEXT,
  plan_id TEXT,
  leg_id TEXT,
  command_id TEXT,
  idempotency_key TEXT,

  event_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,

  source TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL
);

CREATE INDEX idx_core_events_type_time ON core_events(event_type, event_at);
CREATE INDEX idx_core_events_command ON core_events(command_id);
CREATE INDEX idx_core_events_intent ON core_events(intent_id);
CREATE INDEX idx_core_events_account_time ON core_events(account_id, event_at);
CREATE UNIQUE INDEX idx_core_events_message_id ON core_events(message_id)
  WHERE message_id IS NOT NULL;
```

```sql
CREATE TABLE deadletter_events (
  deadletter_id TEXT PRIMARY KEY,
  message_id TEXT,
  message_type TEXT,
  schema_version TEXT,
  reason TEXT NOT NULL,
  raw_payload TEXT,
  received_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  source TEXT NOT NULL DEFAULT 'legacy',
  raw_payload_length INTEGER CHECK (
    raw_payload_length IS NULL OR raw_payload_length >= 0
  ),
  error_message TEXT NOT NULL DEFAULT ''
);

CREATE TABLE system_events (
  system_event_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (
    severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')
  ),
  component TEXT NOT NULL,
  message TEXT NOT NULL,
  metadata_json TEXT CHECK (
    metadata_json IS NULL OR json_valid(metadata_json)
  ),
  timestamp INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_deadletter_events_reason_time
ON deadletter_events(reason, received_at, deadletter_id);

CREATE INDEX idx_system_events_component_time
ON system_events(component, timestamp, system_event_id);
```

`deadletter_events.message_type / schema_version / raw_payload` 来自 V0002；V0007 再增加 `source / raw_payload_length / error_message`，以区分来源、记录截断前长度并保存受限诊断文本。表仍由 V0002 的 append-only trigger 禁止更新和删除。

#### Wire / Session Tables

```sql
CREATE TABLE execution_client_sessions (
  session_id TEXT PRIMARY KEY,
  client_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  terminal_id TEXT,
  platform TEXT NOT NULL CHECK (platform IN (
    'MT5', 'BINANCE', 'OKX', 'IBKR', 'PAPER', 'BACKTEST', 'EXCHANGE'
  )),
  status TEXT NOT NULL CHECK (status IN (
    'ACTIVE', 'STALE', 'DISCONNECTED', 'REJECTED'
  )),
  capabilities_json TEXT NOT NULL CHECK (json_valid(capabilities_json)),
  remote_addr TEXT,
  connected_at INTEGER NOT NULL,
  last_heartbeat_at INTEGER,
  last_time_sync_at INTEGER,
  clock_sync_status TEXT CHECK (clock_sync_status IS NULL OR clock_sync_status IN (
    'SYNCED', 'DEGRADED', 'UNSYNCED'
  )),
  disconnected_at INTEGER,
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  updated_at INTEGER NOT NULL DEFAULT 0,
  last_outbound_sequence INTEGER NOT NULL DEFAULT 1
    CHECK (last_outbound_sequence > 0),
  max_inflight_commands INTEGER NOT NULL DEFAULT 1
    CHECK (max_inflight_commands > 0)
);

CREATE UNIQUE INDEX idx_active_session_identity
ON execution_client_sessions(client_id, account_id, COALESCE(terminal_id, ''))
WHERE status = 'ACTIVE';

-- terminal_id may be NULL; SQLite UNIQUE allows multiple NULL values.
-- COALESCE or a generated non-null terminal_key is required for active session uniqueness.

CREATE TABLE wire_inbox (
  message_id TEXT PRIMARY KEY,
  session_id TEXT,
  message_type TEXT NOT NULL,
  sequence INTEGER,
  received_at INTEGER NOT NULL,
  handled_at INTEGER,
  status TEXT NOT NULL,
  payload_hash TEXT NOT NULL
);

CREATE TABLE wire_outbox (
  message_id TEXT PRIMARY KEY,
  session_id TEXT,
  message_type TEXT NOT NULL,
  sequence INTEGER CHECK (sequence IS NULL OR sequence > 0),
  command_id TEXT,
  request_id TEXT,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'PENDING', 'WRITE_STARTED', 'SENT', 'ACKED', 'FAILED', 'CANCELLED'
  )),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT 0,
  sent_at INTEGER,
  acked_at INTEGER,
  last_error TEXT,
  CHECK (
    (message_type = 'execution.command'
      AND command_id IS NOT NULL AND request_id IS NULL)
    OR (message_type = 'reconciliation.request'
      AND command_id IS NULL AND request_id IS NOT NULL)
    OR (message_type NOT IN ('execution.command', 'reconciliation.request')
      AND command_id IS NULL AND request_id IS NULL)
  ),
  FOREIGN KEY (session_id) REFERENCES execution_client_sessions(session_id),
  FOREIGN KEY (command_id) REFERENCES execution_commands(command_id),
  FOREIGN KEY (request_id) REFERENCES reconciliation_runs(request_id)
);

CREATE UNIQUE INDEX idx_wire_outbox_session_sequence
ON wire_outbox(session_id, sequence)
WHERE session_id IS NOT NULL AND sequence IS NOT NULL;
```

#### Execution Tables

```sql
CREATE TABLE trade_intents (
  intent_id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('BUY', 'SELL', 'CLOSE', 'HOLD')),
  status TEXT NOT NULL,
  requested_at INTEGER NOT NULL,
  signal_expires_at INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_trade_intents_idempotency
ON trade_intents(idempotency_key);

CREATE TABLE risk_results (
  risk_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  approved INTEGER NOT NULL,
  reason TEXT NOT NULL,
  snapshot_age_ms INTEGER NOT NULL,
  symbol_metadata_age_ms INTEGER NOT NULL,
  evaluated_at INTEGER NOT NULL,
  valid_until INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL
);

CREATE TABLE execution_plans (
  plan_id TEXT PRIMARY KEY,
  risk_id TEXT NOT NULL,
  intent_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  strategy_id TEXT NOT NULL,
  status TEXT NOT NULL,
  mode TEXT NOT NULL,
  failure_policy TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE execution_legs (
  leg_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE execution_commands (
  command_id TEXT PRIMARY KEY,
  risk_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  account_id TEXT NOT NULL,
  client_id TEXT,
  terminal_id TEXT,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,
  expires_at INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  hmac TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_execution_commands_idempotency
ON execution_commands(idempotency_key);

CREATE TABLE command_delivery_attempts (
  attempt_id TEXT PRIMARY KEY,
  command_id TEXT,
  request_id TEXT,
  session_id TEXT,
  message_id TEXT,
  request_payload_json TEXT CHECK (
    request_payload_json IS NULL OR json_valid(request_payload_json)
  ),
  request_payload_hash TEXT CHECK (
    request_payload_hash IS NULL OR (
      length(request_payload_hash) = 64
      AND request_payload_hash NOT GLOB '*[^0-9a-f]*'
    )
  ),
  status TEXT NOT NULL CHECK (status IN (
    'PENDING', 'SENT', 'ACKED', 'BACKPRESSURE', 'NO_ACTIVE_SESSION',
    'FAILED', 'UNCONFIRMED', 'CANCELLED'
  )),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  attempted_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL DEFAULT 0,
  acked_at INTEGER,
  error TEXT,
  CHECK ((command_id IS NOT NULL AND request_id IS NULL)
      OR (command_id IS NULL AND request_id IS NOT NULL)),
  CHECK ((request_payload_json IS NULL AND request_payload_hash IS NULL)
      OR (request_payload_json IS NOT NULL AND request_payload_hash IS NOT NULL)),
  CHECK (message_id IS NULL OR session_id IS NOT NULL),
  CHECK (status NOT IN ('PENDING', 'SENT', 'ACKED', 'UNCONFIRMED')
      OR (session_id IS NOT NULL AND message_id IS NOT NULL)),
  CHECK (status <> 'ACKED' OR command_id IS NOT NULL),
  CHECK (status <> 'UNCONFIRMED'
      OR (error IS NOT NULL AND length(trim(error)) > 0)),
  FOREIGN KEY (command_id) REFERENCES execution_commands(command_id),
  FOREIGN KEY (request_id) REFERENCES reconciliation_runs(request_id),
  FOREIGN KEY (session_id) REFERENCES execution_client_sessions(session_id),
  FOREIGN KEY (message_id) REFERENCES wire_outbox(message_id)
);

CREATE INDEX idx_command_delivery_attempts_command
ON command_delivery_attempts(command_id, attempted_at);

CREATE INDEX idx_command_delivery_attempts_request
ON command_delivery_attempts(request_id, attempted_at);

CREATE TABLE execution_command_states (
  command_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  status TEXT NOT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0,
  last_delivery_error TEXT,
  created_at INTEGER NOT NULL,
  dispatched_at INTEGER,
  command_received_at INTEGER,
  reconciling_at INTEGER,
  completed_at INTEGER,
  updated_at INTEGER NOT NULL
);

CREATE TABLE execution_events (
  execution_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  plan_id TEXT,
  leg_id TEXT,
  account_id TEXT NOT NULL,
  status TEXT NOT NULL,
  broker_order_id TEXT,
  position_ticket TEXT,
  event_at INTEGER NOT NULL,
  filled_at INTEGER,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
```

V0003 同时增加全局 Circuit Breaker 的 append-only durable journal：

```sql
CREATE TABLE circuit_breaker_snapshots (
  scope TEXT NOT NULL CHECK (scope = 'GLOBAL'),
  state_revision INTEGER NOT NULL CHECK (state_revision > 0),
  schema_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
  recovery_epoch INTEGER NOT NULL CHECK (recovery_epoch >= 0),
  updated_at INTEGER NOT NULL CHECK (updated_at >= 0),
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  PRIMARY KEY (scope, state_revision)
);
```

表级 trigger 必须禁止 snapshot `UPDATE / DELETE`。Writer 接收 `expected_head_revision`，只允许写下一 revision；空 journal 的第一版是 revision 1。完全相同的已成功写入可以幂等重放，其他 head mismatch 返回 stale write。Typed read 校验 canonical JSON/hash 和 denormalized `schema_version / status / recovery_epoch / updated_at`；独立 head-metadata query 只读取可信 `state_revision / recovery_epoch`，使 payload 损坏时的 fail-closed restore 仍能保持 epoch 单调。

`execution_command_states` 的 compare-and-swap 必须同时匹配 immutable identity、预期 `status` 和预期 `updated_at`。目标 `updated_at` 必须严格大于预期值，防止 status 不变的并发 delivery update 同时成功；已经成功写入的完全相同目标状态允许幂等重试。

初始 command-state insert 重放时，如果数据库已有相同 immutable identity 且 `updated_at` 更高的 projection，必须保留现值并返回 `Duplicate`，不得用初始状态覆盖；相同版本但内容不同，或试图通过 insert 而非 CAS 提交更高版本，必须返回 conflict。

V0003 的 plan / leg definition 和 canonical payload 不可变。Leg / plan lifecycle update 必须作为一个 bundle：同时匹配 plan 与每个目标 leg 的 expected status + expected `updated_at`，验证更新后的完整 `ExecutionPlan` 及派生状态一致，再在 savepoint 内提交；任一 stale writer、缺失 leg、跨 plan identity 或最终状态不一致都必须回滚整个 bundle。已经成功提交的完全相同 bundle 允许幂等重放。

`risk_results.payload_json` 必须保留完整 `RiskResult`，包括 request / intent identity、`risk_request_hash`、`sizing_version`、risk base / budget、sizing candidate provenance、market / metadata / capacity age 和最终 `adjusted_legs`。typed repository 必须以 canonical JSON/hash 写入和读取，执行 `RiskResult` 语义校验，并校验 payload 与 denormalized 列以及父 `TradeIntent` 的 account / decision identity 一致。repository 还必须把父 intent 的 `action / requested_at / signal_expires_at` 和完整腿 shape 纳入契约：BUY / SELL approval 必须携带完整 actionable sizing；单腿 candidate 必须恰好一个，使用 `leg:{intent_id}:0` 并匹配父 intent 的 symbol / action / ratio=1 / proposed_sl；多腿按 `leg_id` 一一匹配 symbol / action / ratio / proposed_sl，不得缺腿或增加额外腿。ratio / proposed_sl 属于 provenance identity，使用原始 `f64` 位模式精确比较，不使用算术容差。HOLD 不得携带 sizing，但允许 approved no-op；第一版 CLOSE 不得 approved 且不得携带 sizing。approved 结果必须满足 `evaluated_at >= requested_at`、`evaluated_at < signal_expires_at` 且 `valid_until <= signal_expires_at`。typed read 必须从父 canonical payload 重建并重新验证上述契约，任何父行或 shape 漂移均报告损坏数据。相同 `risk_id`、父 intent 和完整 payload 的重放返回 Duplicate；相同 `risk_id` 的任意 identity / payload 漂移返回 conflict。同一 intent 可以使用不同 `risk_id` 追加多次独立评估，既有 `risk_results` 不得更新或删除。

对风险增加 intent，`RiskResult` 必须先于 execution plan / command 持久化。V0003 typed repository 已在同一 transaction boundary 内组合 intent / risk result / plan / leg / command / pristine command-state creation，保证每个 command lots 都能追溯到唯一风控审批；完整重放幂等，父图或 payload 漂移冲突，任一晚期失败整体回滚。Risk 领域模块本身不得依赖 store。

#### SQLite Foreign-key Registry

V0002 启用以下持久化引用；复合键同时约束账户归属，nullable 引用只在值存在时生效：

```text
risk_results(intent_id, account_id)
  → trade_intents(intent_id, account_id)

execution_plans(risk_id, intent_id, account_id)
  → risk_results(risk_id, intent_id, account_id)

execution_legs(plan_id)
  → execution_plans(plan_id)

execution_commands(risk_id, account_id)
  → risk_results(risk_id, account_id)
execution_commands(plan_id, risk_id, account_id)
  → execution_plans(plan_id, risk_id, account_id)
execution_commands(plan_id, leg_id)
  → execution_legs(plan_id, leg_id)

execution_command_states(command_id, account_id)
  → execution_commands(command_id, account_id)
execution_command_states.plan / leg
  → execution_plans / execution_legs

wire_inbox.session_id → execution_client_sessions.session_id
wire_outbox.session_id → execution_client_sessions.session_id
wire_outbox.command_id → execution_commands.command_id
command_delivery_attempts.command / session / message
  → execution_commands / execution_client_sessions / wire_outbox

outbound_spool.event_id → event_stream_log.event_id ON DELETE SET NULL
```

`core_events` 和 `execution_events` 故意不引用 projection：事实必须能在 projection 缺失或损坏时先落盘并参与恢复。Risk / plan / command workflow records 则必须按上述顺序在同一 write transaction 中创建，不能依赖关闭 `PRAGMA foreign_keys` 绕过约束。

#### Latest-state Projection Tables

```sql
CREATE TABLE market_bars (
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at INTEGER NOT NULL,
  PRIMARY KEY(account_id, symbol, timeframe, timestamp)
);

CREATE TABLE market_snapshots (
  account_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(account_id, symbol)
);

CREATE TABLE symbol_metadata_latest (
  account_id TEXT NOT NULL,
  broker_symbol TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(account_id, broker_symbol)
);

CREATE TABLE account_snapshots_latest (
  account_id TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE position_snapshots_latest (
  account_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(account_id, position_id)
);

CREATE TABLE order_snapshots_latest (
  account_id TEXT NOT NULL,
  broker_order_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY(account_id, broker_order_id)
);
```

#### Reconciliation Durable State（V0004）

V0004 增加 transport-neutral reconciliation run、账户级完整集合 checkpoint 和当前 set-membership projection。`request_id` 直接作为 run identity，不再引入第二个 `run_id`。物理字段注册表如下：

```text
reconciliation_runs
  → request_id PRIMARY KEY
  → request_event_id UNIQUE FK core_events
  → account_id / terminal_id? / client_id? / reason
  → scope = ACCOUNT | TARGETED
  → command_ids_json? / command_ids_hash?
  → since_server_time? / requested_at
  → status = REQUESTED | PENDING_EVIDENCE | COMPLETED | MANUAL_RECONCILIATION_REQUIRED
  → request_payload_json / request_payload_hash
  → result_event_id? UNIQUE FK core_events
  → result_observed_at? / result_payload_json? / result_payload_hash?
  → result_evaluation_json? / result_evaluation_hash?
  → completeness_json? / completeness_hash?
  → symbol_metadata_complete? / command_scope_complete?
  → manual_evidence_json? / manual_evidence_hash?
  → manual_evaluation_json? / manual_evaluation_hash?
  → created_at / updated_at

account_reconciliation_checkpoints
  → account_id PRIMARY KEY
  → source_request_id FK reconciliation_runs
  → result_observed_at
  → account_refreshed_at?
  → positions_observed_at / positions_set_hash
  → orders_observed_at / orders_set_hash
  → symbol_metadata_refreshed_at?
  → pending_commands_reconciled_at?
  → updated_at

reconciliation_position_set_members
  → PRIMARY KEY(account_id, position_id)
  → set_observed_at / payload_json / payload_hash

reconciliation_order_set_members
  → PRIMARY KEY(account_id, broker_order_id)
  → set_observed_at / payload_json / payload_hash
```

`scope=ACCOUNT` 必须配套 `command_ids_json/hash = NULL`；`scope=TARGETED` 必须配套非空、唯一、稳定排序的 command IDs 及 canonical hash。Result identity、payload、evaluation、completeness、`symbol_metadata_complete` 和 `command_scope_complete` 必须作为一组同时存在或同时缺失；两个 completeness alias 都必须与 canonical completeness payload 一致。manual evidence 和 manual evaluation 也必须成组存在。所有 JSON 使用 canonical JSON/hash 并在 typed read 时复核。

Set-membership 的 `payload_json` 保存对应 position / order snapshot 的 canonical payload，不只保存 digest。Typed checkpoint read 必须重新解析 payload，校验 account / member key / `set_observed_at` alias，重算完整集合 hash，并与同水位 latest projection 对照；`positions_observed_at / orders_observed_at` 必须等于 checkpoint 的 result 水位，两个集合 hash 还必须同时锚定到同一个 durable `reconciliation.result` canonical fact。任一缺行、额外行、自洽但脱离 result fact 的 payload/hash 漂移都报告持久化损坏。

`request_id / request_event_id / account_id / route / reason / scope / command IDs / since_server_time / requested_at / request_payload_* / created_at` 是 immutable request definition，必须由 trigger 禁止更新，run 也不得删除。Run transition 以当前 status 及对应 result / manual payload 尚不存在为条件；首个写入胜出，`updated_at` 单调推进。完全相同的已成功提交允许幂等重放，identity、result 或 evaluation 漂移必须冲突。

Result commit 必须在同一 transaction 中完成以下操作：

```text
追加 reconciliation.result durable fact
更新 reconciliation_runs 的 result / disposition
按 account_id 原子替换 position_snapshots_latest 完整集合
按 account_id 原子替换 order_snapshots_latest 完整集合
同步替换 reconciliation_position_set_members / reconciliation_order_set_members
更新 account_reconciliation_checkpoints 的 full-set 水位和集合 hash
account 存在时才更新 account_snapshots_latest / account_refreshed_at
```

`commit_reconciliation_result` 只接受 `Completed` 或 `PendingEvidence` evaluation。`Completed` 当且仅当 attention command 集合为空，`PendingEvidence` 必须至少保留一个 attention command；result 的每个 `unresolved_command_id` 都必须出现在该 attention 集合中，因此 unresolved 非空的 result 不能伪装为 `Completed`。`ManualRequired` 不能借 result payload 隐式落库，必须调用独立 manual escalation API，并持久化 `request_id / escalated_at / non-empty reason` 以及对应 manual evaluation；缺失 result 的升级保持 `result_observed_at = NULL`。Completed run 不能再升级为 manual。

Position / order full-set replacement 使用 `result.observed_at` 作为两个账户级水位。空数组必须删除该账户此前集合并记录 empty-set hash；不能解释成“本次没有提供”。只有更新的 result 水位可以覆盖当前 full set；相同水位 / 相同集合 hash是幂等重复，相同水位 / 不同集合 hash是 observation conflict，更旧 result 只能保留为事实，不能回退 projection。

账户级 full-set watermark 同时承担 tombstone 防线：之后到达的单行 position / order fact 若 `observed_at <` 对应 full-set watermark，不得插入或复活已被完整集合删除的业务键。同一账户、业务键和 `observed_at` 同时存在 single-row fact 与 full-set 时，只有 full-set 包含该键且 canonical payload 完全相同才相容；缺键或 payload 不同都是 `ObservationConflict`，第二个写入必须连同 durable fact 一起回滚。在线写入和 rebuild 都必须从 durable facts 做这项顺序无关校验，不能依赖 latest row 或同毫秒内的到达顺序。单行 fact 若新于水位，可以按 latest-row 规则写入，但此时所有行不再共享 checkpoint 水位，trusted Risk assembler 必须认为 full-set evidence 不一致并 fail closed，直到更新的完整 reconciliation result 收敛。

只有 `command_ids=None`、`terminal_id=None`、`client_id=None`、disposition 为 `Completed` 且 `command_scope_complete=true` 的无 route 限制账户级 run 可以把 `pending_commands_reconciled_at` 推进到 result 水位。`None` 只表达 request route 内的全量范围；当 `terminal_id` 或 `client_id` 存在时，scope 仍被限制在对应 session route，不能代表全账户。`command_scope_complete` 另行证明 evaluator 使用了同一可信 Store read snapshot 中该请求 scope 的完整 command 集合；targeted `Some` 不得声明该字段为 true。它是 trusted Core application assembler 的内部 attestation，不是 Execution Client 或 Gateway 可提交的 wire 字段。route-restricted completion 或 `command_scope_complete=false` 均不得推进账户级 command watermark。`Completed` 只证明所评估 scope 的投递不确定性已有权威 command lifecycle evidence 覆盖，并且本次没有 unresolved / manual finding，不表示订单 terminal。`PendingEvidence` 和 `ManualRequired` 都不得推进该 watermark。`account` 缺失时不推进 `account_refreshed_at`；第一版协议没有内建 metadata 完整范围，不能仅因 `symbol_metadata` 非空推进 `symbol_metadata_refreshed_at`。只有上层显式提供并持久化 `symbol_metadata_complete=true` 的完整性证据时才可推进。

#### Spool / Event Stream Tables

```sql
CREATE TABLE event_stream_log (
  stream_sequence INTEGER PRIMARY KEY AUTOINCREMENT
    CHECK (stream_sequence > 0),
  event_id TEXT NOT NULL UNIQUE,
  topic TEXT NOT NULL CHECK (topic IN (
    'market.snapshot',
    'risk.summary',
    'execution.summary',
    'system.event',
    'deadletter.summary'
  )),
  account_id TEXT CHECK (
    (account_id IS NOT NULL AND length(account_id) > 0)
    OR (account_id IS NULL AND topic IN ('system.event', 'deadletter.summary'))
  ),
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL CHECK (
    length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'
  ),
  created_at INTEGER NOT NULL
);

CREATE INDEX idx_event_stream_topic_time
ON event_stream_log(topic, created_at, stream_sequence);

CREATE INDEX idx_event_stream_topic_sequence
ON event_stream_log(topic, stream_sequence);

CREATE INDEX idx_event_stream_account_sequence
ON event_stream_log(account_id, stream_sequence);

CREATE INDEX idx_event_stream_created_sequence
ON event_stream_log(created_at, stream_sequence);

CREATE TABLE outbound_spool (
  spool_id TEXT PRIMARY KEY,
  target TEXT NOT NULL,
  event_id TEXT,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  payload_hash TEXT NOT NULL CHECK (
    length(payload_hash) = 64 AND payload_hash NOT GLOB '*[^0-9a-f]*'
  ),
  status TEXT NOT NULL CHECK (
    status IN ('PENDING', 'SENT', 'ACKED', 'FAILED', 'RETRYING', 'DEADLETTER')
  ),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_retry_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (event_id) REFERENCES event_stream_log(event_id)
    ON UPDATE RESTRICT ON DELETE SET NULL
);

CREATE INDEX idx_outbound_spool_due
ON outbound_spool(status, next_retry_at);

CREATE TRIGGER trg_event_stream_log_no_update
BEFORE UPDATE ON event_stream_log
BEGIN
  SELECT RAISE(ABORT, 'event_stream_log entries are immutable');
END;
```

`event_stream_log` 的 global 可见性由 topic 和账户列共同决定，不能由 publisher 自行解释：

```text
market.snapshot / risk.summary / execution.summary
  → account_id 必填且非空

system.event / deadletter.summary
  → 允许 account_id=NULL，表示所有具有 event:subscribe scope 的 principal 可见
  → 若携带 account_id，则仍按普通账户授权过滤

global summary
  → system.event 当前 payload 只允许 severity / component / timestamp
  → deadletter.summary 当前 payload 只允许 reason / received_at
  → 禁止携带 remote_addr、session_id、message_id、认证 identity、token/secret/HMAC、raw payload、parser detail 或任意未审查 metadata/message
  → 当前 TransportEvent 实际携带的诊断字段只保存在访问受限的 system_events / deadletter_events durable fact
```

当前 `TransportEventEvidence` 只携带经过类型约束的 `message_type / schema_version / raw_payload_length`。listener 在能够安全解析时填充 type/schema，并始终尽力记录原始 byte length；生产 adapter 把这些字段写入受限 durable fact，但有意令 `raw_payload = NULL`，因为原始内容可能包含 client credential、token 或 command HMAC。全局 summary 仍只保留白名单字段，不得把 typed evidence 或 parser detail 扩散到 Event WebSocket。

#### Message → Storage Mapping

| Execution Client message type | append-only | projection / side effect |
|---|---|---|
| `session.hello` | `system_events` on accepted/rejected | `execution_client_sessions` |
| `heartbeat` | `system_events` only on status change/anomaly | update `execution_client_sessions` |
| `market.tick` | optional sampled `core_events` | upsert `market_snapshots` |
| `market.bar` | `core_events` | insert `market_bars`; 不更新缺少 tick 字段的 `market_snapshots` |
| `symbol.metadata` | `core_events` | upsert `symbol_metadata_latest` |
| `account.snapshot` | `core_events` | upsert `account_snapshots_latest` |
| `position.snapshot` | `core_events` | upsert `position_snapshots_latest` |
| `order.snapshot` | `core_events` | upsert `order_snapshots_latest` |
| `execution.command` | `core_events` once per command creation | insert `execution_commands`, upsert `execution_command_states`, insert `command_delivery_attempts` per dispatch |
| `command.received` | `core_events` | update `execution_command_states.command_received_at/status` |
| `execution.event` | `core_events`, `execution_events` | project command / leg / plan state |
| `reconciliation.request` | `core_events`, `reconciliation_runs` request journal | persist transport-neutral run; pure domain separately returns eligible Execution-state CAS targets for the application layer; later Gateway binding may add `wire_outbox` |
| `reconciliation.result` | `core_events` | atomically update run / checkpoint and replace position / order full sets; snapshot alone does not change execution lifecycle |
| invalid schema/type | `deadletter_events` | no business projection |

Latest-state projection 的统一写入规则：

```text
fact append 与对应 projection write 必须位于同一 transaction。
重复 fact 的 ingest 必须使用已存事实的 metadata / payload 幂等执行 projection apply；即使 projection 缺失也要能够自愈，最终仍返回 Duplicate。
只有更大的 observed_at 才能覆盖 latest row；更旧的事实只追加，不回退 projection。
同一业务键、相同 observed_at、相同 payload_hash 视为幂等重复。
同一业务键、相同 observed_at、不同 payload_hash 必须返回 ObservationConflict，不允许以到达顺序静默覆盖。
payload_json 中存在的 account_id / symbol / position_id / broker_order_id / broker_symbol 必须与 projection 列业务键一致；不一致视为持久化损坏。MarketBar / MarketSnapshot payload 没有 account_id，其账户身份必须来自已认证 envelope，并与 core_events.account_id / projection account_id 一致。
所有列表查询必须使用确定性排序。
授权账户集合必须显式传入；空集合返回空结果，绝不解释为“所有账户”。
聚合多张 projection 的状态查询必须在同一 SQLite read transaction / snapshot 中完成。
state-ingest replay 顺序固定为 received_at、created_at、event_id 升序，不依赖 SQLite rowid。
```

V0002 的普通 `position.snapshot` / `order.snapshot` 仍是单行业务键 observation，单批缺失某行不表示删除。V0004 的 `reconciliation.result.positions / orders` 则是显式账户完整集合：在线 projection 和 rebuild 都按 result 水位原子替换，并用账户级 watermark / empty-set hash 作为 tombstone 防线。普通单行事实不得凭空删除其他行，也不得以旧于或等于 full-set 水位的 observation 复活已删除行。需要当前开放集合的流程必须同时验证所有行与新鲜 checkpoint 水位一致，否则 fail closed。

#### Gateway Delivery Durable State（V0005）

V0005 为 session epoch、outbound sequence、wire outbox 和 delivery attempt 增加明确的 revision/CAS 边界。它不引入 transport listener，也不修改 `ExecutionCommandState`。

```text
execution_client_sessions
  → revision / updated_at 提供 session heartbeat、stale、disconnect 和 replacement fencing
  → last_outbound_sequence 是该 session 的 Gateway→Client 单调序列
  → session.accepted 固定占用 sequence=1；第一个后续 outbound message 从 2 开始
  → max_inflight_commands 是 session.accepted 时协商并在 session epoch 内不可变的限制

wire_outbox
  → revision / updated_at 提供状态 CAS
  → command_id 与 request_id 二选一绑定 execution.command 或 reconciliation.request
  → PENDING 表示尚未开始 transport write
  → WRITE_STARTED 表示 write 已开始，进程崩溃后必须按不确定投递恢复，禁止自动重放
  → SENT 表示完整 envelope bytes 已被 transport write 接受
  → ACKED 只表示收到匹配 message_id / message_type / session 的 transport.ack

command_delivery_attempts
  → command_id 与 request_id 二选一，支持 command 和 reconciliation 两类 durable outbound subject
  → 尚未生成 outbox 的 typed rejection 保存完整未绑定 DeliveryRequest 的 canonical request_payload_json/hash；同 message_id 重放必须精确匹配 route、envelope 和 payload
  → ACKED 只用于 execution.command 的 command.received，不等于 transport.ack
  → UNCONFIRMED 表示 write 开始后因超时、断线或进程恢复而无法证明客户端是否收到
  → timeout 原因写入 error，不再把 TIMEOUT 当作状态
```

原子规则：

```text
同 route 新 session 的 ACTIVE replacement 必须在 BEGIN IMMEDIATE transaction 中 stale 旧 epoch 并插入新 epoch。进程内共享 activation mutex 必须把 live fence、durable replacement、live publish、disconnect 和 startup fence 串行化，禁止发布已经被并发 close 的 handle。
route 的 optional client_id / terminal_id 是筛选条件；0 个候选返回 NoActiveSession，多个候选返回 AmbiguousRoute，禁止任选一个。
command delivery 只允许 ACTIVE、heartbeat fresh、clock_sync_status=SYNCED 的 session；reconciliation.request 在 ACTIVE 且 fresh 的 session 上允许用于恢复 clock/broker state。
session 解析、inflight 检查和 sequence reserve 必须在同一 write transaction 中完成。
sequence reserve 与 outbox + attempt bundle insert 必须使用同一 transaction；失败整体回滚。
transport I/O 不得发生在 SQLite transaction 内。bundle commit 后先以 CAS 将 outbox 置为 WRITE_STARTED，再调用 transport sink。
明确 backpressure / 未写入分别落 BACKPRESSURE / FAILED；无法确认是否写入落 UNCONFIRMED。
transport.ack 只推进 wire_outbox：ACCEPTED / DUPLICATE 推进 ACKED，REJECTED 保持 FAILED rejection fact 并结束 transport-admission inflight 占用；它不把 delivery attempt 置为 ACKED，attempt 的 PENDING→SENT 仍来自 transport write completion。command.received 只推进 delivery attempt。late command.received 可以把 SENT / UNCONFIRMED 收敛为 ACKED，timeout/disconnect 不得覆盖已 ACKED attempt，late receipt 也不得抹掉 outbox rejection fact。
delivery replay matrix 必须稳定：PENDING/PENDING 只能由同一 durable envelope 继续一次 write；WRITE_STARTED 或 UNCONFIRMED 禁止自动重写；terminal sink outcome 和 typed rejection 原样返回；任一 subject、route、draft identity 或 payload 漂移都返回 conflict。
进程启动必须 fence 遗留 WRITE_STARTED 和旧 ACTIVE session；旧 transport callback 只能按精确 session_id/revision 操作，不能影响 replacement session。
Gateway 返回 delivery outcome 给 Execution application service；是否推进 DISPATCHED、DELIVERY_UNCONFIRMED、EXPIRED 或 reconciliation 仍由 Execution 状态机决定。
```

#### Event Stream Sequence 与 Inbound Durable Admission（V0006 / V0007）

V0006 将 Event WS 的内部排序从不可靠的 `(created_at, event_id)` 改为数据库分配的 `stream_sequence`。`event_id` 仍是外部 cursor 和幂等 identity；相同 `event_id` 只有 topic、账户、type、canonical payload 和 `created_at` 全部一致时才是 duplicate，任一漂移都返回 conflict。publisher 必须先提交 `event_stream_log`，再做 process-local live fanout。

V0007 为 Gateway 的 ACK-before-durable-admission contract 增加三张业务表：

```text
inbound_admissions
  → message_id 主 identity，(session_id, sequence) 也是唯一 identity
  → 保存 authenticated client/account/terminal route、message_type/schema、correlation/causation、完整 canonical envelope/hash 和 received_at
  → PENDING / PROCESSING / HANDLED / FAILED，revision + lease_owner + lease_expires_at 提供 crash recovery fencing

inbound_rejections
  → append-only stable typed rejection
  → 保存完整 canonical envelope/hash 与 authenticated route
  → 相同 identity/payload/reason 可幂等重放，identity 或 payload 漂移不得改写既有 decision

session_resume_admissions
  → hello_message_id 和 session_id 唯一
  → 保存 authenticated route、完整 canonical cursor/hash 和 received_at
  → 使用与 inbound admission 相同的 PENDING / PROCESSING / HANDLED / FAILED、revision 和 lease recovery
  → 可记录由 handler 创建的 reconciliation_request_id，但绝不授权 command replay
```

V0007 应用后 schema 共有 30 张业务表（不含 `schema_migrations`）：V0002 创建 22 张，V0003 新增 1 张，V0004 新增 4 张，V0007 新增 3 张；V0005/V0006 只重建或扩展既有表，不增加最终表数。三张 admission 表只提供 durable intake 与 recovery 状态，不改变事实表和领域 projection 的 owner transaction 边界。

V0008 新增 append-only `risk_capacity_snapshots` 和单调 `risk_capacity_snapshots_latest` 两张表，并为新写入的 `trade_intents` 强制合法 `decision_timestamp`；升级前的 NULL 保持 NULL，不能用 `requested_at` 伪造。V0009 新增 `outbound_delivery_work`。V0010 为 `inbound_admissions` 增加不可变 `raw_payload_length`，从 listener 的原始 `wire_bytes.len()` 取得，不能以 canonical envelope 长度代替；它不新增业务表。因此当前完整 schema 共有 33 张业务表，`user_version = 10`。

`outbound_delivery_work` 是 Execution application 层的 durable 调度 ownership，不是 Gateway transport retry queue：

```text
CREATED execution.command / REQUESTED reconciliation run
  → 首次 claim 时创建稳定 work identity 和 generation=1 的确定性 message_id
PENDING / PROCESSING
  → revision CAS、lease owner、lease expiry 和指数退避支持崩溃恢复
DeliveryInfrastructureError 或 delivery evidence 自相矛盾
  → 保留同一 generation/message_id 重试，不能把未知结果伪装成明确未写入
DefinitelyNotWritten 或可重试 typed rejection
  → 只有 Execution policy 明确允许时推进 generation，使用新的确定性 message_id
Sent
  → command 由 Execution 状态机推进 DISPATCHED，work 原子完成
Unconfirmed
  → command 推进 DELIVERY_UNCONFIRMED，并在同一 owner transaction 创建定向 reconciliation run；禁止盲目重发
Expired
  → 以显式服务器时间证据推进 EXPIRED 并终结 work
IdentityMismatch / peer TransportRejected
  → work 以 PERMANENT_REJECTION 终结；command 保持 CREATED，等待显式运维/策略处置，不伪造 lifecycle
receipt / execution evidence 已抢先推进 lifecycle
  → reclaim 后不再调用 transport，work 以 SUPERSEDED 终结
```

状态与 lease 规则：

```text
Accepted
  → 完整 envelope/cursor 已在 SQLite 中提交为 PENDING，可在崩溃后重新 claim
  → 不表示 handler-specific business fact/projection 已经提交

Duplicate
  → durable identity、authenticated route 和 canonical payload 完全一致
  → 不重复创建业务事实

Rejected
  → stable typed reason 已持久化，可对同一请求确定性重放

PENDING → PROCESSING
  → claim 必须设置非空 lease_owner、未来 lease_expires_at 并递增 revision

expired PROCESSING → PROCESSING
  → 只能由 reclaim 操作更换 owner/lease 并递增 revision

PROCESSING → HANDLED / FAILED
  → 必须同时匹配 identity、expected revision、lease owner，且完成时间必须严格早于 lease_expires_at
  → 过期 owner 即使尚未被其他 worker reclaim，也不能提交 terminal outcome
```

Production dispatcher 必须按 message type 调用 owner handler。`command.received`、`execution.event`、`reconciliation.result` 只有在领域事实和所有相关 projection 由 handler-specific owner transaction 原子提交后才允许 `HANDLED`；decode/schema/领域校验失败必须追加 deadletter 或稳定业务 rejection，并形成明确 terminal failure。不能用 noop handler、仅记录日志或多个互不原子的 public Store 调用把 admission 标为已处理。

当前 Rust 实现已经提供 handler-specific owner transaction，并把 Native TCP / Execution WebSocket listener 与 durable recovery dispatcher 接入 `sinan-core`。`PENDING` row 仍只表示可靠 intake；只有领域事实、projection 与 admission terminal state 同事务提交后才是 `HANDLED`。TransportEvent 只贯通 typed type/schema/raw length，raw content 按 credential/HMAC 脱敏策略明确不持久化。

### 23.3 Rust Crate Boundary

Rust workspace 按协议和状态边界拆，不按“技术热闹程度”拆。crate 依赖必须保持单向，避免 gateway、execution、risk 互相调用成一个大泥球。

目标 workspace：

```text
crates/
  types           # package: sinan-types
  protocol        # package: sinan-protocol
  domain          # package: sinan-domain
  store           # package: sinan-store
  gateway         # package: sinan-gateway
  risk            # package: sinan-risk
  execution       # package: sinan-execution
  reconciliation  # package: sinan-reconciliation
  events          # package: sinan-events
  http            # package: sinan-http
  core            # package: sinan-core
```

Workspace `Cargo.toml` 骨架：

```toml
[workspace]
resolver = "2"
members = [
  "crates/types",
  "crates/protocol",
  "crates/domain",
  "crates/store",
  "crates/gateway",
  "crates/risk",
  "crates/execution",
  "crates/reconciliation",
  "crates/events",
  "crates/http",
  "crates/core",
]

[workspace.package]
edition = "2021"
license = "MIT"

[workspace.dependencies]
anyhow = "1"
async-trait = "0.1"
axum = "0.7"
chrono = { version = "0.4", default-features = false, features = ["clock"] }
hmac = "0.12"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
rust_decimal = { version = "1", default-features = false, features = ["std"] }
sqlx = { version = "0.7", features = ["runtime-tokio", "sqlite", "migrate", "macros"] }
thiserror = "1"
tokio = { version = "1", features = ["full"] }
tokio-tungstenite = "0.21"
uuid = { version = "1", features = ["v7", "serde"] }
```

#### Crate 职责

```text
sinan-types
  → shared DTO / enum / error code / schema version
  → 不依赖 store / gateway / execution

sinan-protocol
  → transport frame/message encode/decode
  → WireMessage validation
  → message registry
  → transport ack envelope
  → transport-independent payload types
  → HMAC signing string generation / verification
  → 只依赖 sinan-types 和协议级库
  → 不做 risk / execution state transition

sinan-domain
  → server time provider
  → id generator
  → state transition rules
  → validation helpers

sinan-store
  → SQLite migrations
  → repository traits / SQLx implementation
  → transaction boundary
  → append-only event writer
  → projection writer / query reader

sinan-gateway
  → Native TCP / Execution WS transport adapters
  → session registry
  → GatewayInboundRouter / GatewayOutboundRouter pipeline
  → delivery result reporting
  → 不拥有 command lifecycle

sinan-risk
  → accept a complete immutable RiskRequest
  → pure deterministic hard risk gate / position sizing
  → RiskResult creation with final approved lots
  → no store / socket / HTTP / Compute Service dependency

sinan-execution
  → TradeIntent handling after initial validation
  → load State Store projections and assemble complete RiskRequest
  → execution.plan / execution.command generation
  → exact RiskResult.adjusted_legs lots mapping; no sizing recalculation
  → command signing through sinan-protocol
  → ExecutionCommandState transitions
  → dispatch request to gateway outbound port

sinan-reconciliation
  → delivery unconfirmed recovery
  → transport-neutral reconciliation.request generation and deterministic scope normalization
  → reconciliation.result validation / finding evaluation
  → Completed / PendingEvidence / ManualRequired disposition
  → explicit manual / missing-result escalation evidence
  → 不创建 WireMessage / wire_outbox，不选择 session，不从 snapshot 制造 execution fact 或 retry decision

sinan-events
  → event stream manager
  → bounded summary replay / live fanout semantics
  → account authorization filter and slow-consumer isolation
  → no execution decision

sinan-http
  → REST schema
  → auth / read-only debug auth
  → Event WS upgrade and text-message transport
  → map HTTP request to application command/query
  → no direct SQL string outside store query API

sinan-core
  → binary composition root
  → config / dependency injection / graceful shutdown
```

#### 当前 Rust 实现边界（第九里程碑）

```text
已完成并在 workspace 中组合
  → Control Plane REST：POST /trade-intents、GET /state、GET /time、intent/command detail
  → 独立 WS /events：durable cursor replay、high-water replay/live handoff、授权 filter、slow-consumer fail-closed
  → V0006 stream_sequence、V0007 inbound/resume admission、V0008 trusted Risk inputs 与 V0009 outbound work durability
  → Gateway production admission/resume/TransportEvent persistence ports；TransportEvent 只持久化 typed type/schema/raw length，不保存 raw content
  → durable Circuit Breaker 在 HTTP bind 前 restore
  → handler-specific owner transaction：transport.ack、market/account/position/order/metadata、command.received、execution.event 与 reconciliation.result
  → trusted single-snapshot RiskRequest assembler 与 TradeIntent→Risk→Execution worker
  → Native TCP / Execution WebSocket listener、session registry、DurableRecoveryDispatcher、durable outbound worker、HTTP/Event WS 和 retention 的统一启动、监督与 graceful shutdown；关键 worker 的失败、panic 或意外退出会立即触发 supervisor

仍需在 Control Plane 消费业务事件前完成
  → inbound/Risk/Execution owner transaction 原子 append market.snapshot/risk.summary/execution.summary
  → commit 后按 stream_sequence 有序 live fanout；崩溃恢复继续依赖 durable cursor replay
```

runtime shutdown 可以取消正在等待 transport 的 outbound batch item，不会继续串行等待批次剩余项。若取消发生在 durable claim 之后，work 保持 lease 和原 generation/message ID，后续进程只能在 lease 到期后按既有 reclaim 规则恢复，不能把取消伪装成 `DefinitelyNotWritten`。

因此 HTTP `202 ACCEPTED`、transport ACK `ACCEPTED` 和 V0007 `PENDING` 都只表示各自的 durable intake 承诺，不得写成 hard-risk 已审批、command 已生成、业务 handler 已完成或 broker 已执行。

当前 `sinan-core` 的实际运行配置如下；除标记为必填的两项外，右侧均为默认值：

```text
SINAN_CONTROL_PLANE_TOKEN              必填
SINAN_CONTROL_PLANE_ACCOUNTS           必填，逗号分隔；过滤后允许为空，表示仅可见全局脱敏事件
SINAN_CONTROL_PLANE_SUBJECT            control-plane
SINAN_CONTROL_PLANE_SCOPES             control-plane:write-intent,control-plane:read-state,event:subscribe
SINAN_DATABASE_URL                     sqlite://sinan.sqlite
SINAN_HTTP_ADDR                        127.0.0.1:8080
SINAN_EVENT_LIVE_CAPACITY              1024
SINAN_EVENT_REPLAY_LIMIT               1000
SINAN_EVENT_MAX_MESSAGE_BYTES          65536
SINAN_EVENT_WRITE_TIMEOUT_MS           5000
SINAN_EVENT_RETAIN_LATEST              10000
SINAN_EVENT_RETENTION_AGE_MS           900000
SINAN_EVENT_RETENTION_INTERVAL_MS      60000
SINAN_EXECUTION_NATIVE_TCP_ADDR        127.0.0.1:9100，仅允许 loopback plaintext bind
SINAN_EXECUTION_WS_ADDR                127.0.0.1:9101，仅允许 loopback plaintext bind
SINAN_EXECUTION_CLIENT_CREDENTIALS     []，严格 JSON client/account secret 列表
SINAN_DURABLE_RECOVERY_INTERVAL_MS     100
SINAN_DURABLE_RECOVERY_BATCH_SIZE      64
SINAN_DURABLE_RECOVERY_LEASE_MS        30000
SINAN_DURABLE_RECOVERY_HANDLER_TIMEOUT_MS 10000
SINAN_DURABLE_RECOVERY_FINALIZATION_BUDGET_MS 5000
SINAN_DURABLE_OUTBOUND_INTERVAL_MS     100
SINAN_DURABLE_OUTBOUND_BATCH_SIZE      64
SINAN_DURABLE_OUTBOUND_LEASE_MS        30000
SINAN_DURABLE_OUTBOUND_CONFIRMATION_TIMEOUT_MS 5000
SINAN_DURABLE_OUTBOUND_RETRY_BASE_MS   250
SINAN_DURABLE_OUTBOUND_RETRY_MAX_MS    30000
SINAN_RISK_WORKFLOW_INTERVAL_MS        250
SINAN_RISK_POLICY_JSON                 Risk worker 五项 all-or-none，无默认值
SINAN_STRATEGY_RISK_POLICY_JSON        Risk worker 五项 all-or-none，无默认值
SINAN_EXECUTION_POLICY_JSON            Risk worker 五项 all-or-none，无默认值
SINAN_EXECUTION_ROUTES_JSON            Risk worker 五项 all-or-none，无默认值
SINAN_EXECUTION_COMMAND_SIGNING_SECRET Risk worker 五项 all-or-none，无默认值
```

`SINAN_CONTROL_PLANE_ACCOUNTS` 的原始环境变量必须存在且非空；按逗号分隔、trim 并过滤空项后的账户集合可以为空，此时该 principal 不能读写任何账户对象，只能订阅全局脱敏的 `system.event / deadletter.summary`。所有 capacity、limit、batch size 和 duration 配置都必须为正数；outbound retry max 不得小于 base。Risk worker 的五项可信配置 `SINAN_RISK_POLICY_JSON / SINAN_STRATEGY_RISK_POLICY_JSON / SINAN_EXECUTION_POLICY_JSON / SINAN_EXECUTION_ROUTES_JSON / SINAN_EXECUTION_COMMAND_SIGNING_SECRET` 要么全部缺失并关闭 worker，要么全部存在且非空；route 必须按 `(account_id, symbol)` 唯一。

#### Dependency Direction

下层 crate 不依赖上层 crate。越靠下越接近纯类型 / domain / storage，越靠上越接近 transport / composition。

```text
sinan-core
  → sinan-gateway / sinan-http
  → sinan-risk / sinan-execution / sinan-reconciliation / sinan-events
  → sinan-store
  → sinan-domain
  → sinan-types

sinan-protocol
  → sinan-types
```

允许：

```text
sinan-execution → sinan-risk
sinan-execution → sinan-store
sinan-execution → sinan-domain
sinan-execution → sinan-protocol for signing / protocol payloads
sinan-execution → outbound delivery port trait defined in execution or domain
sinan-reconciliation → sinan-protocol / sinan-types / sinan-execution
sinan-gateway → sinan-protocol
sinan-gateway → sinan-store for session / wire inbox / wire outbox / delivery attempts
sinan-gateway → sinan-execution only to implement the Execution-owned outbound delivery port
sinan-http → sinan-execution application service
sinan-http → sinan-store query service
sinan-http → sinan-events subscription service
sinan-events → sinan-store projection reader
```

禁止：

```text
sinan-protocol → sinan-store / sinan-gateway / sinan-risk / sinan-execution
sinan-gateway router → sinan-risk
sinan-gateway router → direct SQL for execution state
sinan-risk → sinan-gateway
sinan-risk → sinan-store / HTTP client / Compute Service client
sinan-execution → Compute Service client / live position sizing HTTP
sinan-reconciliation → sinan-gateway / sinan-http
sinan-store → sinan-gateway / sinan-http / sinan-execution
sinan-http → sinan-gateway session internals
sinan-events → mutate execution_command_states
```

#### Handler Ownership

| message / API | handler owner | writes |
|---|---|---|
| `session.hello` | gateway-session | sessions, system_events |
| `heartbeat` | gateway-session | sessions, system_events on anomaly |
| `time.sync.request` | gateway-time | none |
| `market.tick` | gateway inbound → market ingest service | market_snapshots |
| `market.bar` | gateway inbound → market ingest service | core_events, market_bars |
| snapshots | state ingest service | core_events, latest-state tables |
| `command.received` | execution service | core_events, execution_command_states |
| `execution.event` | execution projection service | core_events, execution_events, command/leg/plan projection |
| `execution.command` | execution service + gateway outbound port | execution_commands, execution_command_states; Gateway only adds command_delivery_attempts and wire_outbox |
| `reconciliation.request` | reconciliation service + gateway outbound adapter | core_events and reconciliation_runs; pure domain returns optional command-state CAS targets but run persistence does not imply they were applied; Gateway only adds command_delivery_attempts and wire_outbox |
| `reconciliation.result` | reconciliation service | core_events, reconciliation_runs, account checkpoint, atomic position / order full sets; no snapshot-driven command transition |
| `POST /trade-intents` | TradeIntent intake application service | trade_intents；后续 workflow processor 在完整一致性 risk input 可用后写 risk_results、plans、commands |
| `GET /state` | HTTP query service | read-only projections |
| `WS /events` | HTTP Event WS transport + event stream manager | transport 只读；publisher 先写 event_stream_log，再做 live fanout |

#### Key Port Traits

Port trait 必须定义在 domain / execution 侧，adapter 实现在 gateway / store 侧。这样 Execution Engine 可以请求投递，但不能知道具体 transport。

```rust
use std::{future::Future, pin::Pin};
use sinan_protocol::{ExecutionClientMessage, ReconciliationRequest};
use sinan_types::{
    AccountId, ClientId, CommandId, ExecutionCommand, MessageId, SessionId,
    TerminalId,
};

pub struct DeliveryRequest<T> {
    pub account_id: AccountId,
    pub client_id: Option<ClientId>,
    pub terminal_id: Option<TerminalId>,
    pub command_id: Option<CommandId>,
    pub message: ExecutionClientMessage<T>,
    pub expires_at: Option<i64>,
}

pub struct DeliveryReceipt {
    pub attempt_id: String,
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub sent_at: i64,
    pub confirmation_deadline_at: i64,
}

pub enum DeliveryRejectionReason {
    NoActiveSession,
    AmbiguousRoute { candidate_count: usize },
    ClockUnhealthy,
    Expired,
    IdentityMismatch { field: &'static str },
    Backpressure { queue_depth: usize },
    InflightLimit { limit: u64 },
    TransportRejected { reason: String },
}

pub enum DeliveryOutcome {
    Sent(DeliveryReceipt),
    Rejected(DeliveryRejection),
    DefinitelyNotWritten(DeliveryFailure),
    Unconfirmed(DeliveryUncertainty),
}

pub type DeliveryFuture<'a> = Pin<Box<
    dyn Future<Output = Result<DeliveryOutcome, DeliveryInfrastructureError>>
        + Send
        + 'a,
>>;

pub trait OutboundDeliveryPort: Send + Sync {
    fn deliver_execution_command(
        &self,
        request: DeliveryRequest<ExecutionCommand>,
    ) -> DeliveryFuture<'_>;

    fn deliver_reconciliation_request(
        &self,
        request: DeliveryRequest<ReconciliationRequest>,
    ) -> DeliveryFuture<'_>;
}
```

`DeliveryRejection`、`DeliveryFailure`、`DeliveryUncertainty` 都必须携带稳定的 `attempt_id / message_id`、可用时的 `session_id`、服务器时间和明确原因。`DeliveryInfrastructureError` 只表示 Store 损坏/不可用等 adapter 无法形成可信 delivery outcome 的基础设施故障；可预期的无 session、route 歧义、clock unhealthy、expiry、backpressure 和 transport write 结果都必须返回 typed `DeliveryOutcome`。

传入的 `WireMessage` 是未绑定 session 的 draft：`session_id / sequence / sent_at` 必须为空。Gateway 在选择当前 authenticated session 并原子 reserve sequence 后填充这些字段；Execution 不得预选 session。

State Store port 示例：

```rust
use async_trait::async_trait;
use sinan_types::{
    CommandId, CoreEvent, ExecutionCommand, ExecutionCommandState,
    ExecutionEvent, TradeIntent,
};

#[async_trait]
pub trait ExecutionRepository: Send + Sync {
    async fn append_core_event(&self, event: CoreEvent) -> anyhow::Result<()>;
    async fn insert_trade_intent(&self, intent: TradeIntent) -> anyhow::Result<()>;
    async fn insert_execution_command(&self, command: ExecutionCommand) -> anyhow::Result<()>;
    async fn update_command_state(&self, state: ExecutionCommandState) -> anyhow::Result<()>;
    async fn append_execution_event(&self, event: ExecutionEvent) -> anyhow::Result<()>;
    async fn get_command_state(&self, command_id: &CommandId) -> anyhow::Result<Option<ExecutionCommandState>>;
}
```

Clock / ID port 示例：

```rust
pub trait ServerClock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub trait IdGenerator: Send + Sync {
    fn new_id(&self, prefix: &str) -> String;
}
```

约束：

```text
OutboundDeliveryPort 只能返回 delivery outcome，不得直接修改 ExecutionCommandState。
ExecutionRepository 的 transaction API 必须支持 intent / risk / plan / command 同事务写入。
Gateway adapter 实现 OutboundDeliveryPort，并负责写 command_delivery_attempts / wire_outbox。
Execution service 根据 DeliveryOutcome 推进 ExecutionCommandState。
DeliveryOutcome::Sent 只允许在完整 wire envelope 已被当前 active transport 的实际 write 接受后返回；仅写入 SQLite PENDING 或进入尚未执行的用户态队列不构成 Sent。
wire_outbox.ACKED 只由 ACCEPTED / DUPLICATE transport.ack 推进；REJECTED transport.ack 保留 FAILED fact。command_delivery_attempts.ACKED 只由已验证的 command.received 推进。
Gateway 不公开 retry API；同一 command 的后续 delivery 必须由 Execution policy 显式请求，并使用新的 message_id / attempt_id。
```

### 23.4 HTTP API Schema

HTTP API 不暴露 Execution Client transport session / raw WireMessage，也不允许 Control Plane 直接提交 `execution.command`。HTTP schema 由 State Store projection 决定。

#### HTTP Auth Header

所有 HTTP / Event WebSocket 请求必须携带认证信息。推荐格式：

```http
Authorization: Bearer <internal_service_token>
X-Request-Id: req_20260526_000001
X-Correlation-Id: corr_trade_20260526_000001
X-Idempotency-Key: idem_intent_20260526_000001
```

规则：

```text
Authorization 必填。
X-Request-Id 必填，用于 HTTP request 级追踪。
X-Correlation-Id 推荐必填，用于跨 workflow / intent / command 串联。
X-Idempotency-Key 对 POST /trade-intents 必填，必须等于 TradeIntent.idempotency_key。
Debug read-only API 必须使用带 debug:read scope 的 token。
Control Plane token 不得访问 /debug/commands 的 command payload HMAC，除非具备 execution:debug-sensitive scope。
Event WebSocket 在 handshake 阶段使用同一 Authorization header。
```

scope 建议：

```text
control-plane:write-intent
control-plane:read-state
event:subscribe
debug:read
execution:debug-sensitive
admin:maintenance
```

#### HTTP Error Response

所有非 2xx 响应使用统一错误结构：

```ts
export interface HttpErrorResponse {
  error_code: ErrorCode
  message: string
  request_id: string
  correlation_id?: string
  server_time: number
  details?: Record<string, unknown>
}
```

HTTP visible `ErrorCode` 必须来自 `7.1 Common Types` 的集中枚举。常用映射：

| HTTP status | error_code | 场景 |
|---:|---|---|
| 400 | `BAD_REQUEST` / `SCHEMA_VALIDATION_FAILED` / `MISSING_REQUIRED_FIELD` / `INVALID_FIELD_TYPE` | JSON 无法解析或字段不合法 |
| 401 | `UNAUTHORIZED` / `AUTHENTICATION_FAILED` | 缺少 token 或 token 无效 |
| 403 | `FORBIDDEN` | token 有效但 scope 不足 |
| 404 | `NOT_FOUND` | intent / command / event 不存在 |
| 405 | `METHOD_NOT_ALLOWED` | endpoint 不支持该 method |
| 409 | `CONFLICT` / `IDEMPOTENCY_KEY_CONFLICT` / `DUPLICATE_IDEMPOTENCY_CONFLICT` | idempotency key 对应不同 payload |
| 422 | `TRADE_INTENT_EXPIRED` / `TRADE_INTENT_TIME_INVALID` / `TIME_SYNC_UNHEALTHY` / `ACCOUNT_SNAPSHOT_STALE` / `SYMBOL_METADATA_STALE` / `ORDER_SNAPSHOT_STALE` | 请求语义合法但不能进入执行流程 |
| 429 | `RATE_LIMITED` / `COMMAND_DISPATCH_BACKPRESSURE` | ingress 限流或执行队列背压 |
| 500 | `INTERNAL_ERROR` | 未分类内部错误 |
| 503 | `SERVICE_UNAVAILABLE` / `STATE_STORE_UNAVAILABLE` / `REDIS_UNAVAILABLE` | 依赖不可用或服务降级 |

成功响应状态码：

| Endpoint | HTTP status | body status |
|---|---:|---|
| `POST /trade-intents` 新 intent 接收并进入流程 | 202 | `ACCEPTED` |
| `POST /trade-intents` 同 idempotency key 重放且 payload 相同 | 200 | `DUPLICATE` |
| `POST /trade-intents` 风控阻断 | 200 | `RISK_BLOCKED` |
| `POST /trade-intents` 业务拒绝但请求已被审计记录 | 200 | `REJECTED` |
| `GET /state` | 200 | n/a |
| `GET /time` | 200 | n/a |
| `GET /trade-intents/:intent_id` | 200 | n/a |
| `GET /execution/commands/:command_id` | 200 | n/a |

上表是完整目标 workflow 语义。当前 `POST /trade-intents` 仍只负责 durable intake，因此同步请求只返回 `202 / ACCEPTED` 或幂等的 `200 / DUPLICATE`；异步 Risk→Execution worker 已接入，但不会把后续生成的 RiskResult 伪装成原 POST 的同步响应。已持久化的 rejected RiskResult 会在 `GET /trade-intents/:intent_id` 中派生为 `RISK_BLOCKED`。

#### POST /trade-intents

```ts
export interface SubmitTradeIntentRequest {
  intent: TradeIntent
}

export interface SubmitTradeIntentResponse {
  intent_id: string
  status:
    | "ACCEPTED"
    | "RISK_BLOCKED"
    | "REJECTED"
    | "DUPLICATE"
  reason: ErrorCode | "OK"
  correlation_id: string
  accepted_at: number
  state_ref?: {
    plan_id?: string
    risk_id?: string
  }
}
```

规则：

```text
ACCEPTED
  → intent 已通过持久化前的 schema、header、认证、账户授权和基本时间格式校验，并已进入 State Store 的 crash-recoverable intake
  → 后续 hard-risk 只能由 Trading Core 使用同一可信读快照组装完整 RiskRequest；intake handler 必须校验并持久化 TradeIntent.decision_timestamp，不得用 requested_at 代替它来重建 StrategyDecision.timestamp，也不得在缺少 policy / capacity / full-set watermark 时假装已完成风控
  → 不代表 broker 已成交
  → 不代表已经生成 risk_result、execution.plan 或 execution.command

DUPLICATE
  → intent_id 或 idempotency_key 已存在
  → 返回原始处理结果摘要

RISK_BLOCKED / REJECTED
  → 不生成 execution.command
  → 必须可在 audit / risk summary 中查询原因
```

持久化前即可确定的认证、schema、header/body idempotency、账户 scope 和请求时间格式错误使用对应 4xx，且不得创建 TradeIntent。TradeIntent 已持久化后，由完整 `RiskRequest` 产生并持久化的 hard-risk rejection 使用 `200 / RISK_BLOCKED`；其他已经形成稳定审计事实的业务拒绝使用 `200 / REJECTED`。不得把同一类 durable risk rejection 有时映射为 422、有时映射为 200。

#### GET /state

```ts
export interface TradingCoreStateResponse {
  server_time: number
  clock_health: "HEALTHY" | "DEGRADED" | "UNHEALTHY"

  accounts: AccountSnapshot[]
  positions: PositionSnapshot[]
  orders: OrderSnapshot[]
  symbols: SymbolMetadataSnapshot[]

  sessions: Array<{
    session_id: string
    client_id: string
    account_id: string
    terminal_id?: string
    platform: string
    status: "ACTIVE" | "STALE" | "DISCONNECTED"
    clock_sync_status?: "SYNCED" | "DEGRADED" | "UNSYNCED"
    last_heartbeat_at?: number
  }>

  execution: {
    open_plans: ExecutionPlan[]
    pending_commands: ExecutionCommandState[]
    recent_events: ExecutionEvent[]
  }

  risk: {
    latest_results: RiskResult[]
    circuit_breaker_active: boolean
    circuit_breaker?: CircuitBreakerSummary
  }
}
```

`GET /state` 是 projection query，不是事实流导出。它用于 Control Plane 校准上下文、Debug UI 诊断、WS gap 后恢复。

`GET /state` 是调用方授权账户范围内的多账户聚合 projection。`accounts` 来自 `account_snapshots_latest`，无可见账户快照时返回空数组，不伪造默认账户。`accounts / positions / orders / symbols / sessions` 以及其他 account-bound 数据必须使用相同的 authorization scope，并且只能通过 `account_id` 关联。HTTP query service 必须在同一 SQLite read transaction / snapshot 中读取相关 projection，避免返回跨表时间撕裂的状态。

`GET /state` 的 `open_plans / pending_commands / recent_events / latest_results` 必须分别使用显式配置的正数上限，并采用稳定排序。默认口径是账户、业务 ID 升序；有时间语义的 bounded 集合先按时间倒序选取上限，再在响应中按 `(time, id)` 升序输出，保证相同快照和配置得到确定结果。`latest_results` 表示每个可见 intent 的最新 `evaluated_at / risk_id`，不是无界导出全部历史。

#### GET /time

```ts
export interface TradingCoreTimeResponse {
  server_now_ms: number
  server_receive_at: number
  server_send_at: number
  clock_health: "HEALTHY" | "DEGRADED" | "UNHEALTHY"
  max_internal_server_skew_ms: number
  max_decision_time_skew_ms: number
  max_decision_time_sync_age_ms: number
  max_decision_time_sync_rtt_ms: number
  control_plane_time_sync_interval_ms: number
  max_decision_intent_age_ms: number
}
```

`server_now_ms` 必须等于 `server_send_at`。`server_receive_at / server_send_at` 用于 Control Plane 估算 offset；业务判断仍以 Trading Core 当前 server time 为准。

#### GET /trade-intents/:intent_id

```ts
export interface TradeIntentStatusResponse {
  intent_id: string
  status: string
  reason?: ErrorCode | "OK"
  risk_id?: string
  plan_id?: string
  command_ids: string[]
  created_at: number
  updated_at: number
}
```

#### GET /execution/commands/:command_id

```ts
export interface ExecutionCommandStatusResponse {
  command_id: string
  state: ExecutionCommandState
  command?: ExecutionCommand
  events: ExecutionEvent[]
}
```

`command` payload 可以对 Debug UI 返回，但对普通 Control Plane 默认只返回 state summary。生产环境应按权限隐藏 HMAC 和 broker-sensitive 字段。

#### WS /events Subscribe

```ts
export interface EventSubscribeRequest {
  topics: Array<
    | "market.snapshot"
    | "risk.summary"
    | "execution.summary"
    | "system.event"
    | "deadletter.summary"
  >
  account_id?: string
  last_event_id?: string
}

export interface EventSubscribeResponse {
  status: "SUBSCRIBED" | "RESUME_FAILED"
  reason?: "GAP_DETECTED" | "CURSOR_EXPIRED" | "UNAUTHORIZED"
  server_time: number
  next_event_id?: string
  requires_state_reload: boolean
}
```

Event WS 每个 frame 必须是一个 UTF-8 Text JSON message，禁止 Binary。client message 使用显式 discriminant：

```ts
export type EventClientMessage =
  | ({ op: "subscribe" } & EventSubscribeRequest)
  | { op: "unsubscribe" }
  | { op: "ping" }

export type EventServerMessage =
  | ({ op: "subscription" } & EventSubscribeResponse)
  | {
      op: "event"
      event_id: string
      topic: "market.snapshot" | "risk.summary" | "execution.summary" | "system.event" | "deadletter.summary"
      account_id?: string
      event_type: string
      created_at: number
      payload: unknown
    }
  | { op: "pong"; server_time: number }
```

`topics` 必须非空、唯一且只包含固定枚举；`account_id` 只能缩小 principal 的账户授权范围。一个连接同一时刻最多有一个 active subscription，新的 `subscribe` 原子替换旧订阅；`unsubscribe` 停止推送但不关闭 socket。未知 `op`、非法 JSON、超限 message 或 Binary frame 关闭连接。`next_event_id` 第一版保持省略；subscriber 只能以已经实际收到的 event item 中的 `event_id` 推进 cursor，不能以尚未写出的 high-water 跳过事件。

恢复规则：

```text
SUBSCRIBED + requires_state_reload=false
  → cursor resume 成功

RESUME_FAILED + requires_state_reload=true
  → client 必须 GET /state / GET /time
  → CURSOR_EXPIRED / UNAUTHORIZED 时当前连接保持打开但没有 active subscription，可以在校准/纠正 scope 后重新 subscribe
  → GAP_DETECTED（当前由 replay limit exceeded 产生）响应后服务端以 1013 关闭，client 必须新建连接再 subscribe
```

#### Debug Read-only API

以下是目标 Debug API；当前第八里程碑尚未注册这些 `/debug/*` route。现有 `GET /execution/commands/:command_id` 只在 bearer principal 额外具备 `execution:debug-sensitive` scope 时返回 command payload。

```text
GET /debug/state
GET /debug/sessions
GET /debug/events?since_event_id=...
GET /debug/commands/:command_id
```

约束：

```text
必须使用 debug/read-only credential。
不得提供 POST /debug/execute。
不得直接写 execution.command。
不得绕过 TradeIntent / Risk / Execution Engine。
```

---

## 24. 第一版实现前验证清单

第一版进入真实执行前，必须先通过 fake Execution Client / Paper Executor 的协议测试。

### 必测场景

```text
Native TCP length-prefixed JSON 拆包 / 粘包
Native TCP frame length <= 0 → WIRE_PROTOCOL_VIOLATION
Native TCP frame length > max_frame_bytes → WIRE_FRAME_TOO_LARGE
Execution WebSocket one message = one WireMessage
Execution WebSocket message > max_message_bytes → WIRE_FRAME_TOO_LARGE
WireMessage schema validation
schema_version format ecp.v<major>.<minor>
golden sample JSON parses in Rust / TS / MQL5
hello / time.sync.request may omit sent_at
post-sync business WireMessage missing sent_at → schema/deadletter
schema major mismatch → deadletter.event
unknown message type → deadletter.event
session_id / client_id / account_id / terminal_id mismatch → SESSION_IDENTITY_MISMATCH
HMAC golden vectors: Execution Engine generate / MQL5 verify
HMAC golden vectors: wrong field order must fail
HMAC golden vector matches 044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e
RFC3986 encoding: 空格、中文、特殊字符
number formatting: price digits / volume step / trailing zeros
TradeIntent accepted / risk_blocked / duplicate / rejected
TradeIntent missing account_id / idempotency_key / decision_timestamp → schema validation failed
TradeIntent decision_timestamp < 0 or decision_timestamp > requested_at → TRADE_INTENT_TIME_INVALID
same TradeIntent idempotency_key with different payload → IDEMPOTENCY_KEY_CONFLICT
TradeIntent.action BUY / SELL / CLOSE / HOLD persists to trade_intents.action without direction remapping
TradeIntent cannot carry final execution.command fields
Trading Core derives execution.plan / execution.command from TradeIntent
HTTP retry with same intent_id and same idempotency_key is idempotent
position sizing same complete RiskRequest + sizing_version → byte-for-byte reproducible result
unknown position_sizing_version → RISK_INPUT_INVALID; no implicit algorithm fallback
Compute Service unavailable → live hard-risk position sizing remains available locally
position / order empty set without fresh account-level full-set watermark → fail closed
position / order row watermark mismatch, or account / full-set / capacity evidence predating command reconciliation → fail closed
cross-account market / metadata / snapshot input → fail closed
RiskCapacity account / strategy mismatch or insufficient remaining_strategy_legs → fail closed
position sizing known vectors: single-leg / multi-leg / cost buffer / volume-step floor
position sizing missing SL / market / tick_value_loss / fresh metadata → fail closed
BUY / SELL proposed_risk_pct outside (0, 100] or non-finite → RISK_INPUT_INVALID
position sizing raw lots < volume_min → reject whole intent; never round up or drop a leg
position sizing respects volume_max / exposure / margin caps and actual_risk <= risk_budget
pending order / command margin is conservatively accumulated without inferred offset
UNKNOWN or internally inconsistent broker order, mismatched command state identity / lifecycle → fail closed
terminal command state without completed_at, or non-terminal state with completed_at → fail closed
active order / BUY-SELL command broker_symbol mismatches canonical symbol metadata → fail closed
non-terminal MODIFY without a structured risk-reduction proof → PENDING_EXPOSURE_CONFLICT
Decimal lots that round up or lose volume-step alignment through the executable f64 DTO → INVALID_VOLUME
position sizing properties: step-aligned; wider stop or smaller budget never increases lots
multi-leg sizing sums absolute worst-stop loss and applies no correlation / hedge offset
HOLD → approved no-op RiskResult without sizing fields or execution plan
CLOSE without target position / close lots → RISK_REDUCTION_NOT_PROVABLE
circuit breaker recovery evidence timestamp < triggered_at → remain OPEN
circuit breaker different incident evidence while OPEN → advance recovery epoch; old readiness remains invalid
circuit breaker healthy observation while OPEN → IncidentEvidenceCleared; same violation recurring starts a new epoch
circuit breaker HALF_OPEN financial value above baseline → reopen, even when below policy threshold
circuit breaker same safety error is idempotent; different CircuitBreakerError starts a new epoch
circuit breaker durable restore round-trips complete OPEN / HALF_OPEN state and append-only revision
circuit breaker missing / corrupt / unknown snapshot → persist OPEN before live flow; corrupt high epoch → known epoch + 1
circuit breaker Store unavailable / recovery epoch overflow → return inspectable fail-closed outcome; no live flow
sinan-core startup restores/persists fail-closed Circuit Breaker before binding HTTP
RiskResult repository enforces parent intent action / requested_at / signal_expires_at and complete single/multi-leg shape on write and typed read
single-leg RiskResult leg_id != leg:{intent_id}:0, or parent ratio / SL differs by one f64 ULP → reject as invalid
approved risk-increasing RiskResult missing adjusted_legs or leg_id mismatch → no execution plan
ExecutionCommand.lots exactly equals approved RiskResult lots; parameter drift → re-risk / reject
Event WebSocket cannot publish execution.command or broker order
Execution WebSocket can carry execution.command only under Execution Client Protocol auth scope
Event WebSocket event cursor resume followed by GET /state calibration
Event WebSocket event replay window expired → resume_failed → GET /state calibration
Event WebSocket replay limit exceeded → RESUME_FAILED/GAP_DETECTED response, then connection close; no later event delivery
event_stream_log account_id=NULL on market/risk/execution topic → SQLite CHECK and repository validation reject
account A subscription cannot observe account B event; empty account scope can observe only redacted global system/deadletter summaries
global TransportEvent summary omits message/metadata/raw payload/remote/session/message identity/parser detail; durable fact retains typed message type/schema/raw length evidence, while raw content remains deliberately absent
TransportAdapter reports delivery failure but cannot mutate ExecutionCommandState directly
GatewayInboundRouter / GatewayOutboundRouter stage tests: no risk / execution state mutation
Execution Client message registry rejects unknown type before business handler
transport.ack does not advance ExecutionCommandState
execution.command creation is stored once; delivery retry only adds command_delivery_attempts
intent / risk / plan / legs / commands / pristine states commit atomically; late conflict rolls back the complete workflow
command CAS and leg / plan lifecycle bundle CAS reject stale writers and never expose partial projection
SQLite migrations create unique idempotency indexes
SQLite migrations create CHECK constraints for all status / enum TEXT fields
trade_intents.action CHECK accepts BUY / SELL / CLOSE / HOLD and rejects unknown values
schema_migrations checksum mismatch refuses startup
State Store projection can be rebuilt from core_events for execution state
crate dependency check: store does not depend on gateway / http / execution
crate dependency check: risk / execution have no Compute Service or live position-sizing HTTP client
workspace Cargo.toml members match crate boundary
OutboundDeliveryPort implementation cannot mutate ExecutionCommandState directly
concurrent hello for one client/account/terminal leaves exactly one ACTIVE session; NULL terminal is a real route component
replacement, stale heartbeat and disconnect callbacks are fenced by exact session_id/revision and cannot affect the new session
session.accepted reserves outbound sequence 1; concurrent deliveries allocate unique increasing sequence values from 2; reconnect resets the counter
account-only or partial route with multiple eligible sessions → AmbiguousRoute and zero transport writes
expired / stale / clock-unhealthy / identity-mismatched / inflight-limited delivery → typed rejection and zero transport writes
outbox + delivery attempt preparation is atomic; transaction failure leaves neither row nor consumed sequence
PENDING → WRITE_STARTED is durable before transport I/O; startup recovery converts interrupted writes to UNCONFIRMED and never auto-replays them
transport backpressure / definite-not-written / uncertain write persist distinct attempt outcomes
ACCEPTED / DUPLICATE transport.ack only ACKs wire_outbox; REJECTED persists FAILED, releases transport-admission inflight and does not mutate lifecycle; command.received ACKs command attempt; late receipt wins over timeout without erasing rejection facts
execution.command and reconciliation.request both bind their durable subject to the selected session, sequence, outbox and attempt
durable inbound/resume Accepted commits complete canonical envelope/cursor as PENDING before transport ACK/session acceptance
durable admission duplicate requires identical authenticated identity and payload; message_id or (session_id, sequence) drift creates stable rejection
lease owner/revision mismatch or completion/failure at an expired lease cannot commit terminal durable admission state
crash recovery reclaims expired PROCESSING rows, but no noop production handler marks them HANDLED
HTTP /state reads projection only and never exposes raw wire_inbox
HTTP /state returns accounts=[] for zero visible accounts and an accounts array for one or many accounts
HTTP /state account-bound projections join by account_id, share one authorization scope, and use one SQLite read snapshot
HTTP auth missing → 401 UNAUTHORIZED
HTTP scope mismatch → 403 FORBIDDEN
HTTP idempotency key conflict → 409 IDEMPOTENCY_KEY_CONFLICT
time.sync request / response offset calculation
heartbeat reports clock_sync_status and effective_server_now
time sync high RTT sample discarded
time sync stale / unhealthy → TIME_SYNC_UNHEALTHY
client local wall clock drift does not affect expires_at
decision control plane stale time sync → no new TradeIntent
TradeIntent decision_timestamp / requested_at ordering invalid, or requested_at too old / future → TRADE_INTENT_TIME_INVALID
expired TradeIntent signal_expires_at → TRADE_INTENT_EXPIRED
internal server clock skew blocks command generation / dispatch
expires_at derivation = min(signal_expires_at, risk_result.valid_until, server_now_ms + max_command_ttl_ms)
Execution 不得延长 signal_expires_at / risk_result.valid_until
expired execution.command 不进入 command inbox，不下单
CANCEL command requires broker_order_id and may omit lots / order_type
max_inflight_commands 超限 → COMMAND_DISPATCH_BACKPRESSURE
command.received ACK 正常路径
command.received 丢失 → DELIVERY_UNCONFIRMED → reconciliation
reconciliation request command_ids=None retains full account/route scope; Some([]) / duplicate IDs fail; Some IDs are stably sorted
command_ids=None with terminal_id/client_id still covers only that route; route-restricted completion never advances account pending-command watermark
account-wide Completed advances pending-command watermark only when terminal_id/client_id are absent and command_scope_complete=true from one trusted Store read snapshot
CONNECTION_RESTORED / STATE_STORE_RESTORED create independent runs and do not regress advanced command lifecycle
reconciliation positions / orders are full sets; empty sets advance watermarks and remove prior rows atomically
reconciliation position / order row account or observed_at mismatch, or duplicate business key → reject result atomically
old single-row observation at or before full-set watermark cannot revive a removed position / order
new single-row observation after full-set watermark invalidates uniform full-set readiness until the next complete result
snapshot showing ORDER_SENT / FILLED / missing order does not advance command state or authorize retry
reconciliation result without authoritative command evidence → PendingEvidence, not FAILED / EXPIRED / retry
client unresolved despite authoritative Core state → PendingEvidence + finding; no automatic override
command already in MANUAL_RECONCILIATION_REQUIRED during result evaluation → PendingEvidence until explicit escalation evidence
Completed only means scoped delivery uncertainty is covered; targeted completion does not advance account pending-command watermark
missing result / unresolved result enters ManualRequired only with explicit timestamped evidence and non-empty reason
result commit with ManualRequired disposition is rejected; explicit manual API persists evidence and evaluation atomically
account missing from result does not advance account refresh; non-empty metadata alone does not prove full metadata refresh
reconciliation result / checkpoint / full-set replacement commit and rebuild are atomic and deterministic
重复 command_id / idempotency_key 不重复下单
idempotency_key conflict → execution.event FAILED
断线重连后重发 command.received / execution.event
partial fill → plan PARTIAL → RollbackPolicy
Redis unavailable → Trading Core fanout spool → Redis restored → flush
State Store unavailable → reject new TradeIntent with STATE_STORE_UNAVAILABLE
State Store restored → replay emergency spool before accepting new TradeIntent
accepted-but-not-command intent freezes and revalidates after State Store restored
spool duplicate flush 幂等
manual reconciliation required path
secret rotation ACTIVE / NEXT / RETIRED / REVOKED
clock skew detected
Control Plane /time sampling computes effective_trading_core_now_ms
Control Plane stale time sync blocks TradeIntent before POST
Circuit Breaker trigger blocks new TradeIntent with RISK_BLOCKED
Circuit Breaker OPEN still allows reconciliation / execution.event / snapshots
Circuit Breaker reset requires reconciliation and audit.event
New Execution Client session resets both direction sequences to 1
session.hello resume cursor includes previous_session_id / last message ids / pending_command_ids
resume cursor does not auto-replay execution.command
```

### Fake Execution Client 能力

```text
模拟正常成交
模拟拒单
模拟 broker timeout
模拟部分成交
模拟 ACK 丢失
模拟 socket 断线
模拟 time sync high RTT
模拟 time sync stale / unhealthy
模拟 client wall clock drift
模拟超大 frame / 非法 frame length
模拟 session identity mismatch
模拟 expired command
模拟 max_inflight_commands 超限
模拟重复 command
模拟 Redis 恢复后的事件补写
模拟 stale snapshot / stale symbol metadata
```

### MQL5 Execution Client 运行模型验证

```text
EA 长时间无 tick（超过 heartbeat_timeout_ms）→ OnTimer 仍完成 heartbeat / time sync / reconnect，session 活性不依赖 OnTick
market tick 突发 → OnTick 只更新 / 合并 market.tick / market.bar 并入队，不执行 socket I/O
inbound / outbound backlog 超过任一 pump 上限 → 本轮在 message / byte / duration 首个上限处停止，下一次 OnTimer 继续
fragmented frame 跨多个 OnTimer → 保留 partial frame，重组后只处理一次
socket 无数据或写阻塞 → OnTimer 在 max_pump_duration_ms 内返回，不调用 Sleep，不 busy loop
OnTradeTransaction 在断线期间到达 → 先持久化 broker 状态，重连且 time sync 恢复后幂等上报 execution.event / snapshot
callback instrumentation → 任意时刻最多一个 handler active，不存在后台 worker 推进 socket / journal / command state
OnDeinit 遇到未清空队列 → 有界退出并保留 durable journal，不无限 flush
```

### 验收标准

```text
所有协议 golden tests 通过
所有状态迁移符合 transition rules
所有 terminal state 不回退
所有执行相关事件进入长期审计 sink
没有任何路径绕过 Risk Layer 生成 execution.command
MQL5 callback / bounded pump tests 通过；无 tick、网络突发或 socket stall 不破坏连接活性、broker 事件持久性和 fail-closed 约束
```
