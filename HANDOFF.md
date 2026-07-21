# Sinan 实现交接文档

## 项目

Sinan 是一个多平台量化交易系统，覆盖研究、策略与决策控制、硬风控、执行、对账，以及券商和交易所适配器。

项目名称为 **Sinan**。具体的 Cargo 包名使用 `sinan-*` 前缀；`crates/` 下的内部目录不带前缀，例如 `types`、`protocol` 和 `core`。架构层名称保持与具体语言和框架无关。

## 权威设计

以下文档是唯一事实来源：

```text
docs/quant_trading_7_layer_target_architecture.md
```

实现前必须完整阅读该文档，尤其是第 4-7 节和第 23-24 节。如果代码与文档冲突，应停止实现并明确解决设计冲突，不得静默引入第三种行为。

## 当前状态

- 架构和实现规格已经完成记录，并与当前代码保持同步。
- 第一至第八里程碑已经完成。内部 crate 目录不带前缀，Cargo 包名仍使用 `sinan-*`。
- `sinan-types`、`sinan-protocol`、三个协议黄金样例、SQLite migration、repository 和 state-ingest projection 均已实现并通过测试。
- `sinan-store` 当前共有 30 张业务表，最高 migration 为 V0007，启动时同步并校验 `PRAGMA user_version = 7`：V0002 的 22 张基础表、V0003 的 Circuit Breaker journal、V0004 的 4 张 Reconciliation run / checkpoint / set-membership 表，以及 V0007 的 inbound admission、inbound rejection 和 session resume admission 表。V0005 为 session、wire outbox 和 delivery attempt 增加 revision/CAS、sequence high-water、WRITE_STARTED/UNCONFIRMED 和双 subject 持久化边界；V0006 按旧表 `rowid` 顺序回填 bounded event stream 的数据库单调 `stream_sequence`，并建立 topic/account/created-time 的 sequence 索引；V0007 增加完整 canonical envelope/cursor、stable rejection 和 lease/revision recovery。canonical JSON/hash、显式写事务、typed repository、授权 latest-state query 和原子 projection rebuild 均已实现。
- `sinan-risk` 已实现不可变 `RiskRequest` / policy / capacity / full-set watermark 领域模型、确定性 Decimal position sizing evaluator 和纯 circuit breaker 状态机。
- `sinan-execution` 已实现从 approved `RiskResult` 构建 plan / command、精确 lots 映射、command 状态机、leg / plan projector 和 recovery decision；`sinan-store` 已实现 intent / risk / plan / leg / command / initial state 的原子 workflow commit，以及 command、leg、plan projection 的 CAS 持久化边界。
- Circuit breaker 已有完整、带版本的 durable snapshot 和启动恢复 adapter；缺失、损坏或未知 snapshot 会创建并持久化新的 `OPEN` safety incident，已知 `recovery_epoch` 不会回退。
- `sinan-reconciliation` 已实现 transport-neutral request 生成、result 校验、`Completed / PendingEvidence` 评估和显式 evidence 驱动的 `ManualRequired` 升级；V0004 已实现 reconciliation run、checkpoint、position / order full-set 原子替换及旧逐行事实防复活。
- `sinan-gateway` 已实现 live/durable session fencing、heartbeat clock health、Execution-owned `OutboundDeliveryPort` adapter，以及共享同一协议状态机的 Native TCP / Execution WebSocket binding；client auth、resume handoff、durable inbound/resume admission adapter、lease recovery dispatcher、单 writer、有界资源/停机和真实 loopback 测试均已落地。`TransportEventPort` 的生产 adapter 会先写 system/deadletter durable fact，再发布脱敏的全局 bounded summary。当前 `TransportEvent` 类型尚不携带 raw frame、message type 或 schema evidence，因此 deadletter 的 `raw_payload / raw_payload_length / message_type / schema_version` 目前可能为空，不能宣称已经保存完整原始证据。
- `sinan-http` 已实现严格 JSON 的 `POST /trade-intents`、`GET /state`、`GET /time`、intent/command 查询和独立 `WS /events`；Bearer token 同时约束 scope 与账户范围，Event WS 支持 cursor-exclusive durable replay、replay→live high-water 去重和慢消费者 fail-closed。
- `sinan-core` 已组合 Control Plane、Event Stream Manager、Gateway durable persistence ports，并在开放 HTTP 前恢复 durable Circuit Breaker。当前二进制实际启动的是 HTTP/Event WS 与 event retention task；Native TCP / Execution WebSocket listener、`DurableRecoveryDispatcher` 及 handler-specific inbound business processor 尚未在 `main` 中启动，因此 V0007 的 `PENDING` admission 目前只具备可恢复持久化边界，不代表业务事实已经处理完成。
- 现有 MQL5 EA 仍位于 MetaTrader 工作区，不属于此 Rust 仓库。
- 当前工作区已通过 `cargo fmt --all --check`、`cargo check --workspace`、`cargo test --workspace` 和 `git diff --check`。

## 不可妥协的架构决策

1. Trading Core 是交易正确性边界。
2. Strategy & Decision Control Plane 提交的 `TradeIntent` 必须包含 `account_id` 和稳定的 `idempotency_key`；它不得直接创建或发送 `execution.command`。
3. Risk Engine 是 Trading Core 内部的硬门禁。
4. Execution Client Protocol 与传输方式无关。
5. Native TCP 和 Execution WebSocket 是 Execution Client Protocol 的传输绑定。
6. Event WebSocket（`/events`）是独立通道，不得承载 `execution.command`。
7. SQLite 中只追加的事实和 projection 是本地执行状态的权威来源。
8. `ExecutionEvent` 是执行事实来源；command、leg 和 plan 状态都只是 projection。
9. `execution.command` 必须按照第 23.1 节规定的固定签名字符串使用 HMAC-SHA256。
10. `transport.ack` 使用第 23.1 节定义的权威 payload；`transport.ack`、`command.received` 和 `execution.event` 语义不同，不得混用。
11. Trading Core 的服务器时间是权威时间。Execution Client 和 Control Plane 使用单调时钟维护时间偏移。
12. 对于已过期、未认证、状态陈旧、未完成对账或被风控阻断的工作，必须失败关闭（fail closed）。
13. WebSocket 事件缺口通过有界重放或 `GET /state` 恢复，事件缺口本身不表示执行失败。
14. Gateway transport 和 router 不得直接修改执行生命周期状态。
15. 每个 MQL5 Execution Client EA 都使用串行事件回调且没有后台 worker：`OnTimer` 独占有界网络泵的活性职责，`OnTick` 只采集并合并行情数据，`OnTradeTransaction` 必须先记录券商状态变化，再将报告加入队列。
16. `sinan-risk` 在本地以确定性方式拥有最终批准 lots。Compute 的 position sizing 只提供建议；实时硬风控不得依赖 Compute/HTTP，Execution 必须精确映射已批准 lots，不得重新计算。
17. `GET /state` 返回 `accounts: AccountSnapshot[]`；所有账户相关 projection 必须使用同一授权范围，并通过 `account_id` 关联。
18. `TradeIntent.action` 持久化为 `trade_intents.action`，不得改名为 `direction`。
19. `RiskRequest` 由 Trading Core 内可信本地 assembler 从同一一致性读快照组装后作为不可变完整输入交给 `sinan-risk`；Risk evaluator 不得自行补读 store 或调用网络服务。
20. position / order 必须携带账户级 full-set 水位，即使集合为空也不能省略；market 必须显式携带 `account_id`，所有风控输入必须与 intent 账户一致。
21. sizing 的 tick ceil、volume-step floor、预算、敞口和保证金比较必须在 Decimal 域完成；第一版不允许多空、相关性或对冲抵消 hard-risk budget。
22. Reconciliation snapshot / result 是 broker 状态观测，不是执行事实；不得据此推进 `ORDER_SENT / FILLED / FAILED / EXPIRED` 或自动 retry。执行状态只能由 typed dispatch / delivery / reconciliation evidence、`command.received`、`ExecutionEvent`、显式时间证据或显式人工证据通过 Execution 状态机推进。
23. `ReconciliationRequest.command_ids=None` 表示该账户及可选 `terminal_id / client_id` route 内的全量 command；`Some` 必须非空、唯一并稳定排序。账户级 `Completed` 只有 request route 完全不受限（`terminal_id=None && client_id=None`），并同时持久化 `command_scope_complete=true`，证明评估使用了同一可信 Store read snapshot 的全账户 command scope，才可推进 pending-command 水位。`positions / orders` 是账户完整集合，空集合也形成水位，且每行 `observed_at` 必须等于 result 的 `observed_at`。
24. Gateway route 的可选 `client_id / terminal_id` 是筛选条件；匹配到多个 active session 时必须返回 `AmbiguousRoute`，不得任选一个。新 session 必须原子 stale 同 route 的旧 session，旧 callback 只能按精确 `session_id / revision` 操作。
25. `wire_outbox.ACKED` 只表示 `transport.ack`；`command_delivery_attempts.ACKED` 只表示已验证的 `command.received`。transport write 的崩溃窗口使用 `WRITE_STARTED`，timeout / disconnect / 不确定 write 使用 attempt `UNCONFIRMED`；Gateway 只报告这些 outcome，不推进 `ExecutionCommandState`。
26. `InboundAdmission::Accepted / Duplicate / Rejected` 都是 ACK 前的 durable decision 承诺：Accepted 必须 crash-recoverable，Duplicate 必须匹配相同 durable identity/payload，Rejected 必须持久化稳定 typed reason；只进入内存队列不得发送 `transport.ack`。admission error / timeout 不发 ACK，并关闭当前 session。
27. `session.accepted` 固定占用 outbound sequence 1；control message、execution command 和 reconciliation request 共用 durable session sequence。单 writer 必须按 sequence 输出；显式无 wire 结果使用 skip，未解释的 gap、overflow 或 backpressure 必须 fail closed。
28. Execution Client client-auth secret 只能接受匹配 identity 的 ACTIVE / NEXT；token 和配置 secret 不得进入 Debug 或日志。非空 resume cursor 必须在 accepted 前交给 durable resume handler，且不得触发 command 自动重放。
29. `heartbeat_timeout / time_sync_interval / max_time_sync_rtt / max_clock_offset` 的下发值与 session route gate 必须使用同一 policy；高 RTT sample 只丢弃 sample，不能丢失合法 heartbeat liveness。
30. 明文 Execution Client transport 只允许 loopback 或具备明确隔离的受控私网；跨主机或跨安全边界必须使用 TLS，服务间优先使用 mTLS。
31. `event_stream_log.account_id = NULL` 只允许 `system.event` / `deadletter.summary`；其他 topic 必须绑定非空账户。全局 summary 对所有合法 event subscriber 可见，只能包含经过白名单筛选的非敏感字段；当前 system summary payload 只含 `severity / component / timestamp`，deadletter summary payload 只含 `reason / received_at`。message、metadata、raw payload、remote/session/message identity 和 parser detail 不得进入全局 summary；其中由当前 transport event 实际携带的诊断字段只保存在受限 durable fact 中。
32. durable admission 的 `Accepted` 只证明完整 envelope/cursor 已 crash-recoverable；只有 handler-specific 领域事实与 projection 在 owner transaction 中提交后才能标记 `HANDLED`。不得用 noop handler 或多个松散 Store 调用伪造业务处理完成。

## Rust 工作区目标

工作区结构如下：

```text
sinan/
  Cargo.toml
  crates/
    types/           # 包名：sinan-types
    protocol/        # 包名：sinan-protocol
    domain/          # 包名：sinan-domain
    store/           # 包名：sinan-store
    gateway/         # 包名：sinan-gateway
    risk/            # 包名：sinan-risk
    execution/       # 包名：sinan-execution
    reconciliation/  # 包名：sinan-reconciliation
    events/          # 包名：sinan-events
    http/            # 包名：sinan-http
    core/            # 包名：sinan-core
  docs/
    quant_trading_7_layer_target_architecture.md
  tests/
    golden/
      execution_client_protocol/
```

`sinan-core` 是二进制组合根，其余 crate 均为库。

依赖方向：

```text
sinan-core
  -> gateway / http
  -> risk / execution / reconciliation / events
  -> store
  -> domain
  -> types
```

`sinan-protocol` 只能依赖 `sinan-types` 和协议级库，不得依赖 store、gateway、risk 或 execution。

## 第一里程碑（已完成）

第一里程碑只实现以下基础能力：

1. 创建 Cargo 工作区和所有 crate 目录。
2. 实现 `sinan-types`：
   - 共享 ID 和 newtype；
   - `ErrorCode`；
   - execution、session 和 storage 状态枚举；
   - 协议所需的通用 DTO。
3. 实现 `sinan-protocol`：
   - `ExecutionClientMessageType`；
   - 泛型 `WireMessage<T>`；
   - `ecp.v<major>.<minor>` 解析和兼容性检查；
   - envelope 验证；
   - HMAC 签名字符串生成和验证；
   - Native TCP framing codec；
   - 与传输方式无关的 payload 类型。
4. 将第 23.1 节的三个黄金 JSON 文件落入仓库。
5. 测试文档规定的 HMAC 向量：

```text
secret: test_command_secret_v1
expected: 044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e
```

6. 实现 `sinan-store` migration 基础设施和初始 `schema_migrations` migration。
7. 仅在工作区编译需要时，为其余 crate 添加公共占位 API。

此里程碑不得实现 TCP listener、WebSocket server、HTTP endpoint、Risk Engine policy、Execution Engine 行为或 MQL5 集成。

## 第一里程碑验收标准

只有以下命令全部通过，第一里程碑才算完成：

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
```

必需测试：

- `WireMessage` JSON 往返序列化。
- 拒绝未知消息类型。
- 拒绝 schema major 版本不匹配。
- 接受兼容的更高 minor 版本。
- 解析黄金 JSON。
- 精确匹配 HMAC 黄金向量。
- 缺失的可选签名字段映射为空字符串。
- 固定小数格式保留要求的末尾零。
- Native TCP 长度前缀的拆包和粘包处理。
- 拒绝超大 frame。
- 拒绝 migration 校验和不匹配。

## 第二里程碑（已完成）

第二里程碑实现 SQLite repository 和 state-ingest projection，不提前实现 Risk、Execution 或 Gateway 状态机：

1. 新增 `V0002__state_store_schema.sql`：
   - 创建第 23.2 节规定的 22 张业务表；
   - 所有 status / action / mode / platform / message type 使用 `CHECK`；
   - 所有 `payload_json` 配套 canonical JSON SHA-256 `payload_hash`；
   - `execution_plans` / `execution_commands` 持久化 `risk_id`；
   - `execution_events.execution_id` 为主键，`command_id` 必填且不引用 projection；
   - 启用复合外键、查询索引和不可变事实 trigger；
   - `event_stream_log` entry 禁止更新，但允许 bounded retention 删除。
2. 实现 `SqliteStateStore` / `StoreOptions`：
   - 统一启用 WAL、foreign keys 和 busy timeout；
   - 自动执行 forward-only migration；
   - 不公开绕过连接配置与 migration 校验的 unchecked pool 构造路径；
   - 写事务使用 `BEGIN IMMEDIATE`，避免 read-then-write 的 `SQLITE_BUSY_SNAPSHOT`。
3. 实现 `CanonicalJson` 和小写 SHA-256 hash，递归稳定排序 object key；该格式只用于存储和幂等检测，不用于 command HMAC。
4. 实现 typed repository：
   - core event、TradeIntent、ExecutionCommand、ExecutionEvent；
   - wire inbox / outbox、session record；
   - `ExecutionCommandState` insert / read / compare-and-swap 持久化原语；
   - command-state CAS 同时匹配预期 status 和 `updated_at`；目标 `updated_at` 必须严格递增，已成功写入的完全相同请求可幂等重试；
   - 初始 command-state 重放遇到同 immutable identity 的更高版本 projection 时保留现值并返回 `Duplicate`；同版本内容漂移或通过 insert 提交更高版本仍返回 conflict；
   - 同主键和幂等键、同 payload 返回 `Duplicate`，任一唯一键复用不同 payload 返回稳定 conflict；
   - typed read 校验 canonical hash 和 JSON / denormalized column 一致性。
5. 实现 handler-specific 原子 ingest：
   - account / position / order snapshot；
   - symbol metadata；
   - market bar；
   - latest-only market snapshot；
   - 重复 core fact 仍基于数据库中的原始事实幂等执行 projection apply，可修复缺失 projection，最终返回 `Duplicate`。
6. latest projection 只允许更大的 `observed_at` 覆盖；更旧事实只追加；相同业务键和时间、不同 payload 返回 `ObservationConflict`。
7. `AuthorizedAccountScope` 必须显式传入；空 scope 返回空集合。多表状态通过同一 SQLite read transaction 读取并稳定排序，market row 通过 `AccountMarketSnapshot` 保留账户归属。
8. `rebuild_ingest_projections` 在单一 write transaction 中重放 account / symbol / position / order / market bar；V0004 落地后，该入口会在保留原 ingest report 计数口径的同时继续执行 reconciliation projection rebuild，保证 full-set-only 成员、membership 和 checkpoint 不会被 standalone ingest rebuild 丢失。失败整体回滚，不修改 tick-only `market_snapshots`。

第二里程碑明确不包含：

- `RiskResult` / execution plan / leg 的 typed commit bundle；相关 schema 已就绪，类型和原子 workflow 由 Risk / Execution 里程碑补齐。
- `ExecutionCommandState`、leg 和 plan 的业务状态转换及 lifecycle rebuild；store 只提供 CAS 持久化原语。
- position tombstone 或账户级 full-set replacement；当前只保留每个 `position_id` 的最新已知 observation，删除语义由 Reconciliation 里程碑实现。
- session replacement / heartbeat registry、wire 状态迁移、delivery attempt 状态机。
- HTTP、WebSocket、TCP listener 或任何外部服务。

## 第二里程碑验收标准

当前工作区已通过：

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
git diff --check
```

相关测试覆盖 migration 升级和校验、22 表 schema、FK / CHECK / trigger、生产连接初始化、canonical JSON、双键幂等、多连接并发 CAS、command-state 创建重放、重复事实补投影、事务回滚、授权查询、latest 冲突、冗余列损坏检测以及原子 rebuild 回滚。

## 第三里程碑（已完成）

第三里程碑只实现 Risk 与 circuit breaker 的纯领域逻辑和 `RiskResult` 持久化原语，不进入 Execution plan / command 状态机、Gateway 或 HTTP：

1. 已定义完整不可变 `RiskRequest`：
   - 固定 `risk_id` 和服务器时间域 `evaluated_at`；
   - position / order 使用账户级 full-set watermarks，空集合也必须有新鲜证据，每一行必须属于声明的 full-set 水位；
   - `pending_commands_reconciled_at` 约束 account / position / order / command-state 的因果水位；
   - market snapshot 显式携带 `account_id`；
   - `RiskCapacity` 绑定 `account_id / strategy_id`，并提供日内亏损、回撤、剩余账户 / 组合风险容量和 `remaining_strategy_legs`。
2. 已定义 `RiskResult` / `AdjustedRiskLeg` 共享 DTO，并为 `risk_results` 增加 typed insert/get：
   - payload 自包含 request / intent identity、`risk_request_hash`、candidate provenance、market / metadata / capacity age 和最终 lots；
   - `RiskResult::validate` 拒绝 approved / rejected / no-op 的非法字段组合、非 finite 数值、非法时间、重复或错配 leg 以及风险算术漂移；
   - 完整 payload 使用 canonical JSON/hash 持久化；
   - typed read 校验 hash、冗余列以及父 intent 的 account / decision / action / signal 时间契约，并重建父 canonical payload；
   - actionable approval 完整绑定父 intent 的腿 shape：单腿使用共享的 `leg:{intent_id}:0`，并精确匹配 symbol / action / ratio=1 / proposed_sl；多腿按 `leg_id` 一一匹配 symbol / action / ratio / proposed_sl，ratio 和 SL 按原始 `f64` 位模式比较；
   - 相同 `risk_id` 和完整 identity/payload 可幂等重放，漂移返回 conflict；
   - 同一 intent 允许通过不同 `risk_id` 追加多次评估。
3. 已加入 pure circuit breaker 状态机：
   - `CLOSED / OPEN / HALF_OPEN`、文档规定的触发源和 action gate；
   - OPEN / HALF_OPEN 阻断风险增加动作，但继续允许状态读取、snapshot / execution event ingest、对账、人工审查、no-op 和已证明的风险降低动作；
   - OPEN 只能先进入 HALF_OPEN；refresh / reconciliation 恢复证据必须携带完成时间且 `>= triggered_at`；
   - OPEN 期间完全相同的 violation observation 保持幂等；健康 observation 只产生 `IncidentEvidenceCleared` 并保持 OPEN，清除旧 fingerprint，之后相同 violation 复发也会推进 `triggered_at` 和 recovery epoch，使旧恢复证据失效；
   - 进入 HALF_OPEN 时记录日内亏损和回撤 baseline；原 incident 的财务阈值可暂时仍被触发，但观察期内任一财务值高于 baseline 都会重新 OPEN，即使新值低于 policy threshold；
   - safety fallback fingerprint 保留具体 `CircuitBreakerError`，相同错误幂等，不同错误开启新 recovery epoch；
   - 非法 policy、输入或服务器时间回退 fail closed；manual reset 产出完整 audit record。
4. 已实现 pure deterministic evaluator：
   - 当前唯一支持 `fixed-risk-at-stop.v1`，未知 sizing version fail closed，不允许标签与实际算法漂移；
   - 所有 sizing 输入先以 base-10 文本转换到 Decimal，tick ceil、volume-step floor、预算、敞口和保证金比较全程使用 Decimal；
   - HOLD 返回 approved no-op，所有 sizing 字段为空且不得创建 plan / command；
   - 当前 CLOSE 缺少目标 position 和 close lots，必须返回 `RISK_REDUCTION_NOT_PROVABLE`；
   - BUY / SELL 单腿 ratio 必须为 `1`，多腿按 `leg_id` 唯一匹配，不做多空、相关性或对冲抵消；
   - 敞口按 `ceil(abs(conservative_price) / tick_size) * tick_value_loss` 的账户币种近似累加；
   - 有效 pending order / command 保守分别累加，不依据方向或推测关联做抵消；
   - active order / BUY-SELL command 的可选 `broker_symbol` 一旦存在，必须与 canonical symbol metadata 精确绑定；
   - 第一版无法证明 pending MODIFY 只降低风险，任何 non-terminal MODIFY 都阻断新的风险增加 intent；
   - `margin_initial > 0`，且同时满足 free-margin 和 `max_margin_usage_pct` 上限；
   - Decimal lots 转为共享 `f64` DTO 后必须经 base-10 round-trip 复核，不得向上舍入或破坏 `volume_step`；
   - `valid_until` 取 approval TTL、signal expiry、command reconciliation 和全部 snapshot / market / metadata freshness 边界的最小值；
   - evaluator 返回 `Result<RiskResult, RiskEvaluationError>`：业务拒绝仍是可审计的 rejected `RiskResult`，只有无法构造合法审计身份或合法 fail-closed 结果时返回错误。

第三里程碑的 trusted assembler 契约已经确定，但从 State Store 单一一致性读快照加载并组装 `RiskRequest` 的 application service 属于后续 Execution 集成范围，本里程碑不为 `sinan-risk` 增加 store 依赖。

Pure circuit breaker 仍不依赖 Store。完整 breaker state 的持久化格式和 application restore adapter 已在第四里程碑落地，见下节；面向 `GET /state` 的摘要 DTO 仍不得作为 durable snapshot。

第三里程碑验收已经通过：

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
git diff --check
```

相关测试覆盖单腿 / 多腿、cost buffer、volume-step floor、volume / exposure / margin 全局缩放、风险预算单调性、freshness、跨账户输入、pending command、HOLD / CLOSE、breaker gate、recovery epoch、`RiskResult` repository 集成以及共享 DTO 语义校验。

## 第四里程碑（已完成）

第四里程碑完成 Circuit Breaker durable restore 和 Execution 领域 / 持久化边界，不实现 Gateway transport 或真实投递：

1. `sinan-execution` 已实现 pure execution builder：
   - 只接受仍在有效期内、语义合法的 approved `RiskResult`；
   - `ExecutionLeg.lots` 和 `ExecutionCommand.lots` 按 `leg_id` 精确复制已批准 lots，不做 sizing 重算或隐式修正；
   - command expiry 取 signal、risk approval 和 execution TTL 的最小值；identity、route、订单参数或审批 provenance 漂移时拒绝构建；
   - HOLD approved no-op 不生成 plan / command，第一版 CLOSE 仍按 Risk 的 fail-closed 口径处理。
2. 已实现 command lifecycle 和 leg / plan projection：
   - delivery outcome、`command.received`、`ExecutionEvent`、显式 reconciliation / manual evidence 各自通过 typed state machine 推进；
   - snapshot、transport ack 和到达顺序都不能制造执行事实；
   - identity、时间单调性、terminal lifecycle、plan / leg 派生状态和跨账户 / 跨计划关系均在领域边界校验。
3. `sinan-store` V0003 已实现 Execution durability：
   - plan / leg definition 不可变；typed read 会校验 canonical payload、冗余列和父图关系；
   - `commit_execution_workflow` 在同一 write transaction 中原子写入 TradeIntent、RiskResult、plan / legs、commands 和 pristine command states，完整重放幂等，任一父图或 payload 漂移冲突，失败整体回滚；
   - command state 使用 identity + expected status + expected `updated_at` CAS；leg / plan lifecycle 作为一致 bundle 使用 CAS，不能留下部分 projection。
4. 已实现 Circuit Breaker durable restore：
   - V0003 使用 append-only、revisioned snapshot，持久化完整 `OPEN / HALF_OPEN` 状态、fingerprint、recovery epoch、clear / half-open 时间、financial baseline 和 blocked count；
   - 启动时先读可信的 denormalized head metadata，再校验版本化 snapshot；缺失、损坏或未知版本会生成并持久化 `OPEN` safety state；
   - 损坏 payload 仍从已知最高 `recovery_epoch + 1` 开启新 incident，不会重置为 1；revision CAS 冲突会有界重读；Store 不可用或 epoch 溢出时返回 fail-closed outcome，调用方不得继续 live flow。

第四里程碑没有接入 live Risk→Execution application flow。后续 HTTP / Core 组合层接入时，仍必须从 State Store 单一一致性读快照组装 `RiskRequest`，并复用原子 workflow commit；不能在 `sinan-risk` 内增加 Store、HTTP 或 Compute 依赖。

## 第五里程碑（已完成）

第五里程碑完成 Reconciliation 的 pure domain 和 V0004 State Store，不实现 Gateway、socket、wire session 或真实 `reconciliation.request` 投递：

1. `sinan-reconciliation` 已实现 request planning：
   - `command_ids=None` 是账户及可选 `terminal_id / client_id` route 内的全量 scope；`Some` 是 targeted scope，必须非空、唯一并按 command ID 稳定排序；
   - `None` 只定义请求范围，不自行证明 application 提供了完整 command 集合；只有 route 完全不受限的账户级评估才能以同一可信 Store read snapshot 组装全账户 command scope，并通过 `command_scope_complete` 显式记录该证据；
   - `CONNECTION_RESTORED` 和 `STATE_STORE_RESTORED` 每次创建独立 `request_id` / run；已进入更后生命周期的 command 不会为对账而倒退。
2. 已实现严格 result 校验与纯评估：
   - request、账户、terminal / client route 和服务器时间必须一致；
   - `positions / orders` 是 `result.observed_at` 时刻的账户完整集合，空数组也合法；每行必须属于该账户且 `observed_at == result.observed_at`，positions / orders / metadata / unresolved IDs 必须按业务键唯一并稳定排序；
   - snapshot / `unresolved_command_ids` 只产生 finding，不产生 `ORDER_SENT / FILLED / FAILED / EXPIRED` 事实，也不授权 retry；与 `ExecutionEvent` 冲突时以 event 为执行事实，但客户端 unresolved 差异仍保持 `PendingEvidence`，不能被 Core 的权威状态自动覆盖。
3. Reconciliation workflow disposition 只有 `Completed / PendingEvidence / ManualRequired`；普通 result evaluation 只产生前两者，`ManualRequired` 只由显式升级产生：
   - 不确定 command 缺少权威 `command.received` / `ExecutionEvent` projection 时保持 `PendingEvidence`；
   - 本次 result 中任何 unresolved command，或已经处于 `MANUAL_RECONCILIATION_REQUIRED` 的 command，都先保持 `PendingEvidence + finding`；result evaluation 不自动产出 durable manual state；
   - `ManualRequired` 只能由带服务器时间和非空原因的显式人工 / 超时证据触发；缺失 result 也只能通过显式调用升级，没有隐式 timer；
   - `Completed` 仅表示该 scope 的投递不确定性已有权威 execution state 覆盖，且本次没有 unresolved / manual finding；不表示订单已经 terminal。
4. `sinan-store` V0004 已实现 durable reconciliation：
   - `request_id` 是 run identity；request definition 不可变，run status 为 `REQUESTED / PENDING_EVIDENCE / COMPLETED / MANUAL_RECONCILIATION_REQUIRED`；
   - 普通 result commit 只接受 `Completed / PendingEvidence`；`Completed` 当且仅当 attention command 集合为空，`PendingEvidence` 必须非空，所有 client unresolved command 都必须保留在 attention 集合中；`ManualRequired` 必须走独立显式升级 API 并持久化 timestamp、非空 reason 和 manual evaluation，Completed run 不能再升级；
   - result、run、account checkpoint 和 position / order full-set replacement 在同一 transaction 中提交，失败整体回滚；
   - full-set 水位、集合 hash 和 set-membership projection 保留空集合删除语义；checkpoint 的两个集合 hash 必须同时锚定到同一个 durable result fact。延迟到达且 `observed_at < full-set watermark` 的旧逐行事实不能复活已删除行；同水位 single-row 与 full-set 缺键或 payload 不同必须 fail closed 并回滚第二个事实，在线与 rebuild 不依赖到达顺序；更新事实若新于水位则使集合失去一致 full-set 证据，Risk 继续 fail closed；
   - 只有 `command_ids=None`、`terminal_id=None`、`client_id=None`、`Completed` 且 `command_scope_complete=true` 才推进账户级 `pending_commands_reconciled_at`；targeted request 不得声明 scope complete，route-restricted completion 或未证明 command scope 完整时都不推进。该 completeness 是可信 Core assembler 基于同一 Store read snapshot 产生的内部 attestation，不来自 wire client。缺失 `account` 不推进 account refresh，非空 metadata 数组本身也不能证明完整 metadata refresh；只有上层显式提供并持久化 `symbol_metadata_complete=true` 时才可推进 metadata readiness；
   - `reconciliation.result` 参与确定性 projection rebuild，重建规则与在线 full-set replacement 一致。

本里程碑只持久化 transport-neutral request / run，不写 `wire_outbox`，也不实现 Gateway session / delivery attempt。Gateway 里程碑负责把已持久化 request 绑定到 active session 和 wire envelope；它仍不得拥有对账或 execution lifecycle 决策。

## 第六里程碑（已完成）

第六里程碑完成 Gateway session registry、V0005 delivery durability 和 transport-neutral 出站投递端口，不实现 Native TCP / Execution WebSocket listener 或 inbound router：

1. `sinan-execution` 定义 object-safe `OutboundDeliveryPort`：
   - `DeliveryOutcome` 明确区分 `Sent / Rejected / DefinitelyNotWritten / Unconfirmed`；
   - Gateway 只返回 delivery evidence，不修改 `ExecutionCommandState`，不拥有 retry 或 reconciliation evaluation；
   - `Sent` 只在真实 transport write 接受完整 envelope 后返回，进入 SQLite 或未执行的用户态队列不构成发送证据。
2. `sinan-gateway` 实现 session registry：
   - 同 route activation 通过共享锁串行化 live fence、durable replacement 和 live publish；disconnect / startup fence 使用同一把锁，旧 callback 不能移除 replacement epoch；
   - registration 在 fence 前完整校验 identity、capabilities 和 `max_inflight_commands`；heartbeat 每次持久化 freshness 与有效 clock status，无效时间证据先落 `UNSYNCED` 再返回错误；
   - process-local handle 以原子 write admission 与 fence 线性化，replacement 与已准入 write 并发时结果按 transport 证据收敛。
3. Gateway outbound adapter 实现 durable bind/write/replay：
   - route 解析、heartbeat/clock/expiry 重验、inflight 检查、sequence reserve、outbox 和 attempt insert 在同一 `BEGIN IMMEDIATE` transaction 中完成；transport I/O 在事务提交之后执行；
   - `session.accepted` 占用 sequence 1，后续从 2 开始；并发 reserve 唯一单调，V4→V5 升级从历史 outbox 回填 high-water，升级后下一次 reserve 为 `MAX+1`；
   - outbox payload 必须与 durable command / reconciliation parent payload 精确一致；尚未生成 outbox 的 rejection 保存完整 draft canonical JSON/hash，同 message ID 的 subject、route、envelope 或 payload 漂移 fail closed；
   - `PENDING → WRITE_STARTED` 在 write 前持久化，crash/timeout/disconnect/uncertain write 收敛为 `UNCONFIRMED` 且禁止自动重放；backpressure、确定未写入和不确定写入保持不同 outcome。
4. ACK 与 inflight 语义已固定：
   - ACCEPTED / DUPLICATE `transport.ack` 只推进 `wire_outbox.ACKED`，不修改 attempt 或 execution lifecycle；
   - REJECTED `transport.ack` 保留 `wire_outbox.FAILED` 和 reason，结束 transport-admission inflight，但不把 attempt 置为 `ACKED`；并发 write completion 可把 `PENDING` 收敛为 `SENT`，已有独立证据保持不变；late `command.received` 仍可推进 attempt `ACKED`，不得抹掉 rejection fact；
   - 只有已验证的 `command.received` 推进 attempt `ACKED`；timeout/disconnect 不得覆盖更强证据。

第六里程碑的 Gateway 测试覆盖 concurrent activation、activation/disconnect、startup fence、invalid heartbeat、sequence 并发、normal/replay/backpressure/failure/unconfirmed、replacement/write admission、ACK/receipt/timeout/disconnect 竞态和 payload drift。第七里程碑在此基线上完成真实 transport binding。

## 第七里程碑（已完成）

第七里程碑完成 Native TCP 与 Execution WebSocket transport binding，并复用第六里程碑的 session/outbound durability，不在 transport 层引入 execution lifecycle、retry 或 reconciliation 决策：

1. 共享连接与认证状态机：
   - `ConfiguredClientAuthenticator` 按 client/account/terminal/platform/remote identity 绑定凭证，只接受 ACTIVE / NEXT client-auth secret，Debug 输出脱敏；
   - 首条数据消息必须是合法 `session.hello`，认证成功后分配新 session；`session.accepted` 真实写成功后才进入 active message loop；
   - `GatewayConnectionService` 同时限制两个 binding 的总 connection 和 pending handshake，并强制 session clock policy 与 hello 下发 policy 一致。
2. 两种真实 transport binding：
   - Native TCP 使用 4-byte unsigned big-endian length prefix，支持拆包、粘包和多 frame；零长度、超限、不完整、非 UTF-8/JSON frame 均立即 fail closed；
   - Execution WebSocket 固定使用 `/execution-client`，一个 Text message 对应一个 `WireMessage`；Binary/raw/超限消息拒绝，且与后续 `/events` endpoint 隔离；
   - public stream adapter 和 listener path 都受共享 semaphore 约束；握手使用统一 deadline，shutdown 具有 grace bound、abort/drain fallback，完成任务优先回收。
   - `inbound_admission_timeout` 强制短于总 `handshake_timeout`（当前默认 2 秒 / 5 秒），使及时 hello 的内部 resume timeout 分支可在总 deadline 前到达；`session.rejected` 在剩余写预算内 best-effort 返回，总 deadline 始终优先 fail closed。
3. 单 writer 与 outbound ordering：
   - 每个 connection 只有一个 writer，bootstrap、control 和 durable business frame 共用有界 admission；oneshot completion 只在实际 transport write 后返回证据；
   - `session.accepted` 固定 sequence 1；后续 control / command / reconciliation 共用 `last_outbound_sequence`，支持有界乱序重排、显式 skip 和 gap timeout；
   - control message 使用 deferred materialization，在 concrete writer 即将写入前生成 `sent_at`；`time.sync.response.server_send_at == server_time == envelope.sent_at`；encode/size/materialization 失败关闭 writer。
4. Inbound、time 和 resume 边界：
   - 两个 binding 共用 direction allowlist、递归 payload identity 校验、session sequence 和 `InboundMessagePort`；只有 durable `Accepted / Duplicate / Rejected` admission 后才发送 transport ACK；
   - admission error/timeout 写 `InboundAdmissionFailed`、不发 ACK 并断开；transport adapter 不修改 `ExecutionCommandState`；
   - heartbeat 将结构非法时间证据降为 UNSYNCED 但不刷新 durable heartbeat；缺失/高 RTT sample 被丢弃，合法 heartbeat 仍刷新 liveness；sent_at/effective time skew 和 clock health transition 产出 typed transport event；
   - 非空 resume cursor 在 accepted 前完整交给 `SessionResumePort`；未配置 durable handler、失败或 timeout 拒绝握手，transport 不自动 replay command。
5. Session 与 Store 生命周期收口：
   - control sequence 与 business delivery 使用同一 durable cursor；exact session close 在单个 `BEGIN IMMEDIATE` 事务中读取最新 revision 后关闭，避免与 sequence reserve 竞争时 CAS 饥饿；
   - replacement 会真实关闭旧 socket；connection/server task cancellation、bootstrap cancellation 和 commit-uncertain activation 都有 RAII cleanup，最终 exact-disconnect durable epoch；
   - writer queue、reorder buffer、write、handshake、admission、event write、connection task 和 shutdown 都有明确资源或时间上限。

本里程碑的 Gateway 单元测试覆盖 auth、identity、clock policy、deferred writer、乱序/skip/gap、shutdown fallback 和原有 outbound durability；Store 测试覆盖 control/business cursor 及 exact-close 并发/幂等。真实 loopback 测试覆盖 Native TCP 拆包/粘包、非法 frame、认证拒绝、replacement、resume 成功/失败/timeout、clock event、durable admission 完成前无 ACK、Duplicate/typed Rejected ACK、admission error/timeout、activation/connection cancellation 和 handshake deadline，以及 Execution WebSocket path、Text/Binary、超限和认证行为。

本里程碑只定义 production inbound durability、resume durability 和 transport event 的强制 port contract，并使用测试 adapter 验证 transport 行为；尚未新增 production wire inbox/spool handler、TransportEvent/deadletter persistence，也未组合真实 Execution/Reconciliation inbound dispatcher。TLS/mTLS termination、HTTP API 和 Event WebSocket 同样不在本里程碑内。下一里程碑必须在 composition/application 层履行这些 durable contract，不能把测试中的内存 recording/noop adapter 当成生产实现。

## 第八里程碑（已完成）

第八里程碑完成 Control Plane HTTP、独立 Event WebSocket、V0006/V0007 durability 以及对应的 production persistence adapter；它建立可靠 intake/query/event 边界，但不宣称已经完成 Risk→Execution workflow 或 Execution Client inbound 业务处理：

1. Control Plane HTTP：
   - `POST /trade-intents` 使用严格 JSON、`X-Request-ID`、`X-Idempotency-Key`、Bearer scope 和账户授权；intake 只持久化合法 TradeIntent，返回 `202 ACCEPTED` 或幂等的 `200 DUPLICATE`，不伪造 RiskResult、plan 或 command；
   - BUY/SELL 的 `proposed_risk_pct` 必须在 `(0, 100]`，HOLD 必须为 `0` 且不得携带 SL/TP/legs；过旧/未来 `requested_at` 返回 `TRADE_INTENT_TIME_INVALID`，已到期 signal 返回 `TRADE_INTENT_EXPIRED`；
   - `GET /state` 从同一 SQLite read snapshot 返回授权范围内的 `accounts: AccountSnapshot[]` 及 bounded execution/risk 集合；有时间语义的 bounded 集合先倒序取上限，再按 `(time, id)` 升序输出；
   - `GET /time` 返回 receive/send server timestamps 和 Control Plane time policy；`GET /trade-intents/{intent_id}` 与 `GET /execution/commands/{command_id}` 不泄露范围外对象，command payload 需要额外 debug-sensitive scope；
   - 已持久化的 rejected RiskResult 在 intent detail 中派生为 `RISK_BLOCKED`，并把 `evaluated_at` 纳入 `updated_at`。
2. Event Stream 与 Event WebSocket：
   - V0006 为 `event_stream_log` 增加数据库分配的单调 `stream_sequence`；Event Stream Manager 坚持先 durable append 再 live fanout，并用“先建 receiver、读取 high-water、cursor-exclusive replay、丢弃 high-water 内 live duplicate”的顺序跨越 replay/live 边界；
   - `/events` 只接受 UTF-8 Text JSON 的 `subscribe / unsubscribe / ping`，与 `/execution-client` 完全分离；topic、账户缩窄和 cursor 都经过授权；
   - replay 超限返回 `RESUME_FAILED / GAP_DETECTED` 后以 1013 关闭；cursor 过期会返回 `RESUME_FAILED / CURSOR_EXPIRED` 且不建立 subscription，broadcast lag、write timeout、非法/Binary/超限 frame 则关闭连接。调用方必须以最后实际收到的 event ID 或 `GET /state` 恢复；
   - 只有 `system.event` / `deadletter.summary` 可以使用空账户形成全局事件；当前 system summary payload 白名单为 `severity / component / timestamp`，deadletter summary payload 白名单为 `reason / received_at`，不包含 diagnostic message/metadata、raw evidence、remote/session/message identity 或 parser detail。
3. Durable inbound/resume 与 TransportEvent：
   - V0007 新增 `inbound_admissions`、`inbound_rejections`、`session_resume_admissions`；保存完整 canonical envelope/cursor、authenticated route、sequence、hash、状态、revision 和 lease；
   - production admission port 在 durable `PENDING` 后才允许 ACK，稳定 duplicate/rejection 可安全重放；dispatcher 支持 claim、过期 lease reclaim、owner/revision/expiry fencing、handler timeout 和 terminal completion/failure；resume handler 不得自动重放 command；
   - production `TransportEventPort` 将 frame/decode/schema 问题写入 deadletter，其余认证/clock/liveness 问题写入 system fact；durable fact 成功后才发布 bounded summary，发布失败保持可观测错误。由于当前 `TransportEvent` 没有 raw frame、message type 和 schema evidence 字段，相关 deadletter 列仍可能为空；这是证据携带边界，不得写成完整 raw evidence 已落库。
4. Core 运行组合：
   - 启动时先连接并迁移 Store，再执行 durable Circuit Breaker restore；缺失/损坏 snapshot 会持久化 OPEN safety incident，restore 失败拒绝开放 HTTP；
   - 必填运行配置为 `SINAN_CONTROL_PLANE_TOKEN / SINAN_CONTROL_PLANE_ACCOUNTS`；后者的环境变量本身必须存在且非空，但逗号分隔、trim 和过滤后的账户集合允许为空，此时 principal 不能读写任何账户对象，其 Event WS 只能看到脱敏的全局 system/deadletter summary。可选配置为 `SINAN_CONTROL_PLANE_SUBJECT / SINAN_CONTROL_PLANE_SCOPES / SINAN_DATABASE_URL / SINAN_HTTP_ADDR / SINAN_EVENT_LIVE_CAPACITY / SINAN_EVENT_REPLAY_LIMIT / SINAN_EVENT_MAX_MESSAGE_BYTES / SINAN_EVENT_WRITE_TIMEOUT_MS / SINAN_EVENT_RETAIN_LATEST / SINAN_EVENT_RETENTION_AGE_MS / SINAN_EVENT_RETENTION_INTERVAL_MS`。当前默认值分别为 subject `control-plane`、三个 Control Plane/Event scope、`sqlite://sinan.sqlite`、`127.0.0.1:8080`、`1024 / 1000 / 65536 / 5000ms / 10000 / 900000ms / 60000ms`；所有容量、上限和 duration 都必须为正数；
   - 当前 `main` 只启动 Control Plane HTTP/Event WS 和 event retention。Gateway persistence ports 已组合并保活，但 Native TCP / Execution WebSocket listener 与 durable recovery worker 尚未启动。

第八里程碑明确没有把 V0007 admission 自动标记为 `HANDLED`。下一里程碑必须实现按 `command.received / execution.event / reconciliation.result` 分派的 handler-specific 原子业务事务、可信 RiskRequest assembler 与 Risk→Execution workflow processor，并把 Gateway listeners、session registry、outbound adapter 和 `DurableRecoveryDispatcher` 组合进进程生命周期；listener 投产前还必须补齐 TransportEvent 的有界 raw/type/schema 证据传递。完成这些边界前，不能把当前二进制描述为完整 production execution path。

## 实现约束

- 业务时间戳使用服务器时间域的 Unix 毫秒值。
- 单调时钟只能用于本地经过时间和 RTT。
- 在可行的情况下保持 payload DTO 不可变。
- 所有工作区 crate 使用仓库的 MIT 许可证。
- command HMAC 不得使用 JSON canonicalization。
- 第 23.1 节及其黄金向量是 HMAC 字段顺序的权威来源。
- `X-Idempotency-Key` 必须等于 `TradeIntent.idempotency_key`。
- 风险百分比 `1.0` 表示一个百分点（`1%`），不是比例值。
- 最终 position sizing 和 volume-step 比较使用十进制或定点整数运算。
- 实时 `sinan-risk` / position sizing 不得添加 Compute Service 或 HTTP client 依赖。
- transport 代码不得拥有重试决策或 command 生命周期决策。
- migration 只允许向前执行，并且必须验证校验和。
- 公共 API 必须保留现有架构术语。
- 没有明确需要时，不得增加依赖。

## 后续里程碑

基础能力稳定后，按以下顺序推进：

1. SQLite repository 和 projection。（已完成）
2. Risk 与 circuit breaker 领域逻辑。（已完成）
3. Circuit Breaker durable restore、Execution command 和状态机。（已完成）
4. Reconciliation 领域和 V0004 State Store。（已完成）
5. Gateway session registry 和出站投递端口。（已完成）
6. Native TCP 和 Execution WebSocket 绑定。（已完成）
7. HTTP TradeIntent/state/time API、Event WebSocket，以及 production inbound、TransportEvent/deadletter persistence adapter。（已完成）
8. handler-specific inbound 原子 dispatcher、Gateway listener 运行组合、TransportEvent 原始证据传递、可信 RiskRequest assembler 和 Risk→Execution workflow processor。（下一里程碑）
9. Fake Execution Client 端到端测试。
10. MQL5 和 OKX 适配器。
11. Strategy & Decision Control Plane。

Risk、Circuit Breaker durable restore、Execution、Reconciliation、Gateway session/outbound durability、两种 Execution Client transport binding、Control Plane HTTP、Event WebSocket 和 durable admission/persistence adapter 已完成。下一里程碑把这些现有边界组合成真实业务处理链：每类 inbound message 使用 owner transaction 提交事实与 projection，Risk→Execution 使用同一可信 Store snapshot，Core 启动并监督 Gateway listeners 与 recovery dispatcher，并补齐 transport 到 deadletter 的有界原始证据；不得用 noop handler 把 admission 标记为已处理。MQL5 adapter 里程碑仍必须满足第 3.1 和 24 节规定的串行回调、有界网络泵约束及测试。

## 建议的开场提示

```text
完整阅读 HANDOFF.md 和 docs/quant_trading_7_layer_target_architecture.md。
将已经实现的协议、State Store、Risk、Execution、Reconciliation、Gateway、Control Plane HTTP、Event WebSocket 和 durable admission/persistence adapter 视为经过验证的基线；修改前先运行基线验收命令。
下一里程碑实现 handler-specific inbound 原子 dispatcher、Gateway listener 运行组合、可信 RiskRequest assembler 和 Risk→Execution workflow processor；复用现有领域状态机和 Store transaction boundary，不得创建绕过 Trading Core 的 execution 通道，也不得把 durable intake 等同于业务处理完成。
报告完成前，运行 cargo fmt --all --check、cargo check --workspace 和 cargo test --workspace。
架构存在歧义时，先指出冲突并解决文档问题，再修改代码。
```
