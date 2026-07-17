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
- 第一、第二、第三里程碑的 Rust 工作区已经实现。内部 crate 目录不带前缀，Cargo 包名仍使用 `sinan-*`。
- `sinan-types`、`sinan-protocol`、三个协议黄金样例、SQLite migration、repository 和 state-ingest projection 均已实现并通过测试。
- `sinan-store` 已有 22 张业务表的 V0002 schema、canonical JSON/hash、显式写事务、typed repository、授权 latest-state query 和原子 ingest projection rebuild。
- `sinan-risk` 已实现不可变 `RiskRequest` / policy / capacity / full-set watermark 领域模型、确定性 Decimal position sizing evaluator 和纯 circuit breaker 状态机。
- `sinan-types` 已增加带语义校验的共享 `RiskResult` / `AdjustedRiskLeg` DTO，`sinan-store` 已增加不可变 `risk_results` typed insert/get、幂等和损坏检测；Execution、Reconciliation、Gateway、HTTP 等仍是后续里程碑范围。
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
8. `rebuild_ingest_projections` 在单一 write transaction 中重放 account / symbol / position / order / market bar；失败整体回滚，不修改 tick-only `market_snapshots`。

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

当前共 85 项测试，其中 `sinan-store` 43 项，覆盖 migration 升级和校验、22 表 schema、FK / CHECK / trigger、生产连接初始化、canonical JSON、双键幂等、多连接并发 CAS、command-state 创建重放、重复事实补投影、事务回滚、授权查询、latest 冲突、冗余列损坏检测以及原子 rebuild 回滚。

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

Pure circuit breaker 当前同样不依赖 Store，尚无 stable durable snapshot / restore adapter。下一里程碑必须先补充并验证完整 breaker state 的持久化恢复（含 recovery epoch / fingerprint、`incident_evidence_cleared_at`、HALF_OPEN 时间与财务 baseline、blocked count）；重启、缺失或损坏时必须 fail closed，禁止默认回到 `CLOSED`。如果该里程碑只实现纯 Execution domain，可以暂不接 Risk→Execution live flow；一旦接入，trusted assembler 和 intent / risk / plan / command 原子 transaction boundary 也必须同时落地。

第三里程碑验收已经通过：

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
git diff --check
```

当前共 175 项测试，其中 `sinan-risk` 69 项（28 项 circuit breaker、41 项 evaluator），覆盖单腿 / 多腿、cost buffer、volume-step floor、volume / exposure / margin 全局缩放、风险预算单调性、freshness、跨账户输入、pending command、HOLD / CLOSE、breaker gate 和 recovery epoch；`sinan-store` 共 56 项，其中 13 项 `risk_results` 集成测试；`sinan-types` 共 17 项，覆盖 RiskResult serde 与语义校验。

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
3. Circuit Breaker durable restore、Execution command 和状态机。（下一里程碑）
4. 对账。
5. Gateway session registry 和出站投递端口。
6. Native TCP 和 Execution WebSocket 绑定。
7. HTTP TradeIntent/state/time API 和 Event WebSocket。
8. Fake Execution Client 端到端测试。
9. MQL5 和 OKX 适配器。
10. Strategy & Decision Control Plane。

Risk 里程碑已经实现第 3.6、7.12-7.13 和 15 节规定的确定性 position-sizing 契约。下一里程碑先实现 Circuit Breaker durable restore，再实现 Execution command / state machine；Execution 必须精确映射已批准 lots，并在任何参数漂移时重新执行风控。若尚未实现 trusted assembler 和原子 workflow commit，则不得接入 live Risk→Execution flow。MQL5 adapter 里程碑必须满足第 3.1 和 24 节规定的串行回调、有界网络泵约束及测试。

## 建议的开场提示

```text
完整阅读 HANDOFF.md 和 docs/quant_trading_7_layer_target_architecture.md。
将已经实现的第一、第二、第三里程碑视为经过验证的基线；修改前先运行基线验收命令。
只实现下一项被明确选择的 Circuit Breaker durable restore 与 Execution command / 状态机里程碑；不得跳过依赖边界或启动无关服务。
报告完成前，运行 cargo fmt --all --check、cargo check --workspace 和 cargo test --workspace。
架构存在歧义时，先指出冲突并解决文档问题，再修改代码。
```
