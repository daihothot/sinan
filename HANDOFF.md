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

- 架构和实现规格已经完成记录，并与基础代码保持同步。
- 第一里程碑的 Rust 工作区已经实现。内部 crate 目录不带前缀，Cargo 包名仍使用 `sinan-*`。
- `sinan-types`、`sinan-protocol`、三个协议黄金样例，以及带校验和验证的 SQLite migration 基础设施均已实现并通过测试。
- 其余 crate 暂时仅作为后续里程碑所需的可编译占位。
- 现有 MQL5 EA 仍位于 MetaTrader 工作区，不属于此 Rust 仓库。
- 当前工作区基线已通过 `cargo fmt --all --check`、`cargo check --workspace` 和 `cargo test --workspace`。

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

1. SQLite repository 和 projection。
2. Risk 与 circuit breaker 领域逻辑。
3. Execution command 和状态机。
4. 对账。
5. Gateway session registry 和出站投递端口。
6. Native TCP 和 Execution WebSocket 绑定。
7. HTTP TradeIntent/state/time API 和 Event WebSocket。
8. Fake Execution Client 端到端测试。
9. MQL5 和 OKX 适配器。
10. Strategy & Decision Control Plane。

Risk 里程碑必须实现第 3.6、7.12-7.13 和 15 节规定的确定性 position-sizing 契约。Execution 里程碑必须精确映射已批准 lots，并在任何参数漂移时重新执行风控。MQL5 adapter 里程碑必须满足第 3.1 和 24 节规定的串行回调、有界网络泵约束及测试。这些内容都不属于第一里程碑交付范围。

## 建议的开场提示

```text
完整阅读 HANDOFF.md 和 docs/quant_trading_7_layer_target_architecture.md。
将已经实现的第一里程碑视为经过验证的基线，修改前先运行其验收命令。
只实现 HANDOFF.md 中下一项被明确选定的里程碑；不得跳过依赖边界或启动无关服务。
报告完成前，运行 cargo fmt --all --check、cargo check --workspace 和 cargo test --workspace。
架构存在歧义时，先指出冲突并解决文档问题，再修改代码。
```
