# Sinan Implementation Handoff

## Project

Sinan is a multi-platform quantitative trading system spanning research, strategy and decision control, hard risk control, execution, reconciliation, and broker/exchange adapters.

The project name is **Sinan**. Concrete package names use the `sinan-*` prefix. Architecture layer names remain language- and framework-independent.

## Authoritative Design

The source of truth is:

```text
docs/quant_trading_7_layer_target_architecture.md
```

Read the complete document before implementation, especially sections 4-7 and 23-24. If code and documentation conflict, stop and resolve the design explicitly; do not silently invent a third behavior.

## Current State

- Architecture and implementation specifications are documented.
- No Rust workspace or production code has been created yet.
- The existing MQL5 EA remains in the MetaTrader workspace and is not part of this Rust repository.
- The first implementation target is the Trading Core foundation, not the MT5 adapter or Control Plane.

## Non-negotiable Architecture Decisions

1. Trading Core is the trading correctness boundary.
2. Strategy & Decision Control Plane submits `TradeIntent`; it never creates or sends `execution.command` directly.
3. Risk Engine is a hard gate inside Trading Core.
4. Execution Client Protocol is transport-independent.
5. Native TCP and Execution WebSocket are Execution Client Protocol bindings.
6. Event WebSocket (`/events`) is separate and cannot carry `execution.command`.
7. SQLite append-only facts and projections are the authoritative local execution state.
8. `ExecutionEvent` is the execution fact source; command, leg, and plan status are projections.
9. `execution.command` requires HMAC-SHA256 using the fixed signing string in section 23.1.
10. `transport.ack`, `command.received`, and `execution.event` have different semantics and must not be conflated.
11. Trading Core server time is authoritative. Execution Client and Control Plane maintain offsets using monotonic clocks.
12. Expired, unauthenticated, stale, unreconciled, or risk-blocked work must fail closed.
13. WebSocket event gaps recover through bounded replay or `GET /state`; they never imply execution failure.
14. Gateway transports and routers cannot mutate execution lifecycle state directly.

## Rust Workspace Target

Create this workspace:

```text
sinan/
  Cargo.toml
  crates/
    sinan-types/
    sinan-protocol/
    sinan-domain/
    sinan-store/
    sinan-gateway/
    sinan-risk/
    sinan-execution/
    sinan-reconciliation/
    sinan-events/
    sinan-http/
    sinan-core/
  docs/
    quant_trading_7_layer_target_architecture.md
  tests/
    golden/
      execution_client_protocol/
```

`sinan-core` is the binary composition root. Other crates are libraries.

Dependency direction:

```text
sinan-core
  -> gateway / http
  -> risk / execution / reconciliation / events
  -> store
  -> domain
  -> types
```

`sinan-protocol` depends only on `sinan-types` and protocol-level libraries. It must not depend on store, gateway, risk, or execution.

## First Milestone

Implement only the foundation below:

1. Create the Cargo workspace and all crate directories.
2. Implement `sinan-types`:
   - shared IDs and newtypes;
   - `ErrorCode`;
   - execution, session, and storage status enums;
   - common DTOs needed by the protocol.
3. Implement `sinan-protocol`:
   - `ExecutionClientMessageType`;
   - generic `WireMessage<T>`;
   - `ecp.v<major>.<minor>` parsing and compatibility checks;
   - envelope validation;
   - HMAC signing string generation and verification;
   - Native TCP framing codec;
   - transport-independent payload types.
4. Materialize the three golden JSON files from section 23.1.
5. Test the documented HMAC vector:

```text
secret: test_command_secret_v1
expected: 044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e
```

6. Implement `sinan-store` migration infrastructure and the initial `schema_migrations` migration.
7. Add placeholder public APIs for the remaining crates only when required for workspace compilation.

Do not implement TCP listeners, WebSocket servers, HTTP endpoints, Risk Engine policies, Execution Engine behavior, or MQL5 integration in this milestone.

## First Milestone Acceptance Criteria

The milestone is complete only when all pass:

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
```

Required tests:

- WireMessage JSON round-trip.
- Unknown message type rejection.
- Schema major mismatch rejection.
- Higher compatible minor acceptance.
- Golden JSON parsing.
- Exact HMAC golden vector match.
- Optional signing fields map to empty strings.
- Fixed decimal formatting preserves required trailing zeros.
- Native TCP length-prefix fragmentation and coalescing.
- Oversized frame rejection.
- Migration checksum mismatch rejection.

## Implementation Constraints

- Use server-time Unix milliseconds for business timestamps.
- Use monotonic clocks only for local elapsed time and RTT.
- Keep payload DTOs immutable where practical.
- Do not use JSON canonicalization for command HMAC.
- Do not let transport code own retry or command lifecycle decisions.
- Keep migrations forward-only and checksum verified.
- Preserve existing architecture vocabulary in public APIs.
- Avoid adding dependencies without a concrete need.

## Future Milestones

After the foundation is stable:

1. SQLite repositories and projections.
2. Risk and circuit breaker domain logic.
3. Execution command and state machine.
4. Reconciliation.
5. Gateway session registry and outbound delivery port.
6. Native TCP and Execution WebSocket bindings.
7. HTTP TradeIntent/state/time APIs and Event WebSocket.
8. Fake Execution Client end-to-end tests.
9. MQL5 and OKX adapters.
10. Strategy & Decision Control Plane.

## Suggested Opening Prompt

```text
Read HANDOFF.md and docs/quant_trading_7_layer_target_architecture.md completely.
Implement the First Milestone exactly as scoped in HANDOFF.md.
Do not start network servers, Risk Engine, Execution Engine, HTTP APIs, or adapters yet.
Run cargo fmt --all --check, cargo check --workspace, and cargo test --workspace before reporting completion.
When the architecture is ambiguous, identify the conflict and resolve the documentation before coding.
```
