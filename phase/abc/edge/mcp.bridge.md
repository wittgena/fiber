# fiber.phase.abc.edge.mcp.bridge
@desc: Fiber MCP Transition Bridge & A2A Egress Connector Specification

## 0. Executive Summary

본 스펙은 무상태(Stateless) MCP 2026-07-28 프로토콜이 야기하는 분산 시스템의 복잡성(동시성 제어, 멱등성, 보안)을 DPHI 생태계가 어떻게 흡수하는지 정의합니다.

이 아키텍처는 클라이언트(Agent)와 제공자(Provider) 양측에 어떠한 구조적 변경도 요구하지 않는 대칭적 무마찰(Symmetrical Zero-Friction)을 달성하며, 기존 Web2 서버와 낡은 CLI 유물들을 단발성 WASM 샌드박스 기반의 자율 에이전트 망(A2A Economy)으로 승화(Sublimation)시키는 완벽한 마이그레이션 경로를 제공합니다.

1. **복잡성 은닉 (The Sync-Async Facade):** 클라이언트에게는 친숙한 단일 API를 제공하고, 내부적으로는 100% 비동기 결정론적 상태 기계(Deterministic FSM)를 오케스트레이션합니다.
2. **Zero-Code A2A 편입 (Lock-Free Ephemeral Sandbox):** 서버 제공자는 코드 수정 없이, 단일 CLI 명령어(`fiber connect`)만으로 레거시 서버를 1회용 샌드박스로 격리하여 무한한 동시성을 갖춘 과금형 노드로 편입시킬 수 있습니다.
3. **상태의 역전 (Yield-Resume FSM):** 인간의 입력을 기다리는 블로킹(Blocking) 레거시를 파괴하지 않고, `YIELD` 상태로 가로채어 에이전트에게 비동기 프롬프트(HTTP 202)로 번역해 냅니다.

---

## 1. 아키텍처 경계(The 3-Tier Transition)

본 시스템은 외부의 혼란(HTTP/Network Faults)과 내부의 질서(WASM Ledger)를 격리하기 위해 3계층의 아키텍처 경계를 확립합니다.

* **[Ingress] Transition Bridge (`edge.mcp.bridge`):** 클라이언트의 트래픽을 수신하는 **동기-비동기 파사드(Facade) 및 부패 방지 계층(ACL)**. 다양한 암호학적 파편(DPoP, JWK)을 흡수하고, 트래픽을 비동기 인텐트로 치환하여 코어 망에 주입합니다.
* **[Core] Kernel Ledger (`rpc.handler`):** 모든 트랜잭션의 생애 주기를 `PENDING` ➔ `YIELD` ➔ `RESOLVED/FAULTED`의 상태 전이(`LogicStream`)로 원장에 영구 씰링(Sealing)하여 결제와 실행의 무결성을 증명합니다.
* **[Egress] Ephemeral Connector (`fiber connect`):** 기존 서버에 부착되는 플러그 앤 플레이 사이드카. DPHI 버스를 구독하여 인텐트가 들어올 때마다 **독립된 1회용 샌드박스(프로세스/WASM)를 Spawn하고, 작업이 끝나면 소멸(GC)시키는 무잠금(Lock-Free) I/O 중계기**입니다.

---

## 2. MCP Bridge 아키텍처 (Stateful ↔ 2026-07-28 Stateless)

**2026-07-28 스펙 도입으로 MCP가 상태 비저장(Stateless) 아키텍처로 전면 전환되면서** 프로토콜 자체는 가벼워졌으나, 매 요청에 대한 암호학적 인증, 동시성 제어(Race condition), 트랜잭션 멱등성 보장이라는 거대한 분산 시스템의 복잡성은 오롯이 클라이언트에게 전가되었습니다.

게이트웨이 내의 `edge.mcp.adapter`와 `bridge`는 이러한 책임 회피적 구조가 낳은 파편화된 Stateless 요청들을 흡수(Complexity Sink)하여, 안전한 결정론적 상태 전이(Deterministic State Transition)로 승화시키는 상태 앵커(State Anchor) 역할을 수행합니다.

```text
[ Stateless Chaos (External) ]       [ edge.mcp (Complexity Sink) ]             [ Deterministic Order ]
                                                                                
Agent A (EXECUTE) ──────┐        ┌────────────────────────────────────┐        ┌──────────────────────┐
  + DPoP / L402         │        │ 1. Cryptographic Adapter (ACL)     │        │ DPHI Core Ledger     │
                        ├─(REST)▶│   - Validate JWK, DPoP, L402       │───┐    │ ┌──────────────────┐ │
Agent B (RESUME) ───────┤        │   - Nonce & Idempotency Mapping    │   │    │ │ ID: txn_A (Exec) │ │
  + TOTP (Elicitation)  │        ├────────────────────────────────────┤   ├───▶│ │ ID: txn_B (YIELD)│ │
                        │        │ 2. Transition Bridge (Facade)      │   │    │ └─────────┬────────┘ │
Agent C (Replay) ───────┘        │   - JSON ↔ LogicStream Translation │   │    │           │          │
  └─ 423 Locked (Blocked)        │   - State Polling Loop (Facade)    │◀──┘    └───────────┼──────────┘
                                 └─────────────────┬──────────────────┘                    ▼ (RPC Bus)
<── HTTP 202 Accepted (YIELD) ─────────────────────┤                           ┌──────────────────────┐
<── HTTP 200 OK (RESOLVED) ────────────────────────┤                           │ Ephemeral Connector  │
<── HTTP 502 Bad Gateway (FAULTED) ────────────────┘                           │ ┌─ Sandbox A (Run) │ │
                                                                               │ ├─ Sandbox B (Park)│ │
                                                                               │ └─ Sandbox C (Dead)│ │
                                                                               └──────────────────────┘

```

* **Cryptographic Adapter (부패 방지 계층):** 커넥션 기반 세션이 사라진 자리를 완벽히 대체합니다. 파편화된 암호화 스펙(Ed25519, RSA, EC)을 정규화하고, 매 요청마다 **DPoP 서명**과 **L402 결제 영수증**을 검증합니다. 분산 캐시 기반의 난수 락(`NonceReplayProtector`)을 통해 리플레이 공격과 중복 결제를 엣지 단에서 즉시 차단합니다.
* **Transition Bridge (동기-비동기 파사드):** 클라이언트의 REST 요청을 비동기 인텐트(`LogicStream`)로 변환하여 원장에 제안합니다. 클라이언트와의 HTTP 커넥션을 유지한 채 상태를 폴링하며, 내부의 복잡한 분산 이벤트 소싱 과정을 추상화합니다.
* **Ephemeral Connector (무잠금 샌드박스):** RPC 버스를 통해 인텐트가 전달되면, 타겟 서버의 커넥터는 기존 프로세스에 락(Lock)을 거는 대신 트랜잭션당 1개의 독립된 1회용 샌드박스를 복제(Spawn)합니다.
* **State Sublimation (상태의 역전과 호흡):** 레거시 샌드박스가 인간의 개입(OTP 등)을 요구하며 멈춰설 때, 시스템은 크래시를 내지 않고 프로세스를 동면(Park)시킵니다. 이 `YIELD` 상태는 원장을 거쳐 브릿지로 전달되며, 브릿지는 즉시 루프를 깨고 클라이언트에게 `HTTP 202 Accepted`를 반환하여 안전하고 우아한 비동기 대화형 제어(Elicitation Trap)를 완성합니다.

---

## 3. Ingress: 클라이언트 연동 규격 (Client ➔ Gateway)

Gateway는 상태 제어용 봉투(FSM Envelope)를 헤더로 추상화합니다.

* **Endpoint:** `POST /v1/mcp-gateway/{target_server_id}/invoke`
* **Content-Type:** `application/json`

### 3.1. 제어 및 신뢰 메타데이터 (Control Headers)

| 헤더 필드 | 타입 | 필수 여부 | 아키텍처 목적 (Purpose) |
| --- | --- | --- | --- |
| `x_idempotency_key` | String | **필수** | 분산 트랜잭션의 고유 식별자(Handle ID). 타임아웃 재시도 및 YIELD-RESUME 맵핑 시 원장 상태와 결속됩니다. |
| `x_nonce` | String | **필수** | Replay Attack 방지용 1회성 난수. |
| `X-X402-Receipt` | String | 선택 (Track A) | **[A2A 종량제 과금]** M2M 연산 연료(Fuel) 결제 증명. |
| `DPoP` | String | 선택 (Track B) | **[엔터프라이즈 인가]** RFC 9449 기반 탈취 방지 서명 및 신원(SPIFFE) 증명. |

### 3.2. Request Body Schema

오직 2026-07-28 스펙을 준수하는 순수 JSON-RPC 객체만 전송합니다.

---

## 4. 트랜잭션 라이프사이클 (The Sync-Async Facade)

클라이언트는 동기식 API처럼 요청을 보내지만, Gateway 내부에서는 철저한 FSM 이벤트 소싱이 오케스트레이션 됩니다.

1. **Idempotency & Security Validation:**
* `x_idempotency_key`를 확인하여 멱등성을 보장하고, DPoP 서명과 Nonce를 검증하여 외부 카오스를 필터링합니다.


2. **Intent Broadcast & Propose:**
* L402 결제를 승인하고 원장에 `PENDING` 상태 전이를 제안합니다.
* 타겟 서버의 Egress Connector를 향해 RPC 버스로 인텐트를 브로드캐스트합니다.


3. **The Facade Loop (동기-비동기 대기):**
* Gateway는 HTTP 커넥션을 물고 원장의 상태를 폴링(Projection)합니다.


4. **State Sublimation (상태 전이 및 반환):**
* **[RESOLVED]:** 실행 완료. 순수 결과를 추출하여 `HTTP 200 OK` 응답.
* **[YIELD]:** 레거시 서버가 사용자 입력(OTP 등)을 대기하며 동면(Park)함. Gateway는 즉시 루프를 깨고 `HTTP 202 Accepted`를 반환하여 에이전트에게 응답을 요구함.
* **[FAULTED]:** 레거시 에러 또는 Sentinel 타임아웃. `HTTP 502 Bad Gateway` 반환.

---

## 5. Egress: 샌드박스 커넥터 규격 (Provider ➔ Core)

제공자(Provider)는 방화벽 개방 없이 단일 CLI 명령어만으로 레거시를 A2A 노드로 승화시킵니다.

### 5.1. Zero-Friction 배포

```bash
# 기존 방식: 단일 호스트, 단일 파이프의 병목
$ node my-legacy-mcp.js

# Fiber 생태계: 인텐트당 1개의 샌드박스를 복제(Spawn)하는 무잠금 병렬 라우터
$ fiber connect --target "my-db-server-01" --exec "node my-legacy-mcp.js"

```

### 5.2. 커넥터의 아키텍처 매커니즘 (Ephemeral Sandbox Spawn)

1. **Outbound Pull:** 내부망(VPC)에서 DPHI Core 망을 향해 아웃바운드 터널을 열고 인텐트를 당겨옵니다.
2. **Ephemeral Spawn (Lock-Free):** 100개의 요청이 동시에 들어오면, 커넥터는 기존 프로세스에 락(Lock)을 거는 것이 아니라 **100개의 독립된 레거시 프로세스(샌드박스)를 띄워 각각의 파이프(`stdin`)에 데이터를 주입**합니다.
3. **FSM Interception (Yield-Resume):** 프로세스의 응답 중 `elicitation`(입력 요구)을 감지하면 프로세스를 죽이지 않고 딕셔너리에 보관(Park)한 뒤 원장에 `YIELD`를 보고합니다.
4. **Garbage Collection:** 최종 결과(`RESOLVED`)나 에러(`FAULTED`)가 반환되면 샌드박스를 즉각 파괴하고 자원을 회수합니다.

---

## WASM Compute-to-Data

물리적 실행과 네트워크 통신을 완벽히 분리했으므로, 이 구조는 **WASM 서버리스 연산**으로 즉시 진화할 수 있습니다.

* 제공자는 낡은 스크립트 대신 `wasmtime run duckdb_mcp.wasm`으로 실행 엔진만 교체합니다.
* WASM 내부의 DB 엔진(DuckDB)은 WASI(Virtual File System)를 통해 외부 스토리지에서 필요한 바이트만 핀포인트로 당겨옵니다(Lazy Loading).
* AI 에이전트의 연산은 정확한 WASM 인스트럭션 사이클(Instruction Cycle) 단위로 측정되어 **수학적으로 완벽한 L402 Fuel(연료) 과금 종량제**를 완성합니다.

---

## 7. 분산 에러 핸들링 매트릭스 (Error Handling Matrix)

| HTTP 상태 | 내부 코드 | 에러 메시지 | 아키텍처적 의미 및 대처법 |
| --- | --- | --- | --- |
| **202 Accepted** | N/A | (YIELD Payload) | 레거시 프로세스가 안전하게 동면(Park)됨. 제공된 Prompt ID로 TOTP/입력을 포함하여 `RESUME` 인텐트를 재전송 요망. |
| **401 Unauth** | `-32001` | `CRYPTOGRAPHIC_BINDING_FAILED` | DPoP 서명/Nonce 검증 실패. |
| **402 Payment** | `-32002` | `KERNEL_AUTHORIZATION_REJECTED` | L402 스테이블코인 잔액 부족. |
| **502 Bad GW** | `-32008` | `LEGACY_SERVER_FAULT` | **[격리 증명]** 샌드박스 내부에서 크래시가 발생하여 해당 프로세스만 안전하게 파괴됨. |
| **504 Timeout** | `-32007` | `TRANSACTION_STILL_PROCESSING` | 타겟 서버 연산 지연. 반드시 동일한 `x_idempotency_key`로 재요청하여 중복 없이 대기열 합류 요망. |