# fiber.phase.abc.bridge.mcp
**@desc:** Fiber MCP Transition Bridge & A2A Egress Connector Specification

## 0. Executive Summary

본 스펙은 무상태(Stateless) MCP 2026-07-28 프로토콜이 야기하는 분산 시스템의 복잡성(동시성 제어, 멱등성, 보안)을 DPHI 생태계가 어떻게 흡수하는지 정의합니다.

이 아키텍처는 클라이언트(Agent)와 제공자(Provider) 양측에 어떠한 구조적 변경도 요구하지 않는 대칭적 무마찰(Symmetrical Zero-Friction)을 달성하며, 기존 Web2 서버와 레거시 스크립트들을 자율 에이전트 망(A2A Economy)의 과금형 노드로 승화(Sublimation)시키는 완벽한 마이그레이션 경로를 제공합니다.

1. **복잡성 은닉 (The Sync-Async Facade):** 클라이언트에게는 친숙한 단일 API를 제공하고, 내부적으로는 100% 비동기 결정론적 상태 기계(Deterministic FSM)를 오케스트레이션합니다.
2. **Tri-Track Concurrency (3단계 동시성 토폴로지):** 서버 제공자는 코드 수정 없이, 단일 CLI 명령어(`fiber connect`)만으로 자신의 스크립트를 그 특성에 맞게 `ephemeral`, `linear`, `multiplex` 3가지 모드 중 하나로 띄워 극한의 동시성과 격리성을 확보할 수 있습니다.
3. **상태의 역전 (Yield-Resume FSM):** 인간의 입력을 기다리는 블로킹(Blocking) 레거시를 파괴하지 않고, `YIELD` 상태로 가로채어 에이전트에게 비동기 프롬프트(HTTP 202)로 번역해 냅니다.

---

## 1. 아키텍처 경계(The 3-Tier Transition)

본 시스템은 외부의 혼란(HTTP/Network Faults)과 내부의 질서(WASM Ledger)를 격리하기 위해 3계층의 아키텍처 경계를 확립합니다.

* **[Ingress] Transition Bridge (`edge.mcp.bridge`):** 클라이언트의 트래픽을 수신하는 **동기-비동기 파사드(Facade) 및 부패 방지 계층(ACL)**. 다양한 암호학적 파편(DPoP, JWK)을 흡수하고, 트래픽을 비동기 인텐트로 치환하여 코어 망에 주입합니다.
* **[Core] Kernel Ledger (`rpc.handler`):** 모든 트랜잭션의 생애 주기를 `PENDING` ➔ `YIELD` ➔ `RESOLVED/FAULTED`의 상태 전이(`LogicStream`)로 원장에 영구 씰링(Sealing)하여 결제와 실행의 무결성을 증명합니다.
* **[Egress] Worker Connector (`fiber connect`):** 기존 서버에 부착되는 플러그 앤 플레이 사이드카. DPHI 버스를 구독하여 인텐트가 들어올 때마다 타겟 워커의 수준에 맞춰 프로세스를 격리(ephemeral), 큐잉(linear), 또는 비동기 다중화(multiplex)하는 무잠금(Lock-Free) I/O 중계기입니다.

---

## 2. MCP Bridge 아키텍처 (Stateful ↔ 2026-07-28 Stateless)

**2026-07-28 스펙 도입으로 MCP가 상태 비저장(Stateless) 아키텍처로 전면 전환되면서** 프로토콜 자체는 가벼워졌으나, 매 요청에 대한 암호학적 인증, 동시성 제어(Race condition), 트랜잭션 멱등성 보장이라는 거대한 분산 시스템의 복잡성은 오롯이 클라이언트에게 전가되었습니다.

게이트웨이 내의 `edge.mcp.bridge`와 `bridge.adapter`는 이러한 책임 회피적 구조가 낳은 파편화된 Stateless 요청들을 흡수(Complexity Sink)하여, 안전한 결정론적 상태 전이(Deterministic State Transition)로 승화시키는 상태 앵커(State Anchor) 역할을 수행합니다.

```text
[ Stateless Chaos (External) ]       [ edge.mcp (Complexity Sink) ]             [ Deterministic Order ]
                                                                                                       
Agent A (EXECUTE) ──────┐        ┌────────────────────────────────────┐        ┌──────────────────────┐
  + DPoP / x402         │        │ 1. Cryptographic Adapter (ACL)     │        │ DPHI Core Ledger     │
                        ├─(REST)▶│   - Validate JWK, DPoP, x402       │───┐    │ ┌──────────────────┐ │
Agent B (RESUME) ───────┤        │   - Nonce & Idempotency Mapping    │   │    │ │ ID: txn_A (Exec) │ │
  + TOTP (Elicitation)  │        ├────────────────────────────────────┤   ├───▶│ │ ID: txn_B (YIELD)│ │
                        │        │ 2. Transition Bridge (Facade)      │   │    │ └─────────┬────────┘ │
Agent C (Replay) ───────┘        │   - JSON ↔ LogicStream Translation │   │    │           │          │
  └─ 423 Locked (Blocked)        │   - State Polling Loop (Facade)    │◀──┘    └───────────┼──────────┘
                                 └─────────────────┬──────────────────┘                    ▼ (RPC Bus)
<── HTTP 202 Accepted (YIELD) ─────────────────────┤                           ┌──────────────────────┐
<── HTTP 200 OK (RESOLVED) ────────────────────────┤                           │ Worker Connector     │
<── HTTP 502 Bad Gateway (FAULTED) ────────────────┘                           │ ┌─ Ephemeral Mode  │ │
                                                                               │ ├─ Linear Mode     │ │
                                                                               │ └─ Multiplex Mode  │ │
                                                                               └──────────────────────┘

```

* **Cryptographic Adapter (부패 방지 계층):** 파편화된 암호화 스펙(Ed25519, RSA, EC)을 정규화하고, 매 요청마다 **DPoP 서명**과 **x402 결제 영수증**을 검증합니다. Check-Then-Act 레이스를 방어하는 분산 락(`IdempotencyMapper`)과 난수 캐시(`NonceReplayProtector`)를 통해 리플레이 공격을 엣지 단에서 차단합니다.
* **Transition Bridge (동기-비동기 파사드):** 클라이언트의 REST 요청을 비동기 인텐트(`LogicStream`)로 변환합니다. 타임아웃 발생 시 즉각 `FORCE_ROLLBACK`을 발행하여 좀비 워커를 차단하고 504를 반환하는 강력한 타임아웃 롤백 기전을 수행합니다.
* **State Sublimation (상태의 역전과 호흡):** 워커 샌드박스가 인간의 개입(OTP 등)을 요구하며 멈춰설 때(`ephemeral/multiplex`), 시스템은 프로세스를 동면(Park)시키고 `YIELD` 상태를 원장에 보고합니다. 브리지는 즉시 루프를 깨고 클라이언트에게 `HTTP 202 Accepted`를 반환하여 안전하고 우아한 비동기 대화형 제어(Elicitation Trap)를 완성합니다.

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

---

## 4. 트랜잭션 라이프사이클 (The Sync-Async Facade)

클라이언트는 동기식 API처럼 요청을 보내지만, Gateway 내부에서는 철저한 FSM 이벤트 소싱이 오케스트레이션 됩니다.

1. **Idempotency & Security Validation:**

* `x_idempotency_key`를 확인하여 멱등성을 보장하고, DPoP 서명과 Nonce를 검증하여 외부 카오스를 필터링합니다.

2. **Intent Broadcast & Propose:**

* x402 결제를 승인하고 원장에 `PENDING` 상태 전이를 제안한 뒤, RPC 버스를 통해 타겟 서버 커넥터로 인텐트를 브로드캐스트합니다.

3. **The Facade Loop (동기-비동기 대기 및 롤백 방어):**

* Gateway는 커넥션을 유지한 채 원장 상태를 폴링합니다. 30초 내 응답이 없으면 타겟 워커를 향해 `FORCE_ROLLBACK`을 브로드캐스트한 후 `HTTP 504`로 종료합니다.

4. **State Sublimation (상태 전이 및 반환):**

* **[RESOLVED]:** 순수 결과를 추출하여 `HTTP 200 OK` 응답.
* **[YIELD]:** 서버가 사용자 입력 대기 중. `HTTP 202 Accepted` 반환.
* **[FAULTED]:** 레거시 에러 또는 롤백. `HTTP 502 Bad Gateway` 반환.

---

## 5. Egress: 3단계 동시성 커넥터 규격 (Provider ➔ Core)

제공자(Provider)는 방화벽 개방 없이 단일 CLI 명령어만으로 자신의 스크립트 특성에 맞는 동시성 모드를 선택하여 A2A 노드로 승화시킵니다.

### 5.1. Zero-Friction 배포와 토폴로지 선택

```bash
# 1. Ephemeral 모드 (상태 격리 / 인간 개입)
# - 요청마다 1회용 샌드박스를 복제(Spawn)하여 GC 파괴. 완벽한 메모리 누수 방어.
$ fiber connect --mode ephemeral --target "db-admin" --exec "node deploy.js"

# 2. Linear 모드 (초고속 순차 큐잉 데몬)
# - 무거운 초기화 라이브러리(QuantLib 등)가 필요하며 연산만 하는 워커.
# - 콜드스타트 없이 1개의 데몬이 STDIN 큐를 차례대로 고속 처리.
$ fiber connect --mode linear --target "fin-engine" --exec "python finlib.py"

# 3. Multiplex 모드 (비동기 인메모리 다중화)
# - 이벤트 루프(asyncio)가 내장되어 네트워크 I/O 병목을 해결한 고도화된 워커.
# - 수만 건의 트래픽을 단일 데몬 안에서 병렬로 스트리밍.
$ fiber connect --mode multiplex --target "oracle" --exec "python oracle.py"

```

### 5.2. 커넥터의 아키텍처 매커니즘

1. **Outbound Pull:** 내부망(VPC)에서 DPHI Core 망을 향해 아웃바운드 터널을 열어 인바운드 방화벽 위협을 원천 차단합니다.
2. **I/O Abstraction:** 레거시는 어떠한 네트워크 라이브러리도 알 필요가 없습니다. 커넥터가 프로세스를 통제하며 오직 표준 입출력(`stdin`/`stdout`)으로만 JSON-RPC 객체를 스트리밍합니다.
3. **FSM Interception (Yield-Resume):** `ephemeral`이나 `multiplex` 환경에서 프로세스의 응답 중 `elicitation`(입력 요구)을 감지하면, 락(Lock)을 해제하고 프로세스(또는 코루틴)를 동면시킨 채 코어에 `YIELD`를 보고합니다. 이후 클라이언트의 입력이 돌아오면 다시 데이터를 밀어 넣어 트랜잭션을 재개(`RESUME`)합니다.

---

## 6. WASM Compute-to-Data (The Endgame)

물리적 실행과 네트워크 통신을 완벽히 분리한 3단계 토폴로지 구조는 **WASM 서버리스 연산**으로 즉시 진화할 수 있는 토대입니다.

* 제공자는 낡은 파이썬 스크립트 대신 `wasmtime run duckdb_mcp.wasm`으로 실행 엔진만 교체합니다.
* WASM 내부의 로직은 WASI(Virtual File System)를 통해 외부 스토리지에서 필요한 바이트만 당겨오며, 커넥터가 이를 완벽히 중계합니다.
* AI 에이전트의 연산은 정확한 WASM 인스트럭션 사이클(Instruction Cycle) 단위로 측정되어 **수학적으로 완벽한 x402 Fuel(연료) 과금 종량제**를 완성하게 됩니다.