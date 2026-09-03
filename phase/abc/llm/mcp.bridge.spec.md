# fiber.phase.abc.llm.mcp.bridge.spec

**@desc:** Fiber MCP Transition Bridge & A2A Egress Connector Specification
**@version:** 4.0 (Sync-Async Facade & Symmetrical Zero-Friction Edition, 2026.09.03)

---

## 0. 스펙의 목적 및 아키텍처 비전 (Executive Summary)

본 스펙은 무상태(Stateless) MCP 프로토콜이 야기하는 분산 시스템의 복잡성(동시성 제어, 멱등성, 보안)을 DPHI 생태계가 어떻게 흡수하는지 정의하는 통합 연동 백서(Integration Whitepaper)입니다.

이 아키텍처는 클라이언트(Agent)와 제공자(Provider) 양측에 어떠한 구조적 변경도 요구하지 않는 대칭적 무마찰(Symmetrical Zero-Friction)을 달성하며, 기존 Web2 서버를 WASM 기반의 자율 에이전트 망(A2A Economy)으로 승화(Sublimation)시키는 완벽한 마이그레이션 경로를 제공합니다.

1. **복잡성 은닉 (The Sync-Async Facade):** 클라이언트에게는 친숙한 동기식(Synchronous) 단일 API를 제공하고, 내부적으로는 100% 비동기 결정론적 상태 기계(Deterministic FSM)를 오케스트레이션합니다.
2. **Zero-Code A2A 편입 (Egress Sidecar):** 서버 제공자는 방화벽 개방이나 코드 수정 없이, 단일 CLI 명령어(`fiber connect`)만으로 레거시 서버를 DPHI 생태계의 과금형 노드로 편입시킬 수 있습니다.
3. **멱등성 보장 (Idempotency Enforcement):** 네트워크 타임아웃 시 클라이언트의 단순 재시도(Retry)만으로도 이중 과금(Double-billing)과 중복 실행(Double-execution)을 원천 차단하는 상태 전이(LogicStream) 아키텍처를 구현합니다.

---

## 1. 아키텍처 경계 원칙

본 시스템은 외부의 혼란(HTTP/Network Faults)과 내부의 질서(WASM Ledger)를 격리하기 위해 3계층의 아키텍처 경계를 확립합니다.

* **[Ingress] Transition Bridge (`edge.mcp.bridge`):** 클라이언트의 트래픽을 수신하는 **동기-비동기 파사드(Facade)**. L402 및 DPoP 인증을 수행한 후, 트래픽을 비동기 인텐트(Intent)로 치환하여 코어 망에 주입하고 결과가 도달할 때까지 커넥션을 홀딩(Holding)합니다.
* **[Core] Kernel Ledger (`rpc.handler`):** 모든 MCP 트랜잭션의 생애 주기를 `PENDING` ➔ `RESOLVED` ➔ `FAULTED`의 비가역적 상태 전이(`LogicStream`)로 원장에 영구 씰링(Sealing)하여 결제와 실행의 무결성을 증명합니다.
* **[Egress] A2A Connector (`fiber connect`):** 기존 MCP 서버에 부착되는 **플러그 앤 플레이(Plug-and-play) 사이드카**. DPHI 메시지 버스를 구독(Pull)하여 순수 JSON-RPC를 추출하고, 이를 레거시 서버의 표준 입출력(`stdio`)과 매핑하는 I/O 추상화 계층입니다.

---

## 2. Ingress: 클라이언트 연동 규격 (Client ➔ Gateway)

클라이언트는 분산 FSM이나 롱폴링(Long-polling) 메커니즘을 학습할 필요가 없습니다. Gateway는 상태 제어용 봉투(FSM Envelope)를 헤더로 추상화하고, Payload에는 순수 MCP 규격만을 허용합니다.

* **Endpoint:** `POST /v1/mcp-gateway/{target_server_id}/invoke`
* **Content-Type:** `application/json`

### 2.1. 제어 및 신뢰 메타데이터 (Control Headers)

| 헤더 필드 | 타입 | 필수 여부 | 아키텍처 목적 (Purpose) |
| --- | --- | --- | --- |
| `x_idempotency_key` | String | **필수** | 분산 트랜잭션의 고유 식별자. 타임아웃 재시도 시 원장의 상태(PENDING/RESOLVED)와 맵핑되어 중복 실행을 방어합니다. |
| `x_nonce` | String | **필수** | Replay Attack 방지용 1회성 난수. (Gateway 레벨에서 Lock 처리) |
| `X-X402-Receipt` | String | 선택 (Track A) | **[종량제 과금]** M2M API 결제 증명 (L402 Macaroon). |
| `DPoP` | String | 선택 (Track B) | **[엔터프라이즈 인가]** RFC 9449 기반 탈취 방지 서명. |

### 2.2. Request Body Schema (Pure MCP Payload)

Gateway는 내부 상태 전이에 필요한 `action`이나 `handle_id`를 Body에 요구하지 않습니다(Facade 패턴). 오직 2026-07-28 스펙을 준수하는 순수 JSON-RPC 객체만 전송합니다.

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "query_database",
    "arguments": {"sql": "SELECT * FROM users"}
  }
}
```

---

## 3. 트랜잭션 라이프사이클 (The Sync-Async Facade)

클라이언트는 평범한 동기식(Sync) API처럼 요청을 보내지만, Gateway 내부에서는 철저한 비동기 분산 이벤트 소싱(Event Sourcing)이 오케스트레이션 됩니다.

1. **Idempotency Mapping (멱등성 매핑):**
* Gateway는 `x_idempotency_key`를 확인하여 새로운 트랜잭션인지, 아니면 타임아웃으로 인한 재시도(Retry)인지 판별합니다.
* *재시도인 경우, 결제 차감 및 인텐트 큐잉을 생략하고 즉시 3단계(대기)로 합류합니다.*

2. **Intent Broadcast & Propose (인텐트 제안 및 브로드캐스트):**
* 신규 요청일 경우, 결제(L402)를 승인하고 원장(Ledger)에 `PENDING` 상태 전이를 제안(`LogicStream` 씰링)합니다.
* 타겟 서버의 Egress Sidecar를 향해 RPC 버스로 인텐트를 브로드캐스트합니다. (Fire-and-forget).

3. **Connection Holding (동기-비동기 대기):**
* Gateway는 클라이언트와의 HTTP 커넥션을 유지한 채, 내부적으로 원장의 상태가 `RESOLVED`로 전이될 때까지 상태를 폴링(Projection)합니다.

4. **Resolution & Response (상태 전이 및 반환):**
* Sidecar가 실행을 마치고 원장 상태를 `RESOLVED`로 확정지으면, Gateway는 캐싱된 순수 MCP 결과물(Result)을 추출하여 클라이언트에게 `HTTP 200 OK`로 응답합니다.

---

## 4. Egress: A2A 커넥터 연동 규격 (Provider ➔ DPHI Core)

기존 MCP 서버 제공자(Provider)는 DPHI 생태계(A2A 경제망)에 연동하기 위해 서버 코드를 수정하거나 방화벽(Inbound Port)을 개방할 필요가 없습니다. 통합 CLI인 `fiber connect`를 통해 레거시 서버를 즉시 A2A 노드로 승화(Sublimation)시킬 수 있습니다.

### 4.1. Zero-Friction 배포 (Deployment)

```bash
# 기존 방식: 단일 호스트 내 격리된 로컬 실행
$ node my-legacy-mcp-server.js

# Fiber 생태계 연동 방식: DPHI A2A 네트워크로의 플러그인 (VPC 내부 실행)
$ fiber connect --target "my-db-server-01" --exec "node my-legacy-mcp-server.js"

```

### 4.2. 커넥터의 아키텍처 매커니즘 (Zero-Trust NAT Traversal)

`fiber connect` 데몬은 가벼운 사이드카 프로세스로 동작하며 다음의 메커니즘을 수행합니다.

1. **Outbound Subscription (인바운드 룰 불필요):** Sidecar는 외부에서 들어오는 트래픽을 열어두지 않고, 내부망(VPC)에서 외부 DPHI Core 망을 향해 아웃바운드 터널(Message Bus)을 열고 자신(`my-db-server-01`)을 향한 인텐트를 당겨옵니다(Pull).
2. **I/O Injection (물리적 브릿징):** 수신한 비동기 이벤트에서 JSON-RPC를 추출하여, 서브프로세스로 실행 중인 레거시 서버의 `stdin`으로 안전하게 밀어 넣습니다.
3. **Deterministic Resolution (결과 보고):** 레거시 서버가 `stdout`으로 결과를 출력하면, 이를 캡처하여 DPHI Core(`rpc.handler`)에 RPC로 보고함으로써 원장 상태를 `RESOLVED`로 전이시킵니다.
4. **The Gateway to WASM (미래로의 전환):** 물리적 실행과 네트워크 통신을 완벽히 분리(`Seperation of Concerns`)했으므로, 제공자는 훗날 `exec` 명령어를 낡은 Node.js 대신 **DPHI WASM 샌드박스 엔진**으로 교체하기만 하면, 클라이언트의 중단 없이 즉시 "초정밀 과금형 스마트 컨트랙트 에이전트"로 전환할 수 있습니다.

---

## 5. 분산 에러 핸들링 매트릭스 (Error Handling Matrix)

Facade 아키텍처에 의해 복잡성은 내부로 숨겨지며, 클라이언트에게는 직관적인 HTTP 상태 코드와 대처 가이드가 제공됩니다.

| HTTP 상태 | 내부 코드 | 에러 메시지 (Error) | 발생 원인 및 클라이언트 대처법 |
| --- | --- | --- | --- |
| **401 Unauthorized** | `-32001` | `CRYPTOGRAPHIC_BINDING_FAILED` | DPoP 서명 검증 실패 또는 SPIFFE 신원 불일치. 새로운 JWT를 서명하여 재시도 요망. |
| **402 Payment Req.** | `-32001` | `KERNEL_AUTHORIZATION_REJECTED` | 할당된 API 예산(Fuel) 고갈 또는 L402 스테이블코인 잔액 부족. 챌린지에 따라 결제 증명 갱신 요망. |
| **502 Bad Gateway** | `-32008` | `LEGACY_SERVER_FAULT: {detail}` | **[Sidecar Fault]** 인텐트 전달은 성공했으나, 타겟 레거시 서버에서 타임아웃/크래시 발생. 원장에 `FAULTED`로 영구 기록됨. |
| **504 Gateway Timeout** | `-32007` | `TRANSACTION_STILL_PROCESSING` | **[Connection Timeout]** 타겟 서버의 연산이 HTTP Holding 시간을 초과함. **반드시 동일한 `x_idempotency_key`로 재요청하여 중복 큐잉 없이 결과를 대기할 것.** |
| **500 Internal** | N/A | `INTERNAL_EDGE_FAILURE` | Gateway 자체 크래시 또는 코어 원장망(RPC Bus) 통신 단절. |