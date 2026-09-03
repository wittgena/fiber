새롭게 확립된 "순수 이벤트 소싱(Event Sourcing) 기반의 상태 전이"와 **"Egress Sidecar를 통한 완벽한 물리적 격리"** 아키텍처를 온전히 반영하여, 스펙 문서를 대폭 고도화 및 재정렬했습니다.

이 문서는 이제 단순한 API 명세서를 넘어, Fiber 생태계가 기존 MCP 서버(Web2)를 어떻게 완벽한 Zero-Friction으로 포용하는지 보여주는 **엔드투엔드(End-to-End) 연동 백서**로 기능합니다.

---

# fiber.phase.abc.llm.mcp.bridge.spec

**@desc:** Fiber MCP Transition Bridge & Egress Sidecar Integration Specification
**@version:** 3.0 (Deterministic FSM & Zero-Friction Edition, 2026.09.03)

---

## 0. 스펙의 목적 및 연동 효과 (Executive Summary)

본 스펙은 2026-07-28에 발표된 MCP 무상태(Stateless) 스펙 도입으로 인해 발생한 분산 트랜잭션의 복잡성을 해소하고, 타사(3rd Party) MCP 서버를 Fiber의 결정론적 원장(Ledger) 생태계에 편입시키기 위한 **통합 연동 SPEC**입니다. 본 규격을 적용할 경우 양 주체(Client / Provider)에 다음의 극단적인 통합 환경이 제공됩니다.

1. **보안 및 과금의 중앙화 (Thin Gateway):** Gateway가 DPoP(B2B 신원 증명) 및 L402(M2M 초소액 결제) 인증을 전담하여, 제공자(Provider)의 검증 오버헤드를 완벽히 제거합니다.
2. **사이드카 기반 Zero-Code 연동 (The Connector):** 서버 제공자는 인바운드 방화벽을 열거나 기존 서버 코드를 단 한 줄도 수정할 필요가 없습니다. 제공되는 경량 커넥터 데몬(Sidecar)을 기존 서버 옆에 띄우기만 하면 망분리(Air-gapped) 환경에서도 완벽히 연동됩니다.
3. **결정론적 상태 전이 보장 (Event Sourcing):** 네트워크 단절이나 백엔드 타임아웃 발생 시에도, 트랜잭션은 DB 업데이트가 아닌 원장 내 비가역적 상태 전이(LogicStream)로 관리되어 완벽한 멱등성(Idempotency)과 복구력을 보장합니다.

---

## 1. 아키텍처 원칙: 삼위일체 (The Triad Boundary)

본 생태계는 외부의 혼란(HTTP)과 내부의 질서(WASM Ledger)를 분리하기 위해 3단계의 아키텍처 경계선을 가집니다.

* **[Edge] Transition Bridge (`mcp.bridge`):** 클라이언트의 REST 요청을 받아 인증을 수행하고, 이를 즉시 "비동기 인텐트(Intent)"로 변환하여 내부 메시지 버스에 브로드캐스트합니다. (응답 대기 없음).
* **[Core] Kernel Ledger (`rpc.handler`):** 모든 MCP 트랜잭션의 생애 주기를 `PENDING` ➔ `RESOLVED` ➔ `FAULTED` 의 상태 전이(`LogicStream`)로 원장에 영구 기록합니다.
* **[Egress] Worker Connector (`worker.connector`):** 기존 MCP 서버 옆에 배치되는 플러그(Plug)입니다. 내부 버스를 구독(Pull)하다가, 순수 MCP JSON-RPC를 추출하여 레거시 서버의 표준 입출력(`stdio`)으로 밀어 넣고 그 결과를 다시 원장에 보고합니다.

---

## 2. Ingress: 클라이언트 연동 규격 (Client ➔ Gateway)

클라이언트는 대상 MCP 서버의 내부 구조를 알 필요 없이, Gateway의 단일 엔드포인트로 상태 기반 통신을 수행합니다.

* **Endpoint:** `POST /v1/mcp-gateway/{target_server_id}/state`
* **Content-Type:** `application/json`

### 2.1. 투-트랙(Two-Track) 인증 및 제어 헤더

| 헤더 필드 | 타입 | 필수 여부 | 설명 |
| --- | --- | --- | --- |
| `x_idempotency_key` | String | **필수** | 중복 실행 방지용 고유 UUID. 타임아웃 재시도 시 반드시 동일한 키 사용. |
| `x_nonce` | String | **필수** | Replay Attack 방지용 난수 (Gateway Redis에서 300초간 Lock 처리됨). |
| `X-X402-Receipt` | String | 선택 (Track A) | **[종량제 과금용] M2M(기계 간) 초소액 API 결제 증명 (L402 Macaroon).** |
| `x_spiffe_id` | String | 선택 (Track B) | [엔터프라이즈용] 에이전트의 SPIFFE 신원 URI. |
| `DPoP` | String | 선택 (Track B) | [엔터프라이즈용] RFC 9449 토큰 탈취 방지 서명. **(Ed25519, P-256, RSA 지원)** |

### 2.2. Request Body Schema (FSM Envelope)

```json
{
  "action": "COMMIT", 
  "handle_id": "mcp_txn_8f9a2b...",
  "payload": {
    "jsonrpc": "2.0",
    "method": "tools/call",
    "params": {
      "name": "query_database",
      "arguments": {"sql": "SELECT * FROM users"}
    }
  }
}

```

---

## 3. 트랜잭션 라이프사이클 (Deterministic FSM)

모든 요청은 즉시 비동기 큐에 위임(Fire-and-forget)되며, 클라이언트는 `QUERY`를 통해 원장(Ledger)의 궤적을 투영(Projection)하여 결과를 얻습니다.

1. **`INITIALIZE` (트랜잭션 초기화 제안):**
* **Client:** `action="INITIALIZE"` (Payload = MCP initialize 규격)
* **Gateway:** 신규 `handle_id` 발급 후, Connector에게 비동기 브로드캐스트. **즉시 HTTP 200 반환.**
* *비고: 클라이언트는 발급받은 ID로 `QUERY`를 폴링하여 타겟 서버의 `capabilities`를 획득합니다.*


2. **`MUTATE` (상태 누적):**
* **Client:** `action="MUTATE"`, 실제 Payload 덩어리 전송.
* **Gateway:** 원장 멤풀(Mempool) 버퍼에 안전하게 조각 보관 (HTTP 200).


3. **`COMMIT` (실행 확정 - 논블로킹):**
* **Client:** `action="COMMIT"`, 병합될 `handle_id` 전달.
* **Gateway:** 원장 상태를 `PENDING`으로 전이(`LogicStream` 씰링). 이후 Connector를 깨움. **즉시 HTTP 202 Accepted 반환.**


4. **`QUERY` (결과 폴링 및 상태 투영):**
* **Client:** `action="QUERY"`로 주기적 폴링 (Long-polling 권장).
* **Gateway:** 원장에 기록된 최신 `LogicStream`을 스캔.
* 상태가 아직 `PENDING`이면 ➔ `status: pending` 반환
* Connector가 결과를 보고하여 `RESOLVED`로 전이되었다면 ➔ `data` 필드에 실제 MCP 결과 반환
* 에러로 인해 `FAULTED`로 전이되었다면 ➔ `status: faulted` 및 에러 내역 반환





---

## 4. Egress: 서버 연동 규격 (Egress Sidecar Connector)

기존 MCP 서버 제공자(Provider)는 DPHI 생태계에 연동하기 위해 **자신의 서버 코드를 1 byte도 수정할 필요가 없으며, 인바운드 방화벽(Port Forwarding)을 개방할 필요도 없습니다.**

대신, Fiber가 제공하는 극경량 파이썬 데몬인 `fiber-connector`를 기존 서버의 실행 래퍼(Wrapper)로 사용합니다.

### 4.1. 배포 및 실행 방법 (Zero-Friction Deployment)

```bash
# 기존 방식: 
$ node my-legacy-mcp-server.js

# Fiber 생태계 연동 방식 (VPC 내부 망에서 실행):
$ python -m fiber.dphi.infra.worker.connector \
    --target "my-db-server-01" \
    --exec "node my-legacy-mcp-server.js"

```

### 4.2. Connector의 내부 동작 메커니즘 (Pull & Inject)

1. **Pull (구독):** Connector는 외부망으로 나가는 아웃바운드(Outbound) 터널을 열고, DPHI Message Bus에서 `my-db-server-01`을 향한 `PENDING` 인텐트 이벤트를 낚아챕니다.
2. **Inject (주입):** 수신한 이벤트에서 순수 JSON-RPC 페이로드를 추출하여, 자식 프로세스로 띄워둔 레거시 서버의 표준 입력(`stdin`)으로 밀어 넣습니다.
3. **Resolve (상태 전이 보고):** 레거시 서버가 표준 출력(`stdout`)으로 뱉어낸 결과값을 캡처한 뒤, DPHI 코어에 RPC를 쏘아 원장 상태를 `RESOLVED`로 전이(`LogicStream` 봉인) 시킵니다.

---

## 5. 에러 핸들링 매트릭스 (Error Handling)

FSM 아키텍처에 기반한 통합 에러 규격입니다.

| HTTP 상태 | Internal Code | 에러 메시지 (Error) | 발생 원인 및 클라이언트 대처법 |
| --- | --- | --- | --- |
| **401 Unauthorized** | `-32001` | `CRYPTOGRAPHIC_BINDING_FAILED` | DPoP 서명 검증 실패 또는 SPIFFE 불일치. 새로운 DPoP JWT를 생성하여 재시도. |
| **402 Payment Req.** | `-32001` | `KERNEL_AUTHORIZATION_REJECTED` | 할당된 예산(Fuel) 고갈 또는 L402 결제 잔액 부족. 반환된 챌린지에 따라 결제 증명 갱신 요망. |
| **200 OK**<br>

<br>*(QUERY 시)* | `-32008` | `status: faulted` (in body) | **[Sidecar Fault]** Gateway 전송은 성공했으나, 타겟 서버에서 타임아웃/크래시 발생. 원장에 `FAULTED`로 영구 기록됨. |
| **423 Locked** | `-32009` | `WASM_EXECUTION_REJECTED_OR_LOCKED` | Replay Attack 감지. 동일 `nonce` 사용됨. 이미 처리 중인 트랜잭션이므로 `QUERY`로 폴링 전환. |
| **500 Internal** | N/A | `INTERNAL_EDGE_FAILURE` | Gateway 자체 크래시 또는 코어 원장망 통신(RPC Bus) 완전 단절. |