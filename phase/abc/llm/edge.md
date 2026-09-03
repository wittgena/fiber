# fiber.phase.abc.llm.edge
합의된 개선 지점(스테이블코인/M2M 결제 기반 포장 및 2026-07-28 무상태 스펙 명칭 통일)을 완벽하게 반영하여 재작성된 `abc.llm.edge` 문서 전문입니다.

---

# fiber.phase.abc.llm.edge

**@desc:** Zero-Trust LLM & MCP Gateway, Kernel Authorization Ingress & Ecosystem Integration for DPHI

## 1. 개요 (Overview)

`edge.llm`은 DPHI(Deterministic Poly-Harmonic Infrastructure) 네트워크로 진입하는 LLM 추론 및 MCP(Model Context Protocol) 도구 호출을 제어하는 Zero-Trust 기반의 API 인그레스(Ingress) 계층이다.

외부 요청이 호스트 시스템 내부로 유입되기 전에 사전 할당된 M2M 초소액 결제 증명(L402/스테이블코인 정산)과 DPoP(Proof-of-Possession) 서명을 검증하여, 에이전트 런타임을 둘러싼 권한 탈취 및 예산 초과(Runaway cost)를 물리적으로 차단한다. 특히, 현대 에이전트 프로토콜들이 클라이언트에게 외주화(Externalize)해버린 분산 환경의 구조적 복잡성을 엣지(Edge) 단에서 온전히 흡수함으로써, 클라이언트는 기존 코드 수정 없이 DPHI의 하드웨어 수준 격리 인프라와 결합할 수 있다.

---

## 2. Enterprise MCP Transition Bridge (Stateful ↔ 2026-07-28 Stateless Anchor)

**2026-07-28 스펙 도입으로 MCP가 상태 비저장(Stateless) 아키텍처로 전면 전환되면서** 프로토콜 자체는 가벼워졌으나, 매 요청에 대한 암호학적 인증, 동시성 제어(Race condition), 트랜잭션 멱등성 보장이라는 거대한 분산 시스템의 복잡성은 오롯이 엔터프라이즈 클라이언트에게 전가되었다.

게이트웨이 내의 `TransitionBridge`는 이러한 책임 회피적 구조가 낳은 파편화된 Stateless 요청들을 흡수(Complexity Sink)하여, 안전한 결정론적 상태 전이(Deterministic State Transition)로 승화시키는 상태 앵커(State Anchor) 역할을 수행한다.

```text
[ Stateless Chaos (External) ]         [ Transition Bridge (Complexity Sink) ]       [ Deterministic Order ]
                                                                                     
Agent A (MUTATE) + DPoP ──┐           ┌─────────────────────────────────────┐       ┌──────────────────────┐
                          │           │ 1. Cryptographic Ingress            │       │ WASM Kernel Ledger   │
Agent B (MUTATE) + DPoP ──┼─(REST)──▶ │    - Validate SPIFFE & DPoP 서명    │──┐    │ ┌──────────────────┐ │
                          │           │    - Nonce Replay Lock (Redis)      │  │    │ │ Mempool Queue    │ │
Agent C (Replay Attack) ──┘           ├─────────────────────────────────────┤  │    │ │ 1. LogicStream A │ │
                         (Blocked)    │ 2. Topological Translation          │  ├───▶│ │ 2. LogicStream B │ │
                                      │    - JSON -> LogicStream 인텐트 변환│  │    │ └─────────┬────────┘ │
                                      ├─────────────────────────────────────┤  │    │           │          │
                                      │ 3. Asynchronous Consensus           │◀─┘    └───────────┼──────────┘
<── HTTP 202 Accepted (Non-blocking) ─┤    - Acknowledge & Queueing         │                   ▼
                                      └─────────────────────────────────────┘         [ State Sublimation ]


```

* **Stateless Auth & Replay Protection (인증 복잡성 중앙화):** 커넥션 기반 세션이 사라진 자리를 완벽히 대체한다. 단순 API 키에 의존하지 않고, 매 요청마다 에이전트의 식별자(SPIFFE URI)와 RFC 9449 기반 **DPoP 서명**을 검증한다. 더불어 분산 캐시 기반의 난수 락(`NonceReplayProtector`)을 통해 엔터프라이즈 환경을 노린 리플레이 공격을 엣지 단에서 즉시 차단한다.
* **Idempotency & Concurrency Control (멱등성 및 동시성 제어):** 수많은 에이전트가 동시에 상태를 `MUTATE` 하려 할 때 발생하는 충돌과, 네트워크 단절 시 도구 호출이 중복 실행되는 문제(Double-mutate)를 해결한다. 명시적인 `x_idempotency_key`를 강제하며, 유입된 인텐트(`INITIALIZE`, `MUTATE`, `COMMIT`, `QUERY`)를 FSM(유한 상태 기계) 관점에서 추적한다.
* **Deterministic Mempool Queueing (물리적 격리 및 비동기 합의):** 원시 MCP REST 페이로드를 호스트 OS에서 직접 실행(OS-level exec)하지 않는다. 대신, 결정론적 이벤트 객체인 `LogicStream`으로 변환하여 WASM 커널(`dvm.wasm`)의 멤풀(Mempool)로 큐잉(Queueing)한다. 에이전트는 즉시 `202 Accepted`를 반환받아 I/O 블로킹 없이 추론을 이어가고, 커널은 인텐트를 순서대로 정렬하여 명령어 주입(Command Injection) 없이 호스트 상태에 안전하게 반영한다.

---

## 3. 코어 아키텍처 및 요청 라이프사이클 (Core Architecture & Request Lifecycle)

`edge.llm`은 FastAPI 기반의 `ContractRouter`로 구현되며, 모든 LLM 및 MCP 요청은 하위 파이프라인으로 넘어가기 전 엄격한 4단계의 라이프사이클을 거친다.

```text
[ Client Agent ]                    [ edge.llm (Gateway) ]                   [ WASM Kernel & Executor ]
       │                                      │                                          │
       │ 1. HTTP POST /chat/completions       │                                          │
       │    Header: X-X402-Receipt            │                                          │
       ├─────────────────────────────────────▶│                                          │
       │                                      │ 2. invoke("AUTHORIZE_INTENT")            │
       │                                      ├─────────────────────────────────────────▶│
       │                                      │                                          │ (Verify receipt/DPoP, 
       │                                      │ 3. Return kernel_auth                    │  Calculate fuel)
       │                                      │    (fuel_budget, audit_hash)             │
       │                                      ◀─────────────────────────────────────────┤
       │                                      │                                          │
       │                                      │ 4. acompletion() or Bridge Translation   │
       │                                      ├─────────────────────────────────────────▶│
       │ 5. Return Response (or SSE Stream)   │                                          │ (Execute with Kinetic Trap)
       ◀─────────────────────────────────────┼─────────────────────────────────────────┤


```

* **Phase 1: Request Interception & Cryptographic Validation**
클라이언트 요청 페이로드와 함께 `X-X402-Receipt` **(스테이블코인 기반 종량제 결제 영수증)** 또는 `DPoP` (권한 증명) 헤더를 추출하여 인그레스 유효성을 검사한다.
* **Phase 2: Kernel Intent Authorization**
`DphiBroker`를 통해 WASM 커널에 `AUTHORIZE_INTENT`를 호출한다. 커널은 증명 내역을 검증하고 할당할 연산 예산(Fuel Budget)과 상태 씰링을 위한 감사 해시(Audit Hash)를 반환한다.
* **Phase 3: Context Binding & Pipeline Execution**
인가된 커널 정보(`kernel_auth`)를 원본 요청 페이로드와 병합하여 하위 미들웨어로 전달한다. 주입된 Fuel Budget은 스트리밍 중 예산 초과 시 커넥션을 물리적으로 끊어버리는 **Kinetic Trap**의 기준값으로 사용된다.
* **Phase 4: Sealed Response Output**
연산 완료 후 결과 또는 SSE 스트림을 클라이언트에게 반환한다. 응답 객체에는 소모된 연료량과 커널의 Audit Hash가 포함되어 실행의 암호학적 무결성을 증명한다.

---

## 4. 엔드포인트 명세 (Component Specification)

### 4.1. Enterprise MCP State (`/mcp-gateway/state`)

**세션 기반의 기존 MCP 인텐트를 완전 무상태(Stateless, 2026-07-28) 생태계로 브릿징하는 FSM 엔드포인트이다.**

* **Action Routing:** `INITIALIZE`, `MUTATE`, `COMMIT`, `QUERY` 등의 MCP 인텐트를 파싱하여 `TransitionBridge`를 통해 커널의 상태 전이 요청으로 매핑한다.
* **Asynchronous Consensus:** 멤풀에 인텐트가 큐잉되면 `202 Accepted`를 즉시 반환하여 외부 에이전트의 I/O 블로킹을 최소화한다.

### 4.2. Chat Completions (`/chat/completions`)

대화형 모델 호출을 위한 OpenAI 호환 엔드포인트이다.

* **Routing Mechanism:** Pydantic의 `extra="allow"` 설정을 통해 LiteLLM 확장 파라미터(custom_llm_provider, fallbacks 등)를 패스스루(Pass-through) 처리한다.
* **Streaming Handler:** `stream=True` 요청 시, 반환된 `StreamWrapper`를 FastAPI의 `StreamingResponse`에 매핑하여 Server-Sent Events (SSE) 규격으로 반환하되, Fuel 고갈 시 안전한 `[DONE]` 시그널과 함께 스트림 종료를 보장한다.

### 4.3. Embeddings (`/embeddings`)

텍스트 임베딩 생성 엔드포인트이다.

* **Vector Compute Authorization:** 커널 인가를 거쳐 `LLM_EMBEDDING` 인텐트로 분기하며, 처리 토큰에 비례하여 요금을 정밀 산정한다.
* **Batch Processing:** 단일 문자열(String) 및 배열(List) 입력을 모두 지원하여 다중 벡터 변환 처리 오버헤드를 줄인다.

---

## 5. 장애 대응 체계 (Error Handling & Resilience)

분산 에이전트 환경의 안정성을 위해 다음의 예외 처리 규격을 가동한다.

1. **401 Unauthorized / 402 Payment Required:** 유효하지 않은 DPoP 서명이나 잔액 부족 시 발생한다. 헤더에 챌린지(`WWW-Authenticate`)를 포함하여 클라이언트의 정상적인 인증/결제 로직 재시도를 유도한다.
2. **502 Bad Gateway:** 인가 통과 후 하위 파이프라인이나 외부 모델 프로바이더에서 타임아웃 발생 시 반환되며, 원천 에러의 컨텍스트를 안전하게 보존한다.
3. **Telemetry Tracing:** 진입점에서 발급된 `req_id`와 `flow_scope`를 통해 요청의 처리 궤적(Trace)을 관제 시스템에 기록하여 사후 감사를 지원한다.

---

## 6. 타입 호환성 및 생태계 연동 (Type Compatibility & Ecosystem Integration)

`llm.param` 모듈은 내부 커널 데이터 구조와 외부 클라이언트 사이의 매끄러운 타입 브릿지(Type Bridge) 역할을 수행한다.

### 6.1. Native OpenAI Type Inheritance

검증된 코어 타입(`fiber.agent.anchor.model.types.core`)을 상속하여 표준 규격을 완벽히 준수한다.

* `ModelResponse` & `EmbeddingResponse**`: 게이트웨이 최종 반환 객체는 OpenAI SDK 규격(Choices, Message, Content)과 동일한 구조를 갖는다.
* **`Usage`**: 차감된 연산 연료(Fuel) 내역은 표준 `Usage` 객체 내 확장 필드로 포함되어 클라이언트 파서 호환성을 유지한다.

### 6.2. Tool Calling 및 Streaming 지원

WASM 샌드박스에서 처리된 함수 호출 결과와 스트리밍 출력을 규격에 맞게 패스스루 처리한다.

* **`ChatCompletionMessageToolCall`**: WASM 내부 IPC 결과물이나 도구 사용 내역은 표준 ToolCall 스키마로 직렬화되어 반환된다.
* `StreamingChoices` & `Delta**`: SSE 청크는 표준 Delta 객체 규격을 따르며, 예산 초과로 인한 Kinetic Trap 발생 시 파서 오류를 방지하기 위해 엣지 단에서 포맷을 정리(Graceful shutdown)하여 전송한다.