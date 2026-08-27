# phase.doc.llm.edge
**@desc:** Zero-Trust LLM Gateway, Kernel Authorization Ingress & Ecosystem Integration for DPHI

---

## 1. Overview (개요)

`edge.llm`은 DPHI(Deterministic Poly-Harmonic Infrastructure) 네트워크로 진입하는 모든 AI 에이전트의 LLM 요청을 통제하는 **Zero-Trust 기반의 API 인그레스(Ingress) 계층**입니다.

기존의 단순한 리버스 프록시나 로드 밸런서와 달리, 이 게이트웨이는 외부의 비결정론적 요청이 시스템 내부로 유입되기 전에 암호학적 지불 증명(L402/X402 Receipt)을 검증하고, WASM 커널로부터 물리적 연산 예산(Fuel Budget)을 인가받는 문지기 역할을 수행합니다. 동시에 `llm.param` 모듈을 통해 **OpenAI API 스펙과 100% 하위 호환**되도록 설계되어, 에이전트들은 기존 코드의 수정 없이 DPHI 인프라의 강력한 통제 및 경제 시스템에 온보딩할 수 있습니다.

---

## 2. Core Architecture & Request Lifecycle

`edge.llm`은 FastAPI 기반의 `ContractRouter`로 구현되었으며, 모든 요청은 내부 파이프라인(`llm.entry`)으로 넘어가기 전 다음 4단계의 라이프사이클을 거칩니다.

```text
[ Client Agent ]                    [ edge.llm (Gateway) ]                   [ WASM Kernel & Executor ]
       │                                      │                                          │
       │ 1. HTTP POST /chat/completions       │                                          │
       │    Header: X-X402-Receipt            │                                          │
       ├─────────────────────────────────────▶│                                          │
       │                                      │ 2. invoke("AUTHORIZE_INTENT")            │
       │                                      ├─────────────────────────────────────────▶│
       │                                      │                                          │ (Verify receipt, 
       │                                      │ 3. Return kernel_auth                    │  Calculate fuel)
       │                                      │    (fuel_budget, audit_hash)             │
       │                                      ◀─────────────────────────────────────────┤
       │                                      │                                          │
       │                                      │ 4. acompletion() with Pipeline Binding   │
       │                                      ├─────────────────────────────────────────▶│
       │ 5. Return Response (or SSE Stream)   │                                          │ (Execute with Kinetic Trap)
       ◀─────────────────────────────────────┼─────────────────────────────────────────┤

```

* **Phase 1: Request Interception & L402 Validation**
클라이언트로부터 들어온 요청의 Payload와 함께 `X-X402-Receipt` 헤더를 추출합니다. 이는 에이전트 간(A2A) 결제망에서 발행된 영수증으로, DPHI 네트워크의 컴퓨팅 자원을 사용하기 위한 입장권입니다.
* **Phase 2: Kernel Intent Authorization**
`DphiBroker`를 통해 WASM 커널에 `AUTHORIZE_INTENT`를 호출합니다. 커널은 결제 내역을 검증하고 허용된 연료 예산(Fuel Budget)과 상태 씰링을 위한 **Audit Hash**를 반환합니다.
* **Phase 3: Context Binding & Pipeline Execution**
인가된 커널 정보(`kernel_auth`)를 클라이언트의 원래 요청 Payload와 병합하여 하위 미들웨어 파이프라인으로 주입합니다. 이때 주입된 Fuel Budget은 `StreamAggregator`에서 동작하는 Kinetic Trap(물리적 커넥션 절단)의 기준값이 됩니다.
* **Phase 4: Sealed Response Output**
연산이 완료되면 반환된 결과(또는 SSE 스트림)를 클라이언트에게 전달합니다. 응답 객체에는 소모된 연료량과 커널의 Audit Hash가 봉인(Sealed)되어 결제 무결성을 증명합니다.

---

## 3. Component Specification (엔드포인트 명세)

### 3.1. Chat Completions (`/chat/completions`)

대화형 AI 모델을 호출하기 위한 핵심 엔드포인트입니다.

* **Routing Mechanism:** Pydantic의 `extra="allow"` 설정을 통해 OpenAI 표준 파라미터는 물론 LiteLLM 확장 파라미터(custom_llm_provider, fallbacks 등)를 투명하게 패스스루(Pass-through)합니다.
* **Streaming Handler:** `stream=True` 요청 시, 파이프라인에서 반환된 `StreamWrapper`를 FastAPI의 `StreamingResponse`로 매핑하여 Server-Sent Events (SSE) 규격에 맞게 스트림을 푸시합니다.

### 3.2. Embeddings (`/embeddings`)

텍스트 임베딩 생성을 위한 엔드포인트입니다.

* **Vector Compute Authorization:** Chat Completion과 동일하게 커널 인가를 거치며, `LLM_EMBEDDING` 인텐트로 분기하여 연산량(토큰)에 비례하는 요금을 청구합니다.
* **Batch Processing:** 단일 문자열(String) 또는 배열(List)을 모두 지원하여 다중 벡터 변환 시의 네트워크 오버헤드를 최소화합니다.

---

## 4. Error Handling & Resilience (장애 대응 체계)

분산 엣지 환경의 안정성을 위해 명시적인 예외 처리 체계를 가동합니다.

1. **402 Payment Required:** 잔액 부족, 영수증 위조, 결제 증명 누락 시 발생합니다. 헤더에 `WWW-Authenticate: L402 macaroon=""`을 포함시켜 에이전트가 자동 결제 로직을 재트리거하도록 유도합니다.
2. **502 Bad Gateway:** 인가는 통과했으나 하위 LLM 파이프라인이나 외부 모델 프로바이더에서 타임아웃, 예외가 발생한 경우입니다. 원천 에러의 Context를 보존하여 반환합니다.
3. **Telemetry Tracing:** 진입점에서 발급된 `req_id`와 `flow_scope`를 통해 모든 요청의 궤적(Trace)을 중앙 관제 시스템에 기록합니다.

---

## 5. Type Compatibility & Ecosystem Integration (하위 호환성 및 무마찰 마이그레이션)

DPHI 인프라가 제공하는 '암호학적 씰링'과 '물리적 자원 통제'라는 강력한 기능에도 불구하고, 기존 AI 에이전트 생태계가 이를 수용하기 위해 별도의 SDK를 학습하거나 시스템을 재작성할 필요는 없습니다. `llm.param` 모듈은 내부 커널 데이터 구조와 외부 클라이언트 사이의 **완벽한 타입 브릿지(Type Bridge)** 역할을 수행합니다.

### 5.1. Native OpenAI Type Inheritance (표준 스펙 네이티브 상속)

독자적인 스키마를 발명하는 대신, 검증된 코어 타입(`fiber.agent.anchor.model.types.core`)을 상속받아 사용합니다.

* **`ModelResponse` & `EmbeddingResponse**`: 게이트웨이를 통과한 최종 반환 객체는 OpenAI SDK가 기대하는 정확히 그 구조(Choices, Message, Content)를 가집니다.
* **`Usage`**: DPHI 커널이 차감한 연산 연료(Fuel) 내역은 표준 `Usage` 객체 내부에 확장 필드로 은닉되거나 기존 토큰 카운트 체계에 매핑되어 클라이언트의 토큰 파싱 로직을 붕괴시키지 않습니다.

### 5.2. 무손실 Tool Calling 및 Streaming 지원

LangChain, AutoGen 등 최신 자율형 AI 에이전트의 핵심 기능인 함수 호출(Function Calling)과 스트리밍 출력 역시 무손실로 패스스루됩니다.

* **`ChatCompletionMessageToolCall`**: WASM 샌드박스 내부에서 발생한 IPC 결과물이나 도구 사용 내역은 표준 ToolCall 스키마로 직렬화되어 반환됩니다.
* **`StreamingChoices` & `Delta**`: SSE로 밀어내는 스트림 청크는 표준 Delta 객체 규격을 따르며, 예산 초과로 인한 Kinetic Trap(커넥션 강제 절단)이 발생하더라도 클라이언트 파서가 크래시되지 않도록 안전한 종료(Done) 시그널을 보장합니다.

### 5.3. Zero-Friction Agent Migration

결과적으로 DPHI 네트워크에 참여하고자 하는 AI 개발자는 코드를 수정할 필요가 없습니다. **오직 Base URL을 DPHI Gateway(`edge.llm`)로 변경**하고, 헤더에 결제 영수증(`X-X402-Receipt`)을 추가하는 것만으로 기존의 비결정론적 AI 워크로드를 DPHI의 결정론적 통제망 및 에이전트 경제(Agent Economy) 인프라 위로 즉시 온보딩(Onboarding)할 수 있습니다.
