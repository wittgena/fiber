# fiber.phase.abc.llm.edge
**@desc:** Zero-Trust LLM Gateway, Kernel Authorization Ingress & Ecosystem Integration for DPHI

---

## 1. 개요 (Overview)

`edge.llm`은 DPHI(Deterministic Poly-Harmonic Infrastructure) 네트워크로 진입하는 LLM 요청을 제어하는 Zero-Trust 기반의 API 인그레스(Ingress) 계층이다.

외부 요청이 시스템 내부로 유입되기 전에 암호학적 지불 증명(L402/X402 Receipt)을 검증하고, WASM 커널을 통해 연산 예산(Fuel Budget)을 인가받는다. `llm.param` 모듈을 통해 OpenAI API 규격과 호환되도록 설계되어, 클라이언트는 코드 수정 없이 DPHI 인프라에 연동할 수 있다.

---

## 2. 코어 아키텍처 및 요청 라이프사이클 (Core Architecture & Request Lifecycle)

`edge.llm`은 FastAPI 기반의 `ContractRouter`로 구현되며, 모든 요청은 내부 파이프라인(`llm.entry`)으로 넘어가기 전 4단계의 라이프사이클을 거친다.

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
클라이언트 요청 페이로드(Payload)와 함께 `X-X402-Receipt` 헤더를 추출하여 결제망 증명을 검증한다.
* **Phase 2: Kernel Intent Authorization**
`DphiBroker`를 통해 WASM 커널에 `AUTHORIZE_INTENT`를 호출한다. 커널은 결제 내역을 검증하고 할당할 연산 예산(Fuel Budget)과 상태 씰링을 위한 감사 해시(Audit Hash)를 반환한다.
* **Phase 3: Context Binding & Pipeline Execution**
인가된 커널 정보(`kernel_auth`)를 원본 요청 페이로드와 병합하여 하위 미들웨어 파이프라인으로 전달한다. 주입된 Fuel Budget은 `StreamAggregator` 내 Kinetic Trap(커넥션 차단)의 기준값으로 사용된다.
* **Phase 4: Sealed Response Output**
연산 완료 후 결과 또는 SSE 스트림을 클라이언트에게 반환한다. 응답 객체에는 소모된 연료량과 커널의 Audit Hash가 포함되어 결제 무결성을 증명한다.

---

## 3. 엔드포인트 명세 (Component Specification)

### 3.1. Chat Completions (`/chat/completions`)

대화형 모델 호출을 위한 엔드포인트이다.

* **Routing Mechanism:** Pydantic의 `extra="allow"` 설정을 통해 OpenAI 표준 및 LiteLLM 확장 파라미터(custom_llm_provider, fallbacks 등)를 패스스루(Pass-through) 처리한다.
* **Streaming Handler:** `stream=True` 요청 시, 반환된 `StreamWrapper`를 FastAPI의 `StreamingResponse`에 매핑하여 Server-Sent Events (SSE) 규격으로 반환한다.

### 3.2. Embeddings (`/embeddings`)

텍스트 임베딩 생성 엔드포인트이다.

* **Vector Compute Authorization:** 커널 인가를 거쳐 `LLM_EMBEDDING` 인텐트로 분기하며, 처리 토큰에 비례하여 요금을 산정한다.
* **Batch Processing:** 단일 문자열(String) 및 배열(List) 입력을 지원하여 다중 벡터 변환 처리 오버헤드를 줄인다.

---

## 4. 장애 대응 체계 (Error Handling & Resilience)

분산 환경 안정성을 위해 다음의 예외 처리 규격을 가동한다.

1. **402 Payment Required:** 잔액 부족, 영수증 위조, 증명 누락 시 발생한다. 헤더에 `WWW-Authenticate: L402 macaroon=""`을 포함하여 클라이언트의 결제 로직 재시도를 유도한다.
2. **502 Bad Gateway:** 인가 통과 후 하위 파이프라인이나 외부 모델 프로바이더에서 타임아웃 또는 예외 발생 시 반환된다. 원천 에러의 컨텍스트를 보존한다.
3. **Telemetry Tracing:** 진입점에서 발급된 `req_id`와 `flow_scope`를 통해 요청의 처리 궤적(Trace)을 관제 시스템에 기록한다.

---

## 5. 타입 호환성 및 생태계 연동 (Type Compatibility & Ecosystem Integration)

`llm.param` 모듈은 내부 커널 데이터 구조와 외부 클라이언트 사이의 타입 브릿지(Type Bridge) 역할을 수행한다.

### 5.1. Native OpenAI Type Inheritance

검증된 코어 타입(`fiber.agent.anchor.model.types.core`)을 상속하여 표준 규격을 준수한다.

* **`ModelResponse` & `EmbeddingResponse**`: 게이트웨이 최종 반환 객체는 OpenAI SDK 규격(Choices, Message, Content)과 동일한 구조를 갖는다.
* **`Usage`**: 차감된 연산 연료(Fuel) 내역은 표준 `Usage` 객체 내 확장 필드로 포함되거나 토큰 카운트 체계에 매핑되어 클라이언트 파서 호환성을 유지한다.

### 5.2. Tool Calling 및 Streaming 지원

함수 호출(Function Calling)과 스트리밍 출력을 패스스루 처리한다.

* **`ChatCompletionMessageToolCall`**: WASM 샌드박스 내부 IPC 결과물이나 도구 사용 내역은 표준 ToolCall 스키마로 직렬화되어 반환된다.
* **`StreamingChoices` & `Delta**`: SSE 청크는 표준 Delta 객체 규격을 따르며, 예산 초과로 인한 Kinetic Trap 발생 시에도 안전한 종료(Done) 시그널을 전송하여 클라이언트 파서 오류를 방지한다.

### 5.3. Agent Migration

클라이언트는 Base URL을 DPHI Gateway(`edge.llm`)로 변경하고, 요청 헤더에 결제 영수증(`X-X402-Receipt`)을 추가하는 방식으로 기존 AI 워크로드를 DPHI 인프라에 연동할 수 있다. 추가적인 SDK 도입이나 클라이언트 시스템 재작성은 요구되지 않는다.