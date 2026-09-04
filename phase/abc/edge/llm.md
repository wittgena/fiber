# fiber.phase.abc.edge.llm
@desc: Zero-Trust LLM Gateway & Kernel Authorization Ingress Specification

## 0. Executive Summary

`fiber.phase.abc.edge.llm`은 DPHI 생태계 외부의 클라이언트(에이전트)가 내부의 LLM 추론 자원을 소비할 수 있도록 통제하는 **Zero-Trust 기반의 API 인그레스(Ingress) 계층**입니다.

이 모듈의 핵심 역할은 **LiteLLM 및 OpenAI SDK 스펙과 호환되는 REST API**를 제공함과 동시에, 모든 요청이 하위 연산 파이프라인으로 진입하기 전에 L402(초소액 결제 증명) 기반의 커널 인가(Kernel Authorization)를 강제하는 것입니다. 이를 통해 에이전트 간에 발생할 수 있는 권한 탈취 및 예산 초과(Runaway Cost)를 물리적으로 차단합니다.

본 API 게이트웨이는 내부적으로 `fiber.llm.entry` 모듈의 비동기 채널 파이프라인을 래핑(Wrapping)하여 구동되며, 시스템의 보안 요건 및 인프라 환경에 따라 다음과 같이 두 가지 방식으로 병행 운용할 수 있습니다.

* **Edge Ingress Mode (네트워크 API):** 본 문서에 정의된 `edge.llm` 라우터를 통해 구동되며, L402 결제 및 커널 인증이 필수적인 외부 분산 에이전트(A2A)용 네트워크 엔드포인트로 활용됩니다.
* **Native Library Mode (로컬 SDK):** 별도의 네트워크 통신이나 Zero-Trust 인증 계층이 불필요한 내부망(Internal) 애플리케이션의 경우, `import fiber.llm`을 선언하여 기존 OpenAI/LiteLLM 코드를 대체하는 라이브러리 형태로 직접 통합(Drop-in Replacement)이 가능합니다.

---

## 1. 커널 인가 파이프라인 (Kernel Authorization Pipeline)

게이트웨이(`edge.serv.llm`)는 단순한 리버스 프록시가 아니며, 요청을 하위 LLM 미들웨어(`acompletion`, `aembedding`)로 전달하기 전 `DphiBroker`를 통한 3단계 권한 통제를 수행합니다.

1. **Receipt Extraction:** 클라이언트 HTTP 헤더에서 `X-X402-Receipt` (결제 및 감사 증명)를 추출합니다.
2. **Intent Authorization:** WASM 커널(`broker.invoke`)에 `LLM_COMPUTE` 또는 `LLM_EMBEDDING` 인텐트를 검증 요청합니다.
3. **Metadata Context Binding:** 커널 인가가 성공하면, 발급된 `KernelAuthPayload`(허용 예산, 감사 해시 등)를 `client_host`, `x_x402_receipt`와 함께 `metadata` 딕셔너리에 바인딩하여 하위 `fiber.llm.entry` 파이프라인으로 주입합니다. 하위 파이프라인은 이 메타데이터를 기준으로 실시간 연료(Fuel) 차감을 수행합니다.

---

## 2. 핵심 엔드포인트 명세 (Endpoint Specification)

### 2.1. Chat Completions (`POST /v1/chat/completions`)

대화형 모델 호출 및 도구 사용(Tool Calling) 처리를 위한 엔드포인트입니다.

* **Request Body (`ChatCompletionRequest`)**:
* Pydantic의 `extra="allow"` 설정이 적용되어 `custom_llm_provider`, `fallbacks` 등 하위 파이프라인 확장을 위한 파라미터를 패스스루(Pass-through)로 수용합니다.

* **Streaming 지원**:
* `stream=True` 요청 시, 하위 파이프라인에서 반환된 `StreamWrapper`를 FastAPI의 `StreamingResponse`로 래핑하여 Server-Sent Events (SSE) 청크로 스트리밍합니다.
* 연료(Fuel) 고갈 시 하위 파이프라인이 닫히면, 표준 규격인 `data: [DONE]` 시그널과 함께 안전하게 스트림을 종료합니다.

* **Response Model**: `ModelResponse` (표준 ChatCompletion 호환)

### 2.2. Embeddings (`POST /v1/embeddings`)

* **Request Body**:
* `input` 파라미터는 단일 문자열(`str`)과 배열(`List[str]`) 형식을 모두 지원합니다.

* **Response Model**: `EmbeddingResponse` (표준 CreateEmbeddingResponse 호환)

---

## 3. 분산 에러 핸들링 매트릭스 (Error Handling Matrix)

게이트웨이는 커널 합의 및 하위 파이프라인에서 발생하는 예외를 클라이언트가 자동 복구 로직을 가동할 수 있도록 표준 HTTP 상태 코드로 정규화하여 반환합니다.

| HTTP 상태 | 원천 레이어 | 발생 원인 및 클라이언트 대처법 (Resolution) |
| --- | --- | --- |
| **402 Payment Required** | `DphiBroker` | **[Kernel Authorization Rejected]**<br>

<br>제공된 `X-X402-Receipt`가 만료되었거나, 할당된 연산 예산(Fuel)이 부족함.<br>

<br>응답 헤더에 `WWW-Authenticate: L402 macaroon=""`를 포함하여 에이전트의 결제 트리거를 유도함. |
| **502 Bad Gateway** | `fiber.llm` | **[Downstream LLM Error]**<br>

<br>하위 LLM 파이프라인 처리 중 외부 프로바이더(OpenAI, Anthropic 등) 타임아웃 또는 커넥션 오류 발생. 원천 에러 메시지가 `detail`에 포함됨. |
| **500 Internal Error** | `edge.llm` | **[Gateway Fault]**<br>

<br>게이트웨이 자체 크래시 또는 DPHI 커널 버스 통신 단절. |

---

## 4. 텔레메트리 및 추적 (Telemetry & Observability)

단일 트랜잭션의 생애 주기를 추적하기 위해 진입점에서 추적 식별자를 발급합니다.

* **Flow Scope (`req_id`)**: 매 요청 진입 시 고유 식별자(`llm_chat_{timestamp}`)를 생성하여 `flow_scope` 컨텍스트 매니저에 바인딩합니다.
* **로깅 (Logging)**: 하위 파이프라인 에러(502) 및 커널 인가 실패(402) 발생 시 `req_id`와 함께 에러 스택이 `edge.llm` 네임스페이스로 기록되어, 사후 감사(Audit) 및 연료 소모 분쟁 해결에 사용됩니다.