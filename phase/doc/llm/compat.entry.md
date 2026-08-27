# fiber.phase.doc.llm.compat.entry
@lineage: phase.doc.llm.compat.entry

## 1. Overview

`fiber.llm` 모듈은 **LiteLLM 및 OpenAI SDK와 100% 호환되는 인터페이스**를 제공하면서, 내부적으로는 DPHI Kernel 기반의 비동기 채널 파이프라인(Channel Pipeline)을 통해 실행되는 고성능 LLM 라우터입니다.

기존에 LiteLLM이나 OpenAI API를 사용하던 코드를 변경할 필요 없이 **Drop-in Replacement**로 사용할 수 있으며, 추가적으로 연료(Fuel) 기반 과금 통제, 동적 폴백(Fallback), 목업(Mocking), 프롬프트 관리, 그리고 응답 규격 정규화 기능을 투명하게 제공합니다.

---

## 2. Core API (핵심 진입점)

LiteLLM과 동일하게 4가지 전역 함수(Global Entrypoints)를 제공합니다. 동기(Sync) 함수는 내부적으로 `nest_asyncio`를 활용하여 이미 실행 중인 이벤트 루프 내에서도 충돌 없이 안전하게 동작합니다.

### Completion

```python
from fiber.llm import completion, acompletion

# Synchronous
response = completion(model="gpt-4o", messages=[{"role": "user", "content": "Hello"}])

# Asynchronous
response = await acompletion(model="gpt-4o", messages=[{"role": "user", "content": "Hello"}])

```

### Embedding (임베딩)

```python
from fiber.llm import embedding, aembedding

# Synchronous
response = embedding(model="text-embedding-3-small", input=["Hello world"])

# Asynchronous
response = await aembedding(model="text-embedding-3-small", input=["Hello world"])

```

---

## 3. Compatibility & Return Types

모든 반환 객체는 `llm.param`에 정의된 **OpenAI 호환 Pydantic 모델**을 따릅니다. 따라서 기존 시스템의 타입 힌팅이나 속성 접근(e.g., `response.choices[0].message.content`)이 그대로 유지됩니다.

| 객체명 | 설명 | 호환성 매핑 (OpenAI/LiteLLM) |
| --- | --- | --- |
| `ModelResponse` | 단일 Completion 반환 객체 | `openai.types.chat.ChatCompletion` |
| `StreamWrapper` | 스트리밍 제너레이터 래퍼 | `openai.Stream` (yields `ModelResponseStream`) |
| `EmbeddingResponse` | 임베딩 반환 객체 | `openai.types.CreateEmbeddingResponse` |
| `Message` | 역할 및 내용을 포함한 메시지 | `openai.types.chat.ChatCompletionMessage` |
| `Usage` | 토큰 사용량 정보 | `openai.types.CompletionUsage` |

### 3.1. Declarative State Translation (Tool Call Recovery)

단순한 Pydantic 래핑을 넘어, 이기종 LLM(Gemini 등)이 `tool_calls` 객체를 누락하고 비표준 JSON 블록(`content.parts.function_call` 등)으로 응답을 반환하는 심각한 규격 위반(Leakage)을 방어합니다.
내부의 `StateMapper`와 `StateTraverser` 미들웨어가 선언적 규칙을 통해 누수된 함수 호출을 찾아내어, 완벽한 OpenAI `tool_calls` 포맷으로 강제 복원(Normalization)하여 반환합니다.

---

## 4. Advanced Kwargs (확장 기능 파라미터)

표준 파라미터(`model`, `messages`, `temperature`, `stream` 등) 외에도, 파이프라인 미들웨어를 제어하기 위한 강력한 확장 파라미터(`**kwargs`)를 제공합니다.

### 4.1. DPHI Kernel Auth & Fuel Control

`ContextBinder` 및 `StreamAggregator` 미들웨어를 통해 커널 레벨의 자원 통제를 지원합니다.

* `metadata={"kernel_auth": {"fuel_budget": 1000}}`: 허용된 토큰(Fuel) 예산을 설정합니다.
* **Kinetic Membrane (Fuel Trap)**: 스트리밍(`stream=True`) 중 지정된 연료를 초과하면 물리적으로 커넥션을 절단(Kill-switch)하여 파산을 방지합니다.
* 반환된 `ModelResponse.usage.fuel_consumed`에 실제 사용된 연료량이 캡처됩니다.
* `audit_hash`: 커널 감사 해시를 주입하면, 보안상 OpenAI 스펙의 `system_fingerprint` 필드에 은닉(Steganography)되어 반환 및 보관됩니다.

### 4.2. Model Fallbacks (`FallbackHandler`)

요청 실패(RateLimit, APIError 등) 시 자동으로 다른 모델로 재시도합니다.

```python
response = completion(
    model="gemini-3.5-flash",
    messages=[...],
    fallbacks=["gpt-4o-mini", {"model": "claude-3-haiku", "temperature": 0.5}]
)

```

### 4.3. Mocking & Bypass (`MockBypass`)

테스트 환경을 위해 실제 LLM 호출을 건너뛰고 가짜 응답이나 지연/타임아웃을 시뮬레이션할 수 있습니다.

* `mock_response="가짜 응답 텍스트"`: API 호출 없이 즉시 Pydantic 호환 응답 객체(`ModelResponse`) 반환.
* `mock_delay=2.0`: 응답 전 2초간 의도적 지연.
* `mock_timeout=True`: 강제로 `TimeoutError`를 발생시켜 폴백/예외 처리 로직 테스트.

### 4.4. Dynamic Prompting (`PromptTransformer`)

하드코딩된 `messages` 대신 중앙화된 프롬프트 레지스트리 ID를 사용할 수 있습니다.

* `prompt_id="sys_coder_v2"`: 파이프라인이 런타임에 최신 프롬프트를 페치하여 주입합니다.

### 4.5. Telemetry & Observability (`ChannelObserver`)

* `session_id="sess_123"`, `trace_id="req_456"` (또는 `metadata` 내부에 포함): 분산 트레이싱을 위해 주입하면 `executor.telemetry` 로거가 자동으로 시작/종료/오류 및 소요 시간(`duration_ms`)을 트래킹합니다.
* 파이프라인 전체를 관통하는 `system_meta`(ExecutionMetadata)로 래핑되어 스트리밍 청크 단위의 TTFT(Time To First Token)까지 일관되게 추적됩니다.

### 4.6. Dynamic Guardrails (`RuleGuardHandler`)

전역 설정(Global Config)에 의존하지 않고, 각 요청(Request) 단위로 독립적인 유해성 필터나 포맷 검증 룰셋을 동적으로 주입할 수 있습니다. 동기/비동기 검증 함수를 모두 완벽하게 지원합니다.

```python
async def pii_filter(text: str) -> bool:
    return "SSN" not in text

response = await acompletion(
    model="gpt-4o",
    messages=[...],
    metadata={
        "post_call_rules": [pii_filter] # 파이프라인이 런타임에 룰을 가로채어 응답/스트림을 검증
    }
)

```

---

## 5. Implementation Architecture

해당 엔트리는 단순한 함수 래퍼가 아닌, Netty 스타일의 비동기 채널 파이프라인(`ChannelPipeline`)으로 설계되었습니다.

**실행 순서 (Head → Tail):**

1. **Transport (Head)**: `CompletionTransport` / `EmbeddingTransport` - 실제 LLM 공급자 API 호출.
2. **StreamAggregator**: 스트림 청크 누적, 내부 파이프라인(RuleGuard 등) 생성 및 **Fuel 예산 실시간 차감**.
3. **PayloadTranslator**: `StateMapper`를 호출하여 이기종 응답을 OpenAI 규격(`ModelResponse`)으로 정규화(Normalization).
4. **FallbackHandler**: 에러 발생 시 Fallback 모델로 재귀(Retry).
5. **PromptTransformer**: `prompt_id` 기반 텍스트 주입 및 `tools` 정규화.
6. **MockBypass**: `mock_response` 감지 시 Transport 진입 전 루프 숏컷(Short-circuit).
7. **ChannelObserver**: 소요 시간 계산 및 로깅 텔레메트리 부착.
8. **ContextBinder (Tail)**: UUID 할당 및 커널 인가(`kernel_auth`), 동적 가드레일(`post_call_rules`) 메타데이터 등록.

이 파이프라인 구조 덕분에, 기존 OpenAI / LiteLLM 코드와 100% 호환되면서도 거대한 엔터프라이즈급 확장 로직을 단일 호출 안에서 투명하게 처리할 수 있습니다.

---

## 6. Edge LLM Gateway Integration (Zero-Trust API)

`fiber.llm`은 파이썬 함수 형태의 진입점 외에도, 분산 환경 및 다중 에이전트(A2A) 통신을 위한 FastAPI 기반의 Zero-Trust API 게이트웨이(`edge.llm`)를 기본 제공합니다. 이 게이트웨이는 OpenAI API 스펙과 100% 호환되며, DPHI 인프라의 경제 시스템(L402)과 강력하게 결합되어 있습니다.

### 6.1. X-X402-Receipt & Kernel Authorization

외부에서 DPHI 네트워크의 컴퓨팅 자원을 호출하려면 물리적 예산 증명이 필요합니다.
게이트웨이는 LLM 호출을 파이프라인으로 넘기기 전, 다음 절차를 엄격하게 수행합니다.

1. **Receipt Extraction**: HTTP 헤더에서 `X-X402-Receipt` (지불 증명/L402 Macaroon)를 추출합니다.
2. **Kernel Intent (DphiBroker)**: WASM 커널에 `AUTHORIZE_INTENT`를 호출하여 영수증을 검증하고, 사용 가능한 연료 예산(Fuel Budget)과 상태 씰링(Audit Hash)을 발급받습니다.
3. **Pipeline Injection**: 발급된 커널 인가 정보(`kernel_auth`)를 `acompletion` 함수의 `metadata` `kwargs`로 주입하여, 파이프라인 미들웨어(ContextBinder, StreamAggregator)가 예산을 통제하도록 위임합니다.

### 6.2. Drop-in Replacement

클라이언트 측 에이전트는 코드 수정 없이 **Base URL과 Header만 변경**하여 DPHI 게이트웨이를 사용할 수 있습니다.

* **엔드포인트:**
* `/v1/chat/completions`
* `/v1/embeddings`


* **호환성 보장:** Pydantic `extra="allow"`를 통해 `custom_llm_provider`, `fallbacks` 등의 확장 파라미터를 그대로 통과(Pass-through)시키며, 스트리밍(`stream=True`) 요청 시에도 SSE(Server-Sent Events) 규격을 엄격히 준수합니다.

### 6.3. Resilience & Error Mapping

인가 실패나 LLM 호출 중 타임아웃 발생 시, 표준화된 HTTP 에러로 매핑하여 반환합니다.

* **402 Payment Required**: 예산 고갈, 영수증 위조 시 발생하며, `WWW-Authenticate: L402 macaroon=""` 헤더를 포함하여 에이전트의 자동 결제 로직을 재트리거합니다.
* **502 Bad Gateway**: 하위 파이프라인에서 오류가 발생할 경우 원천 에러 컨텍스트를 보존하여 반환합니다.