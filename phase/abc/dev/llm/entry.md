# fiber.phase.abc.dev.llm.entry

## 1. Overview

The `fiber.llm` module is an LLM router designed as a drop-in replacement for the OpenAI SDK and LiteLLM. It maintains interface compatibility while its internal architecture utilizes a DPHI Kernel-backed asynchronous Channel Pipeline.

Without requiring modifications to existing codebases relying on OpenAI-compatible SDKs or established LLM routing frameworks, it integrates pipeline capabilities including Fuel-based budget control, dynamic fallbacks, mocking, prompt management, and response normalization.

---

## 2. Core API

### Completion

```python
from fiber.llm.entry import completion, acompletion

# Synchronous execution
response = completion(model="gpt-4o", messages=[{"role": "user", "content": "Hello"}])

# Asynchronous execution
response = await acompletion(model="gpt-4o", messages=[{"role": "user", "content": "Hello"}])

```

### Embedding

```python
from fiber.llm.entry import embedding, aembedding

# Synchronous execution
response = embedding(model="text-embedding-3-small", input=["Hello world"])

# Asynchronous execution
response = await aembedding(model="text-embedding-3-small", input=["Hello world"])

```

---

## 3. Compatibility & Return Types

All return objects map to the OpenAI-compatible Pydantic models defined in `fiber.llm.param`. This preserves existing type hinting and attribute access patterns (e.g., `response.choices[0].message.content`).

| Object Name | Description | Compatibility Mapping (OpenAI / LiteLLM) |
| --- | --- | --- |
| `ModelResponse` | Single completion response | `openai.types.chat.ChatCompletion` |
| `StreamWrapper` | Streaming generator wrapper | `openai.Stream` (yields `ModelResponseStream`) |
| `EmbeddingResponse` | Embedding response | `openai.types.CreateEmbeddingResponse` |
| `Message` | Message object (role and content) | `openai.types.chat.ChatCompletionMessage` |
| `Usage` | Token usage telemetry | `openai.types.CompletionUsage` |

### 3.1. State Translation (Tool Call Normalization)

The pipeline normalizes response structures from heterogeneous LLMs (e.g., Gemini). When providers omit the standard `tool_calls` object or return function calls in provider-specific JSON blocks (e.g., `content.parts.function_call`), the internal `StateMapper` and `StateTraverser` middlewares intercept these structures and parse them into the standard OpenAI `tool_calls` format before returning the response.

---

## 4. Advanced Kwargs

In addition to standard parameters (`model`, `messages`, `temperature`, `stream`), the module accepts extension parameters (`**kwargs`) to configure the internal pipeline middleware.

### 4.1. DPHI Kernel Auth & Fuel Control

Governs kernel-level resource allocation via the `ContextBinder` and `StreamAggregator` middlewares.

* `metadata={"kernel_auth": {"fuel_budget": 1000}}`: Sets the maximum allowed token (Fuel) budget.
* **Fuel Breaker**: During streaming (`stream=True`), if consumed tokens exceed the allocated budget, the pipeline terminates the connection to enforce the budget limit.
* The consumed fuel is recorded in `ModelResponse.usage.fuel_consumed`.
* `audit_hash`: An injected kernel audit hash is embedded into the OpenAI-spec `system_fingerprint` field for auditing purposes.

### 4.2. Model Fallbacks (`FallbackHandler`)

Configures alternative models for retry logic upon encountering downstream failures (e.g., RateLimits, APIErrors).

```python
response = completion(
    model="gemini-3.5-flash",
    messages=[...],
    fallbacks=["gpt-4o-mini", {"model": "claude-3-haiku", "temperature": 0.5}]
)

```

### 4.3. Mocking & Bypass (`MockBypass`)

Bypasses physical LLM network I/O for testing purposes by simulating mock responses or timeouts.

* `mock_response="Mock text"`: Short-circuits the pipeline to return a Pydantic-compatible `ModelResponse`.
* `mock_delay=2.0`: Introduces a 2-second delay before returning the response.
* `mock_timeout=True`: Forces a `TimeoutError` to evaluate fallback or exception-handling logic.

### 4.4. Dynamic Prompting (`PromptTransformer`)

Injects centralized prompt registry templates instead of relying on the hardcoded `messages` array.

* `prompt_id="sys_coder_v2"`: Fetches and injects the specified prompt template at runtime.

### 4.5. Telemetry & Observability (`ChannelObserver`)

* Injecting `session_id="sess_123"` and `trace_id="req_456"` triggers the `executor.telemetry` logger to record lifecycle events and `duration_ms`.
* Bound to the pipeline's `ExecutionMetadata` (`system_meta`), enabling distributed tracing down to the streaming chunk level.

### 4.6. Dynamic Guardrails (`RuleGuardHandler`)

Injects validation rulesets per request without altering global configurations. Supports synchronous and asynchronous validation functions.

```python
async def pii_filter(text: str) -> bool:
    return "SSN" not in text

response = await acompletion(
    model="gpt-4o",
    messages=[...],
    metadata={
        "post_call_rules": [pii_filter] # Pipeline validates responses/streams at runtime
    }
)

```

---

## 5. Implementation Architecture

The entrypoint is implemented as an asynchronous `ChannelPipeline`.

**Execution Sequence (Request Outbound Flow: Tail → Head):**
When a request is initiated, data propagates from the Tail of the pipeline toward the Head prior to network egress.

1. **ContextBinder (Tail)**: Allocates UUIDs, injects kernel authorization (`kernel_auth`), and registers metadata.
2. **ChannelObserver**: Initializes duration tracking and telemetry scopes.
3. **MockBypass**: Checks for `mock_response` attributes; if present, short-circuits the outbound flow.
4. **PromptTransformer**: Injects text based on `prompt_id` and formats tools.
5. **FallbackHandler**: Prepares fallback states to handle recursive retries on downstream errors.
6. **PayloadTranslator**: Invokes `StateMapper` to align heterogeneous requests with the target provider's specifications.
7. **StreamAggregator**: Accumulates stream contexts and initializes Fuel budget tracking.
8. **Transport (Head)**: `CompletionTransport` / `EmbeddingTransport` — Executes the physical network API call.

This architecture allows the framework to execute internal middleware logic within a single call while maintaining interface parity with OpenAI and LiteLLM SDKs.

---

## 6. Edge LLM Gateway Integration (Zero-Trust API)

The `fiber.llm` module includes a FastAPI-based API Gateway (`edge.llm`) for distributed environments and Agent-to-Agent (A2A) communications. It conforms to the OpenAI API specification and integrates with the DPHI infrastructure's economic system (x402).

### 6.1. X-X402-Receipt & Kernel Authorization

External invocations of DPHI computing resources require payment proofs. Before routing the LLM call, the gateway executes the following sequence:

1. **Receipt Extraction**: Extracts the `X-X402-Receipt` from the HTTP headers.
2. **Kernel Intent (DphiBroker)**: Issues an `AUTHORIZE_INTENT` to the WASM kernel to validate the receipt and retrieve the authorized Fuel Budget and audit hash.
3. **Pipeline Injection**: Injects the `kernel_auth` payload into the `metadata` kwargs of the `acompletion` function, delegating budget enforcement to the pipeline middlewares.

### 6.2. Drop-in Replacement

Client-side agents can interact with the DPHI Gateway by updating the Base URL and Headers, without modifying application code.

* **Endpoints:**
* `/v1/chat/completions`
* `/v1/embeddings`


* **Compatibility:** Utilizing Pydantic's `extra="allow"`, extension parameters (`custom_llm_provider`, `fallbacks`) are passed through. The gateway complies with standard Server-Sent Events (SSE) specifications for streaming requests.

### 6.3. Resilience & Error Mapping

Authorization failures and LLM timeouts are mapped to standard HTTP error codes.

* **402 Payment Required**: Triggered upon budget exhaustion or invalid receipts. Includes a `WWW-Authenticate: x402_signed_proof=""` header to initiate automated settlement logic on the agent side.
* **502 Bad Gateway**: Triggered by downstream pipeline errors, returning the origin error context for debugging.