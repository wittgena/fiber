# fiber.phase.abc.gateway.llm.edge
@lineage: fiber.phase.abc.gateway.llm.rest.edge
**@desc:** LLM Gateway & Kernel Authorization Ingress Specification

## 0. Executive Summary

`fiber.dphi.edge.serve.llm` is an HTTP gateway that wraps the internal LLM computation pipeline, `fiber.llm.entry`, into a REST API compliant with OpenAI and LiteLLM standard specifications.

In addition to its role as a reverse proxy, this module acts as a Zero-Trust ingress that enforces **x402 (micro-payment proof) based Kernel Authorization** before any external agent's API request enters the downstream computation pipeline. This controls unauthorized access and runaway costs.

With the introduction of this gateway, the system adopts the following dual resource access architecture:

* **External API (Edge Ingress):** External distributed agents (A2A) consume LLM resources via this gateway's network endpoints alongside payment proofs (x402).
* **Internal SDK (Bypass):** Internal network applications that do not require network communication or payment controls bypass this gateway and directly import the existing `fiber.llm` (Drop-in Replacement).

---

## 1. Kernel Authorization Pipeline

Before routing requests to downstream LLM middlewares (`acompletion`, `aembedding`), the gateway integrates with `DphiBroker` to execute the following 3-step authorization procedure:

1. **Receipt Extraction:** Extracts the `X-X402-Receipt` (payment and audit proof) from the client's HTTP request headers.
2. **Intent Authorization:** Requests validation for the `LLM_COMPUTE` or `LLM_EMBEDDING` computational intent from the WASM kernel (`broker.invoke`).
3. **Metadata Context Binding:** Upon successful authorization, binds the issued `KernelAuthPayload` (allowable budget, audit hash, etc.) along with `client_host` and `x_x402_receipt` data into a `metadata` dictionary, injecting it into the downstream `fiber.llm.entry` pipeline. The downstream pipeline uses this metadata for real-time fuel deduction.

---

## 2. Endpoint Specification

### 2.1. Chat Completions (`POST /v1/chat/completions`)

Endpoint for handling conversational model invocations and tool calling.

* **Request (`ChatCompletionRequest`):**
* Configured with Pydantic's `extra="allow"` to pass-through custom parameters required for downstream pipeline extensions.

* **Streaming Support (`stream=True`):**
* Wraps the `StreamWrapper` returned by the downstream pipeline into FastAPI's `StreamingResponse` to respond with Server-Sent Events (SSE) chunk streaming.
* If the downstream pipeline closes (e.g., due to fuel depletion), it transmits the standard `data: [DONE]` signal to terminate the client-side stream.

* **Response:** `ModelResponse` (Compliant with the standard ChatCompletion specification)

### 2.2. Embeddings (`POST /v1/embeddings`)

Endpoint for generating vector embeddings of documents and texts.

* **Request:** The `input` parameter supports both single string (`str`) and string array (`List[str]`) formats for batch requests.
* **Response:** `EmbeddingResponse` (Compliant with the standard CreateEmbeddingResponse specification)

---

## 3. Error Handling Matrix

The gateway normalizes exceptions occurring during the kernel consensus process and within the downstream pipeline into standard HTTP status codes. Below are the primary HTTP status codes and the client resolution guide.

* **402 Payment Required**
  * **Cause:** **[Kernel Authorization Rejected]** The provided `X-X402-Receipt` has expired, or the allocated compute budget (Fuel) is insufficient.
  * **Resolution:** Reference the `WWW-Authenticate: x402_signed_proof=""` header in the response to re-trigger the agent's payment process.

* **502 Bad Gateway**
  * **Cause:** **[Downstream LLM Error]** A timeout or connection error occurred from the external provider (OpenAI, Anthropic, etc.) during downstream LLM pipeline processing.
  * **Resolution:** It is recommended to check the original error message in the response body (`detail`) and retry applying Exponential Backoff.

* **500 Internal Server Error**
  * **Cause:** **[Gateway Fault]** Gateway crash or the communication with the DPHI kernel bus is disconnected.

---

## 4. Telemetry & Observability

Logging framework for tracking the lifecycle of a single transaction.

* **Flow Scope (Trace Identifier):** Generates a unique identifier (`req_id`: `llm_chat_{timestamp}`) upon each request entry and binds it to the asynchronous `flow_scope` context manager.
* **Audit Logging:** When a kernel authorization fails (402) or a downstream pipeline error occurs (502), the detailed error stack, including the `req_id`, is logged to the `edge.llm` namespace. This data is utilized as audit and verification material in the event of fuel consumption disputes between agents.
