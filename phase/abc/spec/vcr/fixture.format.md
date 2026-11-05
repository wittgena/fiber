# fiber.phase.abc.spec.vcr.fixture.format
@desc: Fiber VCR Fixture Canonical Data Specification

Fiber serializes LLM and tool execution traffic into local JSON fixtures (cassettes) to enable deterministic offline replay, sub-millisecond regression testing, and historical latency emulation. 

The format employs a **Dual-Layer Architecture**:
1. **Canonical Replay Layer (`response_timeline`, `usage`):** Provider-agnostic, normalized representations (coalesced text chunks, OpenAI-compatible tool calls, and standard token counts) for zero-overhead application replay.
2. **Raw Verification Layer (`raw_payload`):** Optional, unmutated upstream provider payloads (`sync_response` or `stream_chunks`) preserved to validate declarative Schema Traverser extraction rules offline.

---

## 1. Canonical JSON Schema

```json
{
  "scenario": "String (Scenario identifier from 'vcr_scenario', defaults to 'auto')",
  "latest_trace_id": "String (32-char hex pointer to the active trace in 'traces')",
  "history_keys": [
    "Array of Strings (Ordered list of recorded trace_ids, capped at max 5 for ring-buffer retention)"
  ],
  "traces": {
    "{trace_id}": {
      "trace_id": "String (Unique 32-char hex identifier for this specific execution trace)",
      "request_context": {
        "model": "String (Normalized model identifier without provider prefix, e.g., 'claude-haiku-4-5-20251001')",
        "provider": "String (Resolved upstream provider, e.g., 'anthropic', 'openai', 'ollama')",
        "messages": [
          {
            "role": "String ('system' | 'user' | 'assistant' | 'tool')",
            "content": "String or Array (Sanitized request message payload)"
          }
        ],
        "parameters": {
          "temperature": "Float or null (Sampling temperature if specified)",
          "max_tokens": "Integer or null (Max output tokens if specified)",
          "stream": "Boolean (True for SSE streaming, False for unary sync execution)"
        }
      },
      "network_metrics": {
        "ttfb_ms": "Float (Time-To-First-Byte in ms; equals total_duration_ms when stream is false)",
        "total_duration_ms": "Float (Total wall-clock duration from request dispatch to stream/sync completion in ms)"
      },
      "response_timeline": [
        {
          "delta_ms": "Float (Time elapsed since previous frame in ms, coalesced by record_tick_ms; 0.0 for sync)",
          "chunk": "String (Normalized text delta extracted by Schema Traverser; empty string '' on pure tool calls)",
          "tool_calls": [
            {
              "id": "String (Normalized call identifier, e.g., 'call_018c9dc8')",
              "type": "String (Fixed to 'function')",
              "function": {
                "name": "String (Target tool/function name)",
                "arguments": "String (Serialized JSON object of arguments)"
              }
            }
          ],
          "finish_reason": "String (Optional. Present only on terminal frames, e.g., 'stop', 'end_turn', 'tool_calls')"
        }
      ],
      "exception_boundary": {
        "occurred": "Boolean (True if execution terminated via exception or mid-stream kill-switch)",
        "error_type": "String (Optional. Exception class name, e.g., 'PermissionError', 'TimeoutError')",
        "message": "String (Optional. Exception detail or guardrail block reason)"
      },
      "usage": {
        "prompt_tokens": "Integer (Normalized input token count)",
        "completion_tokens": "Integer (Normalized output token count)",
        "total_tokens": "Integer (Sum of prompt_tokens and completion_tokens)"
      },
      "raw_payload": {
        "sync_response": "Object (Optional. Present when stream=false; exact upstream JSON response)",
        "stream_chunks": [
          "Array of Objects (Optional. Present when stream=true; raw uncoalesced SSE JSON chunks)"
        ]
      }
    }
  }
}
```

---

## 2. Core Structural Specifications

### 2.1. Multi-Trace Ring Buffer (`latest_trace_id` & `history_keys`)

A single fixture file acts as a self-contained cassette for a named `scenario`.

* **`latest_trace_id`:** Defines the default execution state loaded during standard offline replay (`VCR_MODE=replay`).
* **`history_keys`:** Maintains up to 5 chronological `trace_id` entries. Recording a new trace appends its ID to `history_keys`, updates `latest_trace_id`, and evicts the oldest trace from `traces` once the cap is exceeded.

### 2.2. Execution Mode Semantics (`response_timeline`)

Regardless of whether the upstream call is streaming, synchronous, or invoking tools, the output is unified into the `response_timeline` array:

| Mode | `parameters.stream` | `response_timeline` Behavior |
| --- | --- | --- |
| **SSE Stream** | `true` | Contains **1..N frames**. Micro-chunks arriving within the `record_tick_ms` window (e.g., 100ms) are coalesced into a single frame. The first frame's `delta_ms` matches `ttfb_ms`. Only the final frame carries `finish_reason`. |
| **Unary Sync** | `false` | Contains **exactly 1 frame** with `delta_ms: 0` and the full text in `chunk`. (`finish_reason` may be omitted for standard text completions). |
| **Tool Call** | `true` or `false` | The terminal frame sets `chunk: ""` (or preceding thought text), populates the normalized `tool_calls` array (`arguments` serialized as a JSON string), and sets `finish_reason: "tool_calls"`. |

### 2.3. Exception & Kill-Switch Boundary (`exception_boundary`)

To deterministically replay negative test cases (network timeouts, DLP guardrail blocks, or `StreamBreaker` quota cutoffs):

* When `occurred: false`, `error_type` and `message` are omitted.
* When `occurred: true` during a stream, `response_timeline` preserves all pre-cutoff chunks emitted prior to termination, and the replay engine raises the recorded `error_type` immediately after yielding the final recorded frame.

### 2.4. Raw Payload Extension (`raw_payload`)

While application tests rely solely on `response_timeline` and `usage` (allowing synthetic fixtures to omit `raw_payload` entirely), live recordings preserve the upstream provider's native structure inside `raw_payload`:

* **`raw_payload.sync_response` (Object):** Captures the unparsed unary JSON body (e.g., Anthropic's `content[].type="tool_use"` or custom usage fields) when `stream` is `false`.
* **`raw_payload.stream_chunks` (Array of Objects):** Captures every individual, uncoalesced SSE chunk before time-window merging when `stream` is `true`.
* **Verification Contract:** Schema Traverser validation suites feed `raw_payload` as raw input into declarative extraction rules and assert that the parsed output exactly matches the canonical `response_timeline` and `usage` blocks.