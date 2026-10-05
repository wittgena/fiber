# fiber.phase.abc.dev.vcr.fixture.format
@desc: Fiber VCR Fixture Specification

Fiber serializes LLM network traffic into local JSON fixtures to enable deterministic offline testing and exact latency emulation. The fixture format abstracts away raw provider-specific payloads, storing only the normalized text, token usage, and precise timing metrics required for seamless application-level replays.

## Fixture JSON Structure

```json
{
  "scenario": "String (The 'vcr_scenario' value from metadata, defaults to 'auto')",
  "latest_trace_id": "String (The trace_id of the most recently recorded fixture)",
  "history_keys": [
    "Array of Strings (List of recent trace_ids, strictly capped at a maximum of 5)"
  ],
  "traces": {
    "{trace_id}": {
      "trace_id": "String (Unique identifier for the execution trace)",
      "request_context": {
        "model": "String (e.g., 'gemma:2b', 'gpt-4o')",
        "provider": "String (e.g., 'ollama', 'openai')",
        "messages": [
          "Array of Objects (List of message dictionaries from the original request)"
        ],
        "parameters": {
          "temperature": "Number or null (Original kwargs value)",
          "max_tokens": "Number or null (Original kwargs value)",
          "stream": "Boolean (Defaults to false)"
        }
      },
      "network_metrics": {
        "ttfb_ms": "Float (Time-to-first-byte for streams, or total duration for sync responses, in ms)",
        "total_duration_ms": "Float (Total time elapsed until stream completion or sync return, in ms)"
      },
      // optional
      "raw_payloads": [
        "Array of Strings (Optional. Raw, unparsed JSON chunks/responses directly from the provider)"
      ],
      "response_timeline": [
        {
          "delta_ms": "Float (Time elapsed since the previous chunk, buffered by tick_ms)",
          "chunk": "String (Pure extracted text parsed by the State Traverser)",
          "finish_reason": "String (Stream termination reason; key exists only when a value is present)"
        }
      ],
      "usage": {
        "Key-Value pairs (Provider-specific token usage data; object is created only if valid, otherwise null)"
      },
      "exception_boundary": {
        "occurred": "Boolean (Indicates if an error happened during execution)",
        "error_type": "String or null (Exception class name, e.g., 'TimeoutError')",
        "message": "String or null (Raw exception message body)",
      }
    }
  }
}
```

## Raw Payload Extension for Rule Validation
To support the offline validation of State Traverser rules (e.g., within the fiber-compats repository) without breaking application-level replay compatibility, the following optional fields can be injected into the fixture structure:

### raw_payloads (Array of Strings):
Appended at the root of the {trace_id} object. This array captures the unparsed, raw network chunks or sync responses exactly as received from the provider. It serves as the original input to test against the response_timeline (which acts as the expected output) when validating community parsing rules.