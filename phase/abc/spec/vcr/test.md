# fiber.phase.abc.spec.vcr.test
@desc: Deterministic Replay, Execution Modes & VCRManager Configuration Specification

Fiber's Application-Layer VCR serializes LLM and tool traffic (requests, stream chunks, and exceptions) into local JSON fixtures. This enables zero-cost deterministic replay, state recovery, and latency emulation without changing business logic.

> **Why Fiber VCR?**
> Standard HTTP mockers (like `vcrpy`) capture raw network bytes, breaking stream timing and bloating memory. Fiber VCR operates at the LLM schema layer—coalescing SSE micro-chunks into configurable time windows (`record_tick_ms=100.0`) while preserving stream semantics.
> Because `StreamBreaker` integrates directly into the recording lifecycle, streams cut off mid-flight by a policy kill-switch or quota limit record only pre-cutoff chunks for exact offline circuit-breaker testing.

---

## 1. Execution Patterns: Pytest Suite vs. Per-Request Invocation

Fiber VCR works identically in offline test suites and live runtimes. `VCRPlaybackConfig` can be scoped globally via a cassette context manager or overridden per request via `metadata`.

### 1.1. Automated Testing (`pytest` Standard — Default: `replay`)
Scoping `VCRInjector.use_cassette` inside an `autouse` fixture sets the test module to offline `replay` mode by default. Setting `VCR_MODE=record` captures live streams into `fiber-compats/suites/pytest`, while CI pipelines run the same test offline without API keys.

```python
import os, pytest
from fiber.infra.vcr.manager import VCRPlaybackConfig
from fiber.infra.vcr.injector import VCRInjector
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.llm.entry import acompletion

@pytest.fixture(scope="module", autouse=True)
def inject_vcr_proxy():
    compats_dir = os.getenv("FIBER_COMPATS_DIR", os.path.abspath("fiber-compats"))
    fixture_dir = os.path.join(compats_dir, "suites", "pytest")

    config = VCRPlaybackConfig(
        mode=os.getenv("VCR_MODE", "replay"),
        speed=os.getenv("VCR_SPEED", "max"),
        chaos_latency_ms=float(os.getenv("VCR_CHAOS", "0.0")),
        record_tick_ms=float(os.getenv("VCR_TICK", "100.0")),
    )
    with VCRInjector.use_cassette(config=config, fixture_dir=fixture_dir):
        yield

@pytest.mark.asyncio
async def test_semantic_routing_pipeline():
    response = await acompletion(
        model="auto",
        messages=[{"role": "user", "content": "Format this to JSON."}],
        metadata={
            "vcr_scenario": "soft_rule_intent",
            "vcr_invoker": "e2e.router.pytest",
            "vcr_filename": "fixture_soft_rule_intent.json"
        }
    )
    assert StateTraverser.resolve(response, "usage.total_tokens", 0) >= 0
```

> **Zero-Refactor SDK Bridge:** For codebases coupled to third-party SDKs (e.g., `import litellm`), calling `vcr_setup_sequence()` from `fiber.dev.bridge.vcr` at boot routes legacy calls through Fiber's VCR and Guardrail harness without code changes.

### 1.2. General Invocation & HTTP Gateway (Per-Request Override — Default: `live`)

In normal runtime or `fiber daemon` (`/v1/chat/completions`), `VCRManager` defaults to `live` passthrough mode. Python callers (`acompletion(..., metadata={...})`) or HTTP clients can trigger recording or replay per request via `VCRPlaybackConfig.resolve_for_ctx()`:

```bash
# 1. Dynamic Record: Executes live LLM call and saves response to a local fixture
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions -H "Content-Type: application/json" -d '{
    "model": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
    "messages": [{"role": "user", "content": "Give me a 3-word motto."}],
    "vcr": "record",
    "vcr_scenario": "gateway_smoke_test"
  }'

# 2. Dynamic Replay: Replays from disk (~3ms) with zero upstream I/O
$ curl -X POST http://127.0.0.1:8356/v1/chat/completions -H "Content-Type: application/json" -d '{
    "model": "llama_server/gemma-3-1b-it-Q4_K_M.gguf",
    "messages": [{"role": "user", "content": "Give me a 3-word motto."}],
    "vcr": "replay",
    "vcr_scenario": "gateway_smoke_test",
    "vcr_speed": "max"
  }'
```

---

## 2. `VCRManager` Configuration & Parameter Matrix

`fiber.infra.vcr.manager` maps options across `VCRPlaybackConfig` fields, CLI flags (`add_vcr_arguments`), environment variables, and request `metadata` keys.

### 2.1. Playback & Recording Options (`VCRPlaybackConfig`)

| Field | CLI Flag | Env Variable | `metadata` Key | Default | Behavior |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`mode`** | `--vcr` | `VCR_MODE` | `vcr` / `vcr_mode` | `"live"` | `"live"` passthrough, `"record"` upstream capture (max 5 traces), or `"replay"` disk playback. |
| **`speed`** | `--vcr-speed` | `VCR_SPEED` | `vcr_speed` | `"max"` | `"max"` yields immediately (`~0ms`); `"real"` emulates recorded `ttfb_ms` and `delta_ms`. |
| **`chaos_latency_ms`** | `--vcr-chaos` | `VCR_CHAOS` | `vcr_chaos` | `0.0` | Adds synthetic latency jitter (ms) during replay. |
| **`record_tick_ms`** | `--vcr-tick` | `VCR_TICK` | `vcr_tick` | `100.0` | Coalescing window (ms) for SSE chunks during record (`0.0` keeps raw chunks). |
| **`target_traces`** | `--vcr-targets` | — | `vcr_target_trace` | `""` | Pins replay to `"scenario:trace_id"` or `"trace_id"` instead of `latest_trace_id`. |
| **`include_raw_payload`** | — | `VCR_INCLUDE_RAW` | `vcr_include_raw` | `False` | Preserves unparsed upstream `raw_payload` for Schema Traverser verification. |

### 2.2. Fixture Identity & Path Resolution (`VCRIdentityRule`)

`VCRManager` resolves the fixture directory and filename in the following order:

* **Directory:** Uses `metadata["vcr_fixture_dir"]` if set; otherwise defaults to `VCRManager.fixture_dir`.
* **Filename (`VCRIdentityRule.get_fixture_filename`):**
  1. **Explicit (`vcr_filename`):** Uses `{vcr_filename}.json` directly (e.g., `fixture_soft_rule_intent.json`).
  2. **Scenario + Invoker (`vcr_scenario` & `vcr_invoker`):** Formats as `fixture_{flattened_invoker}_{vcr_scenario}.json` (dots replaced with `_`, e.g., `fixture_e2e_router_pytest_soft_rule_intent.json`).
  3. **Auto Fallback:** Generates `fixture_{flattened_invoker}_auto_{trace_id[:8]}.json` seeded by the MD5 hash of sorted `messages`.

---

## 3. Replay Verification Workflows (CLI)

```bash
# 1. Record live API streams into coalesced local fixtures (100ms window)
$ VCR_MODE=record pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# 2. Fast-Forward Replay: Run offline at maximum speed (~0ms I/O) for CI
$ pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# 3. Latency & Chaos Replay: Emulate recorded TTFB/delta pacing + 500ms jitter
$ VCR_SPEED=real VCR_CHAOS=500.0 pytest -v -s fiber/llm/pipeline/interceptor/tests/router.py

# 4. HTTP Gateway Roundtrip Verification (Unary, SSE Stream, Record -> Replay)
$ python -m fiber.dev.tester.gateway
```
