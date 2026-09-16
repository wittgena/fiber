# fiber.phase.abc.dev.llm.cost.tracker

## 1. Overview

The `fiber.llm.model.usage` and `fiber.llm.model.cost.unit` modules provide a highly scalable, asynchronous system for tracking LLM token usage and calculating financial costs.

**⚠️ Architectural Design Principles**
Fiber firmly rejects legacy architectural anti-patterns (such as global decorators or synchronous return-value hijacking) often found in conventional LLM wrappers.

1. **Decoupled Telemetry (Asynchronous Interceptors):** To guarantee zero latency overhead on the primary business logic, usage telemetry is collected entirely in the background via event-driven **Interceptors** attached to the `get_emitter` logging system.
2. **Deterministic Control vs. Heuristic Billing:** Hardcoding dynamic, provider-specific pricing logic (e.g., prompt caching, reasoning tokens) directly into the runtime execution path introduces unacceptable fragility. Therefore, Fiber strictly decouples **deterministic budget enforcement (Fuel)** from **post-execution cost estimation (Usage)**.

---

## 2. Usage Tracking System

Instead of mutating or introspecting the returned `ModelResponse` objects, token usage is transparently accumulated using a context manager (`track_usage`) combined with decoupled event interceptors.

### 🔌 2.1. Transparent Collection via Context

Wrapping LLM calls within a `track_usage()` block automatically accumulates token telemetry in the background without polluting the application logic.

```python
from fiber.llm.entry import completion
from fiber.llm.model.usage import track_usage

# 1. Open the track_usage context block
with track_usage() as tracker:
    # 2. Execute multiple LLM pipeline calls internally
    res1 = completion(model="gpt-4o", messages=[{"role": "user", "content": "Task 1"}])
    res2 = completion(model="gpt-4o-mini", messages=[{"role": "user", "content": "Task 2"}])
    
    # 3. Retrieve accumulated total usage (grouped by model)
    total_usage = tracker.get_total_tokens()
    print(total_usage)
    # Example Output: 
    # {
    #   "gpt-4o": {"prompt_tokens": 15, "completion_tokens": 20, "total_tokens": 35},
    #   "gpt-4o-mini": {"prompt_tokens": 10, "completion_tokens": 15, "total_tokens": 25}
    # }

```

### ⚙️ 2.2. Interceptor Mechanics

This system is tightly coupled with DPHI's core observability module (`xphi.watcher.plane.emitter`).

* When the `ChannelObserver` within the `llm.entry` pipeline fires a log event, it injects the raw `usage` dictionary and `model_name` into the event payload.
* The `_usage_tracking_interceptor` captures these events asynchronously and aggregates the data into the `UsageTracker` bound to the current `flow_scope`.
* **Constraint by Design:** Usage tracking will silently fail (without crashing the app) if execution occurs outside a `flow_scope` or if the interceptor is intentionally omitted from the pipeline.

---

## 3. Cost Calculation & Billing

The `TenantEco` class and `UnitCostCalculator` compute estimated token costs based on the pricing matrix defined in `model_cost_registry.json`.

### 🧾 3.1. Tenant Billing

This module is utilized to calculate and charge costs to specific users (Tenants) operating within the agentic system.

```python
from fiber.llm.model.usage import get_tenant_eco

eco_service = await get_tenant_eco()

# Pass the usage object directly from the LLM response
billing_result = await eco_service.calculate_tenant_billing(
    tenant_id="user_abc_123",
    usage=res.usage,
    model_name="gpt-4o",
    provider="openai"
)

if billing_result["status"] == "success":
    print(f"Final Billed Cost: ${billing_result['billing_intent']['financials']['final_cost']}")

```

### ⚠️ 3.2. Cost Calculator Constraints (Heuristic Limitations)

Relying purely on runtime cost calculation (`UnitCostCalculator.generic_cost_per_token`) carries inherent industry-wide limitations due to rapidly evolving provider billing schemas:

1. **Prompt Caching Dynamics:** While the system attempts to calculate discounted rates for cached tokens (e.g., Anthropic's `claude-3-5`, OpenAI), inconsistent `prompt_tokens_details` response formats across providers may result in imprecise discount applications.
2. **Tier-based Thresholds:** Logic for step-tier pricing (e.g., Vertex AI's `>128k tokens` rule) relies on string parsing (`_get_applicable_threshold_string`) and may require manual updates when providers alter their pricing schema structures.
3. **Reasoning / Audio / Image Tokens:** Parsing for multimodal or reasoning tokens (e.g., `o1`, `deepseek-r1`) via `completion_tokens_details` is supported, but radically new or unsupported provider formats may default to standard text token rates or report as zero.

**🛠️ Architectural Best Practices**
When integrating the Cost module into core business logic, adopt a **conservative approach**:

* For mission-critical financial settlements, aggregate the raw `total_usage` metrics and process the final fiat billing via an isolated, authoritative external billing server.
* Internal cost calculators should primarily serve as Observability Metrics or as input thresholds for DPHI Kernel governance.

### ⛽ 3.3. Fuel vs. Token Usage (Separation of Concerns)

The DPHI architecture enforces strict isolation between Token Usage (Observability) and Kernel Fuel (Security).

* **Usage (Token Count):** The actual prompt/completion token count returned by the API provider. This is utilized strictly for **post-billing** and analytics via `TenantEco` and `UsageTracker`.
* **Fuel (Kernel Budget):** The **pre-allocated token budget** explicitly authorized by the DPHI Kernel (X402 Economy) prior to the request. This is monitored in real-time within the pipeline (e.g., `StreamAggregator`) to execute **Kill-switches (Physical stream termination)** upon budget exhaustion.

The `ChannelObserver` cleanly segregates these two telemetry streams during emission:

* *Raw Usage:* `usage={"prompt_tokens": 10, "completion_tokens": 20}` ➔ Aggregated by `UsageTracker` for cost estimation.
* *Kernel State:* `kernel_fuel={"consumed": 30, "budget": 1000}` ➔ Monitored by the physical infrastructure for deterministic budget control.

This segregation guarantees that financial systems compute accurate costs based purely on provider metrics, while system security independently enforces hard, network-level budget constraints without data pollution.