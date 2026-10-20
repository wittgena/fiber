# fiber.phase.abc.spec.lifecycle
@lineage: phase.abc.spec.lifecycle
@desc: Fiber Execution Lifecycle Specification

This specification defines the end-to-end execution lifecycle, state transitions, and cryptographic invariants across the Fiber LLM Pipeline and MCP Gateway control plane.

---

## 1. Execution Topology

Every agent transaction traverses five deterministic stages across the `xphi` runtime kernel before reaching upstream providers or local OS subprocesses:

```text
[Ingress Request]
       │
       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: PRE_TRANSLATE (DuplexChannel)                                       │
│  ├─ EdgeRedisCache      : SHA-256 / Vector lookup -> ctx.fire_channel_read() │
│  └─ SemanticRouter      : Async ONNX classification -> mutates msg["model"]  │
├──────────────────────────────────────────────────────────────────────────────┤
│ STAGE 2: TRANSLATE & POST_TRANSLATE (State Traverser & Guardrail)            │
│  ├─ State Traverser     : Dot-notation schema mapping (compat_rules.json)    │
│  └─ ActiveGuardrail     : Credential regex scan -> raises PermissionError    │
├──────────────────────────────────────────────────────────────────────────────┤
│ STAGE 3: NETWORK I/O & STREAM GOVERNANCE (FuelBreaker & VCR)                 │
│  ├─ FuelBreaker         : Real-time PTA ledger deduction & mid-stream abort  │
│  └─ VCRInjector         : 100ms chunk coalescing (record) / 0ms I/O (replay) │
├──────────────────────────────────────────────────────────────────────────────┤
│ STAGE 4: MCP INGRESS & PRE-FLIGHT TRAP (MCPServerConnector & FSM)            │
│  ├─ Idempotency Gate    : Deduplicates retries via x-idempotency-key         │
│  └─ PreflightGuard      : Regex match -> stashes payload -> HTTP 202 (YIELD) │
├──────────────────────────────────────────────────────────────────────────────┤
│ STAGE 5: OOB ATTESTATION & RESUMPTION (SlackHitlBridge & ValidatorService)   │
│  ├─ OOB Verification    : Slack HMAC-SHA256 + Ed25519 State Token + Nonce    │
│  ├─ Attestation Mode    : RFC-6238 6-Digit TOTP (default) | 1-Click (button:)│
│  └─ Egress Unwrapping   : Lazy-spawns ephemeral stdio or resumes multiplex   │
└──────────────────────────────────────────────────────────────────────────────┘

```

---

## 2. Stage Specifications & Invariants

| Stage | Component / Boundary | Runtime Mechanism | Enforced Invariant |
| --- | --- | --- | --- |
| **1. Pre-Translate** | `PipelineSlot.PRE_TRANSLATE` | Evaluates cache state and executes non-blocking intent routing (`asyncio.to_thread`) prior to schema normalization. | Cache hits short-circuit the pipeline immediately; routing mutations synchronize both `msg["model"]` and `system_meta.base_model`. |
| **2. Post-Translate** | `PipelineSlot.POST_TRANSLATE`<br>

<br>`FIBER_COMPAT_RULES_PATH` | Normalizes provider parameters via declarative path fallbacks and scans serialized payloads against C-level secret regexes. | Zero plaintext credential egress. Any secret match aborts the coroutine prior to socket allocation. |
| **3. Stream & VCR** | `FuelBreaker`<br>

<br>`VCRAdapterProxy` | Monitors token consumption against EdDSA-provisioned PTA quotas; coalesces SSE micro-chunks into `100ms` windows. | Budget exhaustion or `REVOKE` pub/sub signals sever active streams mid-flight; VCR fixtures persist only pre-cutoff chunks. |
| **4. Pre-Flight Trap** | `MCPServerConnector`<br>

<br>`PreflightGuard` | Canonicalizes `params` to sorted JSON and evaluates target regex rules before transitioning to `IntentState.EXECUTING`. | **Zero-Spawn Guarantee:** On match, `stdio/ephemeral` subprocesses are not spawned (`active_sandboxes == 0`) while parked at `HTTP 202`. |
| **5. OOB Resume** | `SlackHitlBridge`<br>

<br>`ValidatorService` | Validates operator approval via TOTP or 1-Click Button, verifies `Ed25519` signature over `payload_hash`, and releases stashed payload. | Stashed payloads are unwrapped with `is_resume_flag = False`, ensuring 3rd-party MCP servers receive standard `tools/call` frames. |

---

## 3. LLM Pipeline Lifecycle (Stages 1–3)

### 3.1. Slot-Based Duplex Interception

The LLM entry facade (`fiber.llm.entry.acompletion`) restructures flat interceptor lists into a strict `DuplexChannel` execution order:
`[Cache] ➔ [Router] ➔ [Translator] ➔ [Active Guardrail] ➔ [FuelBreaker] ➔ [Tracer] ➔ [Network I/O]`

1. **Short-Circuiting (`EdgeRedisCache`):** Computes prompt hashes at `PRE_TRANSLATE`. If a cached resolution exists, `ctx.fire_channel_read()` returns the response upstream without invoking downstream translators or network adapters.
2. **State Synchronization (`SemanticRouter`):** When mutating target models dynamically, the router updates `ctx.get_attr("system_meta").base_model` alongside `msg["model"]` so downstream `TelemetryTracer` and `BillingObserver` instances record accurate model attribution.
3. **Stream Lifecycle Hooks (`TelemetryTracer`):** For streaming responses (`__aiter__`), telemetry emission is deferred by appending callbacks to `meta.framework_flags["on_stream_complete"]`, calculating true end-to-end latency only after the final chunk is consumed.

### 3.2. Application-Layer VCR & Mid-Stream Cutoffs

`VCRInjector` mounts at the adapter boundary in three operational modes:

* **Live Passthrough (`live`):** Default zero-overhead mode for `fiber daemon`, allowing per-request override via HTTP payload fields (`"vcr": "record" | "replay"`).
* **Record (`record`):** Executes live upstream calls, coalesces streaming chunks within `record_tick_ms=100.0`, and writes normalized JSON fixtures. If `FuelBreaker` terminates a stream due to quota exhaustion, the partial stream state is frozen as-is.
* **Replay (`replay`):** Reconstructs Pydantic return objects and `ModelResponseStream` iterators from disk (`~1,180 req/s` unary, `~2,200 chunks/s` streaming, `p50 < 1.0ms` routing overhead), supporting real-time pacing (`VCR_SPEED=real`) and jitter injection (`VCR_CHAOS`).

---

## 4. MCP Gateway & Pre-Flight HITL Lifecycle (Stages 4–5)

### 4.1. Transport & Concurrency Resolution (`resolve_connector_spec`)

When mounting workers via `fiber connect` or `.mcp.json`, the connector infers transport and lifecycle semantics automatically:

| Target Format | Inferred Transport | Default Mode | Sandbox Lifecycle & Pre-Flight Behavior |
| --- | --- | --- | --- |
| `http://`, `https://`<br>

<br>*(e.g., Context7 MCP)* | `network` | `multiplex` | Attaches a persistent `shared_transport` listener. Pre-flight traps hold the JSON-RPC frame in `stashed_payloads` prior to HTTP transmission. |
| CLI Command<br>

<br>*(e.g., `npx server-filesystem`)* | `stdio` | `ephemeral` | Spawns an isolated OS subprocess per transaction. Pre-flight traps intercept `EXECUTE` before `StdioTransport.start()`, preventing process creation until approved. |
| Built-in `AsyncWorkerProtocol` | `stdio` | `multiplex` / `linear` | Determined via AST base-class inspection (`discover_builtin_workers`). |

### 4.2. Pre-Flight Pattern Resolution & Stashing

`PreflightGuard` resolves regex patterns in strict precedence:

1. Explicit CLI/API argument (`-p` / `--preflight`)
2. Target-scoped environment variable (`FIBER_PREFLIGHT_<SANITIZED_TARGET_ID>`)
3. Global environment variable (`FIBER_PREFLIGHT_PATTERN` or `FIBER_HITL_ON`)

**Execution Flow on Trap (`check_preflight_trap`):**

1. Serializes `payload["params"]` into canonical sorted JSON and evaluates `guard_regex`.
2. On match, computes `payload_hash = sha256(canonical_str)` and stores `{"raw_payload", "payload_hash", "user_id", "require_totp"}` in `stashed_payloads[handle_id]`.
3. Transitions `IntentLifecycleFSM` to `IntentState.YIELDED`, emits `MCP_PHASE_YIELD` to `eco.telemetry.events`, and returns `HTTP 202 Accepted` with a synthetic `elicitation/createMessage` prompt.

### 4.3. Tiered Attestation Modes (`TOTP` vs. `1-Click Button`)

`PreflightGuard._parse_mode_and_pattern()` determines the required attestation factor from pattern prefixes (`totp:`, `button:`, `simple:`, `nototp:`, `1click:`) or `FIBER_PREFLIGHT_MODE`:

| Attestation Mode | Configuration Syntax | Slack Block-Kit Rendering (`blocks[3]`) | Verification Path (`verify_and_pop`) |
| --- | --- | --- | --- |
| **RFC-6238 TOTP**<br>

<br>*(Default: `require_totp=True`)* | `-p "DROP|DELETE"`<br>

<br>`FIBER_PREFLIGHT_MODE=totp` | Renders `fiber_totp_block` (`plain_text_input`, 6 digits) above Approve/Deny action buttons. | Invokes `validate.attest` RPC -> queries `admin_users` in `deploy_audit.sqlite` -> decrypts AES-GCM secret via `local_cipher.key` -> validates 6-digit TOTP -> signs `payload_hash` with `Ed25519`. |
| **1-Click Button**<br>

<br>*(Option: `require_totp=False`)* | `-p "button:prod_secrets"`<br>

<br>`FIBER_PREFLIGHT_MODE=button` | Renders `fiber_button_mode_hint` context block; omits text input for 1-click operator ergonomics. | Verifies OOB `attested_user_id` binding -> validates admin record in `ValidatorService` (bypassing 6-digit OTP check) -> signs `payload_hash` with `Ed25519`. |

### 4.4. Out-of-Band Security Gates & Protocol-Clean Resumption

Before a `RESUME` intent is published to `mcp.intent.queue.{target_id}`, `OutOfBandApprovalBridge` enforces:

1. **Webhook HMAC & Drift Check:** Validates Slack `v0` HMAC-SHA256 signature and enforces `abs(now - ts) <= 300s`.
2. **State Token Signature:** Verifies `NodeSigner` `Ed25519` signature over the button's `envelope` (`handle_id`, `target_id`, `prompt_id`, `node_fp`, `require_totp`, `exp`).
3. **Replay Prevention:** Rejects signatures already present in `_consumed_tokens`.
4. **Identity Mapping:** Resolves `slack_user_id` against `identity_map` to bind an authorized internal `spiffe_id`.

**Resumption Unwrapping (`process_intent`):**

* When `MCPServerConnector` receives `action == "RESUME"` for a pre-flight trapped handle, `verify_preflight_resume()` pops the original `raw_payload` after verifying the `Ed25519` signature against `validator_pub_key`.
* Because the underlying 3rd-party worker never initiated a native yield, the connector sets `is_resume_flag = False`:
* **`multiplex` mode:** Dispatches the original `tools/call` payload directly over `shared_transport`.
* **`ephemeral` mode:** Detects `active_sandboxes.get(handle_id) is None` with a valid `verified_payload` and invokes `_execute_ephemeral()`, lazy-spawning the subprocess to execute the request and destroying the sandbox immediately upon completion.

---

## 5. Verification Suites

```bash
# 1. 200-VU Concurrency & VCR Replay Pipeline Verification
$ fiber e2e inter.concurrency --vcr replay --concurrency 200

# 2. Native Worker Out-of-Band Security & Slack TOTP Suite
$ python -m fiber.dev.e2e.worker.oob

# 3. 3rd-Party Pre-Flight HITL Suite (Network/Multiplex TOTP + Stdio/Ephemeral 1-Click Button)
$ python -m fiber.dev.e2e.worker.hitl
```
