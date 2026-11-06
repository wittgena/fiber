# fiber.phase.abc.spec.infra.flare.deploy
@desc: Cloudflare Edge V8 Isolate Architecture & Provisioning Specification

---

## 1. Architectural Mandate (Host-Sovereign State vs. Edge Compute)

Fiber's **Flare Execution Plane (`plane.flare`)** decouples deterministic computation from state ownership across a Zero-Trust Local-to-Edge boundary:

1. **Stateless Edge Co-Processor (Cloudflare V8 Isolates)**:
   - Executes compiled WASM kernels (`phase`, `fsm`) and sandboxed Python (`Pyodide` runtime).
   - Enforces strict runtime isolation: deterministic clock freezing, seeded PRNG idempotency, virtual filesystem/socket restrictions, and hypervisor-level CPU/Wall-Time cutoffs (`Error 1102` / `HTTP 502`).
   - Seals execution boundaries with a cryptographic receipt (`X-XPHI-Canonical-Hash`) computed via the Edge WASM fingerprint kernel.
2. **Host-Sovereign Control & Ledger (Local Host)**:
   - Retains final PTA Epoch Merkle Root rollups, quota/fuel ledger mutations, and real-time control plane policy locks on the local loopback control plane.
   - To prevent cross-tenant state exposure and metering exhaustion, remote Edge execution operates strictly on a **Self-Hosted (BYOC)** basis without shared public endpoints.

---

## 2. Unified Plane Resolution (`XPHI_WASM_PLANE`)

The WASM execution broker resolves its underlying transport dynamically through a single environment variable (`XPHI_WASM_PLANE`), preserving full backward compatibility with local streams when unset.

| `XPHI_WASM_PLANE` Value | Transport Mode | Target Endpoint | Provisioning & Lifecycle Behavior |
| :--- | :--- | :--- | :--- |
| Unset or `"local"` | **Host Native** | Local Stream Store | **Default.** Dispatches over native host streams and local WASM workers. |
| `"flare"` or `"flare:dev"` | **Edge Local Dev** | `http://127.0.0.1:8787` | Auto-provisions local Dual V8 Isolates (`:8787`, `:8788`) on demand if the local gateway port is inactive. |
| `"flare:https://<worker>.<subdomain>.workers.dev"` | **Global Edge (BYOC)** | `https://<worker>.<subdomain>.workers.dev` | Bypasses local spawning and routes JSON-RPC payloads directly to the user-deployed Cloudflare Worker. |

---

## 3. Dual-Worker Topology & Blueprint Synthesis

During provisioning, the Edge Controller synthesizes an ephemeral two-tier microservice workspace comprising an upstream **WASM Gateway Node** and a downstream **Python Engine Node**:

- **Upstream Gateway Node (`xphi-gateway-node`)**:
  - Public HTTP entrypoint (`:8787` locally or `*.workers.dev` remotely).
  - Mounts compiled WASM kernels (`CompiledWasm` rules) for direct ABI execution and boundary fingerprinting.
  - Binds to the downstream Python node via an internal Cloudflare Service Binding (`PYTHON_ENGINE`).
- **Downstream Python Node (`xphi-python-node`)**:
  - Internal V8 Isolate (`:8788` locally, non-public Service Binding target remotely) configured with `compatibility_flags = ["python_workers"]`.
  - Enforces environment scrubbing, virtual time freezing, and deterministic PRNG seeding per request.

```toml
# Synthesized Upstream Gateway Manifest Contract (wrangler.toml)
name = "xphi-gateway-node"
compatibility_date = "2024-01-01"

[[rules]]
type = "CompiledWasm"
globs = ["**/*.wasm"]

[[services]]
binding = "PYTHON_ENGINE"
service = "xphi-python-node"

# [Edge State Anchor Placeholder: Durable Objects / D1 / Queues]
# Core defaults to Host-Sovereign stateless compute.
# [[durable_objects.bindings]]
# name = "XPHI_STATE_ANCHOR"
# class_name = "EdgeEpochCoordinator"
```

---

## 4. Deployment Protocol & Ordering Constraints

Because the upstream Gateway declares a strict Service Binding dependency (`PYTHON_ENGINE`) on the downstream Python node, remote deployment to Cloudflare must execute in bottom-up dependency order.

### 4.1. Prerequisites

1. **Workers Subdomain**: An active account-specific subdomain (`*.<subdomain>.workers.dev`) registered in the Cloudflare Dashboard.
2. **Authentication**: Active Wrangler session (`npx wrangler login`) or `CLOUDFLARE_API_TOKEN` with Worker edit permissions.

### 4.2. Provisioning & Live Smoke Verification (`fiber.phase.ops.flare.deploy`)

Remote deployment and live edge readiness are orchestrated through the dedicated deployment verifier (`fiber.phase.ops.flare.deploy`), which executes the following sequence:

1. **Session & Fast-Path Check**: Verifies Wrangler authentication (`wrangler whoami` or `CLOUDFLARE_API_TOKEN`). If `XPHI_WASM_PLANE` already specifies a remote HTTPS endpoint (`flare:https://...`), automated deployment is bypassed and the verifier binds directly to the target URL.
2. **Downstream Registration**: Deploys `xphi-python-node` first so the target service exists in the Cloudflare account registry.
3. **Upstream Registration & Binding**: Deploys `xphi-gateway-node`, links the `PYTHON_ENGINE` service binding, and captures the provisioned `https://xphi-gateway-node.<subdomain>.workers.dev` endpoint.
4. **3-Stage Live Smoke Verification**: Dispatches live probes across the deployed endpoint to verify:
   - **Stage 1 (`PHASE` ABI)**: Direct WASM invocation (`verify_parity`).
   - **Stage 2 (`PYTHON` Service Binding)**: Gateway-to-Pyodide execution and variable injection.
   - **Stage 3 (Boundary Seal)**: Extraction and verification of the `X-XPHI-Canonical-Hash` receipt header.

---

## 5. Wire Protocol & Boundary Sealing

The Host Transport Bridge translates broker dispatches into HTTP/1.1 JSON-RPC 2.0 `POST` requests targeting the Edge Gateway:

| `vm_target` | RPC `method` | Edge Execution Flow | Boundary Seal (`X-XPHI-Canonical-Hash`) |
| --- | --- | --- | --- |
| **`PHASE`** | `"invoke_wasm"` | Instantiates the Phase WASM kernel, allocates linear memory, invokes the target ABI, and unpacks the 64-bit `(ptr | len)` response. | N/A (Cryptographic proof returned in JSON payload) |
| **`FSM`** | `"execute"` | Instantiates the FSM WASM kernel and executes deterministic state transitions over null-terminated JSON slices. | N/A |
| **`PYTHON`** | `"execute"` | Forwards payload via `PYTHON_ENGINE` Service Binding, harvests execution metrics (`fuel`, `mem_usage`), computes a canonical fingerprint via the Phase WASM kernel, and injects `X-XPHI-Canonical-Hash` into the HTTP response header. | **Enforced** (64-char SHA-256 hex recorded to Host PTA ledger) |

---

## 6. External Extension Boundary (`EdgeStateAnchor`)

Fiber intentionally leaves Edge-native state persistence unimplemented in core modules. External integrations can migrate stateful coordination (HTTP 202 HITL parking, distributed fuel locks, or Edge-side PTA ledgers) to **Cloudflare Durable Objects, D1, or Queues** without modifying core broker or transport logic:

1. **Transport Hook (`EdgeStateAnchor`)**: Subclass the state anchor interface to override control event interception (`on_control_event`) and stream persistence (`stream_produce`, `stream_consume`, `stream_ack`), injecting the instance into the tunnel facade.
2. **Manifest Hook (`edge_state_bindings`)**: Supply custom `[[durable_objects.bindings]]` or `[[d1_databases]]` TOML blocks to the controller to append stateful bindings during gateway manifest synthesis.

---

## 7. Deployment & Verification Commands

```bash
# Live Cloudflare Edge Provisioning & 3-Stage Smoke Verification (Deploys Dual Workers)
$ python -m fiber.phase.ops.flare.deploy

# Zero-Deploy Smoke Verification against an existing Self-Hosted (BYOC) Endpoint
$ XPHI_WASM_PLANE=flare:https://xphi-gateway-node.<subdomain>.workers.dev python -m fiber.dev.tester.flare.deploy

# Full Local Dual-Isolate E2E Suite (23 Scenarios: Bridge + 6-Domain Security & PTA Rollup)
$ fiber e2e infra.plane.flare --mode dev
```