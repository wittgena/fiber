# fiber.phase.abc.spec.mcp.guard
@desc: MCP Pre-Flight HITL & Out-of-Band (Slack/TOTP) Configuration Guide

## Execution Flow

Fiber supports two complementary interception mechanisms that converge into a unified `HTTP 202 (YIELD)` parking and OOB attestation lifecycle:

1. **Zero-Code Pre-Flight Trap (`PreflightGuard`):** Evaluates incoming JSON-RPC `params` against regular expressions *before* reaching the worker. For `stdio/ephemeral` servers (e.g., `npx` packages), this guarantees **Zero-Spawn isolation**—the OS subprocess is never created until an operator approves the request.
2. **Native Worker Elicitation:** Built-in workers (e.g., `worker.deployer`) can natively suspend execution mid-flight by emitting an `elicitation/createMessage` frame when destructive payloads are detected.

```text
[AI Agent] ──(POST /v1/mcp-gateway/{target}/invoke)──► [MCPServerConnector]
                                                               │
      ┌────────────────────────────────────────────────────────┘
      ▼
[PreflightGuard Regex Match]
  ├─ 1. Stash raw_payload & compute SHA-256 payload_hash
  ├─ 2. Emit MCP_PHASE_YIELD on eco.telemetry.events
  └─ 3. Return HTTP 202 Accepted + Location: /.../txn_handle_id
      │
      ▼ (Out-of-Band Channel: Slack Socket Mode / Webhook)
[SlackHitlBridge & OutOfBandApprovalBridge]
  ├─ Gate 1: Verify Slack v0 HMAC-SHA256 & 300s timestamp window
  ├─ Gate 2: Verify NodeSigner Ed25519 signature on State Token envelope
  ├─ Gate 3: Block duplicate clicks via one-time _consumed_tokens cache
  └─ Gate 4: Map slack_user_id -> internal SPIFFE ID & dispatch RESUME
      │
      ▼
[ValidatorService & PreflightGuard.verify_and_pop]
  ├─ Verify Admin Vault (AES-GCM) + 6-Digit TOTP (or 1-Click Identity)
  ├─ Issue & verify Ed25519 signature over payload_hash
  └─ Release stashed payload (is_resume=False) -> Lazy-Spawn / Multiplex I/O
```

---

## Provision Node Identity & Admin TOTP (`fiber sign`)

Before enabling HITL gates, initialize the local node keychain (`~/.ssh/fiber_ed25519` and `local_cipher.key`) and enroll an administrator identity into the encrypted SQLite vault (`deploy_audit.sqlite`).

```bash
# 1. Initialize Zero-Env node signing identity (Ed25519) & local cipher key
$ fiber sign init
$ fiber sign status

# 2. Enroll an administrator SPIFFE identity (renders a terminal QR code)
# Scan the QR code using Google Authenticator, Authy, or 1Password
$ fiber sign enroll -u spiffe://self/fiber

# 3. Verify AES-GCM vault decryption and test a 6-digit RFC-6238 TOTP code
$ fiber sign verify -u spiffe://self/fiber
```

> **Note:** Even when using **1-Click Button Mode** (`button:<regex>`), the approving operator's mapped SPIFFE ID must be enrolled in `deploy_audit.sqlite` so `ValidatorService` can verify account existence and AES-GCM vault integrity prior to issuing an `Ed25519` signature.

---

## Configure Pre-Flight HITL Locks (`TOTP` vs. `1-Click Button`)

Fiber provides two attestation tiers depending on operational risk. By default, all pre-flight traps enforce **6-Digit RFC-6238 TOTP**. Prefixing any regex pattern with `button:` (or `simple:`, `nototp:`, `1click:`) switches that guard to **1-Click Button Mode**.

| Mode | Pattern Syntax | Slack UI Rendered | Cryptographic Verification |
| --- | --- | --- | --- |
| **6-Digit TOTP**<br>

<br>*(Default)* | `"DROP|DELETE"`<br>

<br>*or* `"totp:DROP|DELETE"` | Renders a 6-digit `plain_text_input` box above **Approve & Resume** / **Deny & Rollback** buttons. | Slack HMAC + State Token (`Ed25519`) + Admin Vault AES-GCM Decrypt + **RFC-6238 6-Digit OTP Match** + Payload Hash `Ed25519` Attestation. |
| **1-Click Button**<br>

<br>*(Ergonomic)* | `"button:prod_secrets|RM"`<br>

<br>*or* `FIBER_PREFLIGHT_MODE=button` | Omits the TOTP input box; displays only **Approve & Resume** / **Deny & Rollback** buttons. | Slack HMAC + State Token (`Ed25519`) + Admin Vault AES-GCM Decrypt + **Authorized SPIFFE Binding** + Payload Hash `Ed25519` Attestation. |

### Option A: CLI Sidecar Mounting (`fiber connect`)

Use `-p` / `--preflight` when mounting individual workers or remote MCP endpoints:

```bash
# 1. Default TOTP Mode on a 3rd-Party Postgres MCP Server
$ fiber connect -t prod-db \
  -e "npx -y @modelcontextprotocol/server-postgres postgresql://localhost/mydb" \
  -p "DROP|TRUNCATE|DELETE|ALTER"

# 2. 1-Click Button Mode on a Local Filesystem MCP Server
$ fiber connect -t local-fs \
  -e "npx -y @modelcontextprotocol/server-filesystem /Users/shared/workspace" \
  -p "button:prod_secrets|\.env|id_rsa"

# 3. Remote HTTP/SSE MCP Server with Pre-Flight Lock
$ fiber connect -t ctx7-remote \
  -e https://mcp.context7.com/mcp \
  -p "RESTRICTED_TOPIC" \
  -f .env
```

### Option B: Declarative Agent Config (`.mcp.json` / `.cursor/mcp.json`)

Inject `FIBER_PREFLIGHT_PATTERN` (and optionally `FIBER_PREFLIGHT_MODE`) directly into the `env` block of any server in your existing agent configuration file. `McpConfigMapper` extracts and strips these keys before spawning the worker so internal flags never leak into the 3rd-party environment:

```json
{
  "mcpServers": {
    "fiber-gateway": {
      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"
    },
    "prod-database": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-postgres"],
      "env": {
        "FIBER_PREFLIGHT_PATTERN": "DROP|TRUNCATE|DELETE"
      }
    },
    "workspace-fs": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
      "env": {
        "FIBER_PREFLIGHT_PATTERN": "prod_secrets|DELETE",
        "FIBER_PREFLIGHT_MODE": "button"
      }
    }
  }
}
```

```bash
# Mount all servers defined in .mcp.json with their respective pre-flight guards
$ fiber connect -c .mcp.json
```

### Option C: Host Environment Variables (Precedence Rules)

`PreflightGuard.resolve_pattern()` resolves rules in the following priority order:

1. **Explicit CLI/SDK Argument:** `-p` / `preflight_pattern="..."`
2. **Target-Scoped Env Var:** `FIBER_PREFLIGHT_<SANITIZED_TARGET_ID>` *(Non-alphanumeric characters in `target_id` are replaced with `_` and uppercased, e.g., `hitl-stdio-fs-01` ➔ `FIBER_PREFLIGHT_HITL_STDIO_FS_01`)*
3. **Global Env Var:** `FIBER_PREFLIGHT_PATTERN` or `FIBER_HITL_ON`

Mode overrides follow the same hierarchy: `prefix (button:)` ➔ `FIBER_PREFLIGHT_MODE_<TARGET>` ➔ `FIBER_PREFLIGHT_MODE` / `FIBER_HITL_MODE`.

---

## Slack App & Socket Mode Setup

`SlackHitlBridge` supports both **Outbound Socket Mode** (recommended for local daemons and firewalled environments—no public webhook URL required) and **Inbound HTTP Webhooks**.

### 4.1. Slack App Configuration Checklist

1. **Create a Slack App** at `api.slack.com/apps` and enable **Socket Mode**:
* Generate an App-Level Token with `connections:write` scope (`xapp-...`).


2. **Enable Interactivity & Shortcuts**:
* With Socket Mode enabled, all button clicks and block actions route directly over the WebSocket.


3. **Bot Token Scopes (OAuth & Permissions)**:
* Add `chat:write` (and `chat:write.public` if posting to channels the bot hasn't explicitly joined) to obtain your Bot User OAuth Token (`xoxb-...`).


4. **Invite the Bot** to your target approval channel and copy the **Channel ID** (e.g., `C0A9L79HY57`).

### Environment Variables (`.env`)

Configure the following variables in the `.env` file passed to `fiber daemon -s all -f .env`:

| Environment Variable | Required | Description |
| --- | --- | --- |
| `FIBER_SLACK_BOT_TOKEN` | **Yes** (Live) | Slack Bot User OAuth Token (`xoxb-...`) used for `chat.postMessage`. |
| `FIBER_SLACK_APP_TOKEN` | **Yes** (Socket) | Slack App-Level Token (`xapp-...`) used for `apps.connections.open` WebSocket. |
| `FIBER_SLACK_CHANNEL_ID` | **Yes** (Live) | Target Slack Channel ID (e.g., `C0A9L79HY57`) where HITL prompts are posted. |
| `FIBER_SLACK_USER_ID` | Optional | Slack User ID (e.g., `U012AB3CD`) mapped to the enrolled internal SPIFFE identity. |
| `FIBER_SLACK_SIGNING_SECRET` | Optional | Slack Signing Secret for HTTP webhooks. *(If omitted in Socket/Local mode, Fiber deterministically derives an HMAC key from the node's `Ed25519` public key).* |

---

## Client Invocation & Resumption Protocol (`HTTP 202` ➔ `200`)

When an AI agent or HTTP client calls `/v1/mcp-gateway/{target_id}/invoke` with a payload that triggers a pre-flight or native HITL trap:

### Initial Request & `HTTP 202` Parking

* **Client Request:** Sends a standard MCP `tools/call` JSON-RPC payload with an `x-idempotency-key` header.
* **Gateway Response (`HTTP 202 Accepted`):**
* **Header:** `Location: /v1/mcp-gateway/{target_id}/handles/{handle_id}`
* **Body:**
```json
{
  "jsonrpc": "2.0",
  "id": "prompt_txn_1792523154_ad45a9be4329",
  "method": "elicitation/createMessage",
  "params": {
    "message": "[Pre-flight Guard] High-risk pattern 'RESTRICTED_TOPIC' detected on 'net-context7-remote-01'. Enter TOTP:",
    "payload_hash": "1099bd1e...",
    "require_totp": true
  }
}
```

### Operator Approval

1. `SlackHitlBridge` listens on `eco.telemetry.events` for `MCP_PHASE_YIELD` and posts an interactive Block-Kit card containing the **Target MCP**, **Node Fingerprint** (`node_fp`), **Handle ID**, and **Intercepted Prompt**.
2. When the operator clicks **🛡️ Approve & Resume** (with or without TOTP, depending on `require_totp`) or **🛑 Deny & Rollback**:
* **Multi-Node Filtering:** `is_owned_by_this_node()` verifies that `envelope.node_fp` matches the local node's public key fingerprint, ignoring events meant for other nodes sharing the same Slack channel.
* **Real-Time Card Replacement:** Upon verification, `SlackHitlBridge` posts to Slack's `response_url` (`replace_original: True`), replacing the interactive buttons with an immutable **Zero-Trust Attestation Receipt** (`🟢 RESUMED -> EXECUTING` or `🔴 ROLLED_BACK (FAULTED)`).

### Polling & Resolution

Once the operator approves the prompt in Slack, the connector executes the stashed request against the MCP worker and transitions the handle state to `RESOLVED`. When the client retries or polls `/v1/mcp-gateway/{target_id}/invoke` using the **same `x-idempotency-key**`, the gateway immediately returns the cached `HTTP 200 OK` tool output without re-executing the worker.

---

## Verification & E2E Testing

Fiber includes two comprehensive E2E suites to validate OOB security defenses and 3rd-party Pre-Flight HITL lifecycles. Both suites automatically detect whether `FIBER_SLACK_BOT_TOKEN` and `FIBER_SLACK_APP_TOKEN` are present in the environment—running interactively against **Live Slack Socket Mode** when configured, or falling back to **Local Webhook Emulation** in offline CI/CD environments.

```bash
# Verify 3rd-Party Pre-Flight HITL across both architectures & attestation modes:
#    - Phase 1: Spec Resolver & Dual Guard Arming (TOTP + 1-Click Button)
#    - Phase 2: Safe Call Passthrough (Zero-Overhead 200 OK)
#    - Phase 3: [Network/Multiplex + 6-Digit TOTP] Remote Context7 Trap & Shared Stream Resume
#    - Phase 4: [Stdio/Ephemeral + 1-Click Button] npx Filesystem Zero-Spawn Trap & Lazy Spawn Resume
$ python -m fiber.dev.e2e.worker.guard
```