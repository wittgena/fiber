# anchor.bind.spec.security.mcp.transition
@desc: MCP 1.0/2.0 Dual-Stack Threat Modeling & Defense Specification

## @metadata

* **ID:** BRN-SEC-SPEC-MCP-2026-V1.0
* **Title:** MCP 1.0/2.0 Dual-Stack Architecture Threat Resolution Mapping
* **Description:** Formalizes the structural vulnerabilities during the Model Context Protocol (MCP) transition period and defines Brane/Surgent's deterministic defense mechanisms for Python ASGI-based ecosystems.
* **Format:** Machine-Parsable Markdown (Key-Value structured)

---

## @baseline

During the transition from MCP 1.0 (Stateful) to MCP 2.0 (Stateless), legacy frameworks rely on highly vulnerable "dual-stack" architectures that attempt to parse and route both protocols simultaneously. Brane eliminates this ambiguity by enforcing an absolute integration boundary:

* **No Fallback Ambiguity:** Brane strips the external protocol shell entirely at the ingress boundary.
* **Deterministic Translation:** All inbound traffic, regardless of its original MCP version, is instantly transduced into Brane's singular internal `LogicStreams` topology.
* **Physical ASGI Isolation:** Security validations are executed at the lowest socket/ASGI level before any application-layer (Pydantic) parsing occurs.

---

## @security.threat

### [BRN-SEC-MCP-001] Protocol Downgrade Attack & Auth Bypass

* **Industry Vectors:** JSON-RPC `initialize` Handshake Spoofing, Fallback Exploits
* **Threat Profile:** Attackers manipulate the MCP client to force the `initialize` payload's `protocolVersion` to a lower version (MCP 1.0) while intentionally omitting the mandatory stateless `Authorization` headers required by MCP 2.0. Legacy frameworks, attempting to maintain backward compatibility, automatically route this traffic to the stateful legacy pipeline, bypassing strict security middlewares.

**Brane Resolution**

* **Module:** `SecurityFirewallMiddleware` & `anchor.surface.spec.gatekeeper`
* **Mechanism:** ASGI-Level Pre-Validation and Strict Gatekeeping.
* **Action:** Before the request reaches the application router, the outermost ASGI middleware mandates the presence of `Authorization` headers universally, regardless of the claimed protocol version. Any explicit downgrade attempt utilizing an unauthorized handshake is immediately terminated (Socket Drop) with an HTTP 426 (Upgrade Required) or 401, sealing off the legacy session pools.

### [BRN-SEC-MCP-002] Request Smuggling via Transport Layer

* **Industry Vectors:** HTTP/1.1 `Content-Length (CL)` vs `Transfer-Encoding (TE)` Desynchronization, SSE Pipeline Injection
* **Threat Profile:** Attackers open a persistent SSE (Server-Sent Events) session under the MCP 1.0 specification. Within this continuous connection, they conceal (smuggle) chunked, stateless MCP 2.0 payloads. Due to packet boundary misinterpretations between proxy servers (e.g., Nginx) and ASGI backends (Starlette), the backend router executes the smuggled 2.0 request under the highly privileged context of the open 1.0 session, leading to unauthorized Remote Code Execution (RCE).

**Brane Resolution**

* **Module:** `SecureMCPServerWrapper` & `bound.adapter.bridge.ledger`
* **Mechanism:** Scope Encapsulation & Topological Transduction.
* **Action:** The `sse_app` and `streamable_http_app` are fully encapsulated. The ASGI `receive` channel actively calculates the cumulative size of byte chunks in real-time, instantly destroying the socket if malformed `TE` headers or exceeded `CL` boundaries are detected. Furthermore, `bridge.ledger` translates all payloads into unified `LogicStreams`; smuggled protocol remnants that do not match the deterministic structure are structurally annihilated (Stripped) before execution.

### [BRN-SEC-MCP-003] Parser Desynchronization & Polymorphic JSON OOM DoS

* **Industry Vectors:** Pydantic `Union` Validation Exhaustion, "Frankenstein" JSON Payloads
* **Threat Profile:** To support both specifications, dual-stack servers utilize polymorphic validation (`Union[MCP1Request, MCP2Request]`). Attackers exploit this by transmitting deeply nested JSON payloads that blend mandatory 1.0 fields with complex 2.0 structures. This forces the Pydantic parser into infinite backtracking and recursive validation loops, spiking CPU to 100%, blocking the async event loop, and causing an Out-Of-Memory (OOM) Denial of Service.

**Brane Resolution**

* **Module:** `anchor.surface.shield` & `xphi.scope`
* **Mechanism:** L7 Volumetric Pre-Blocking & Strict Schema Routing.
* **Action:** `anchor.surface` enforces a hard 5MB `Content-Length` limit at the ASGI middleware level, neutralizing initial memory flood attempts before JSON deserialization begins. Internally, `xphi.scope` entirely bypasses expensive `Union` polymorphic checks. Routing is determined strictly via endpoint or header indicators to a single schema. Ambiguous or malformed "Frankenstein" payloads bypass validation entirely and trigger a deterministic `mock.exception`, immediately dropping the request.

---

## @manifest

```yaml
## Brane MCP Dual-Stack Security Configuration (Machine-Readable Profile)
brane_mcp_security_spec:
  version: "1.0.0"
  architecture: "Absolute_Closed_System"
  target_environment: "MCP_Transition_Dual_Stack"
  
  modules:
    anchor.surface.spec.gatekeeper:
      active: true
      anti_downgrade_policy: "Strict_Enforcement"
      require_stateless_auth_universally: true
      unauthorized_handshake_action: "HTTP_426_Socket_Drop"
        
    bound.adapter.transport.wrapper:
      active: true
      smuggling_prevention: "Streaming_Body_Counter"
      cl_te_desync_action: "Immediate_Socket_Destruction"
      payload_transduction: "LogicStreams_Strict_Strip"

    anchor.surface.shield:
      active: true
      l7_volumetric_control:
        max_payload_bytes: 5242880 # 5MB absolute limit
        max_json_nesting_depth: 8
      
    xphi.scope:
      active: true
      polymorphic_validation: false # Disabled to prevent backtracking DoS
      schema_routing: "Strict_Single_Path"
      ambiguous_payload_action: "Deterministic_Mock_Exception_Drop"

```

---

> **Architectural Invariant:** During protocol transitions, backward compatibility inherently breeds structural decay. Brane guarantees survival not by accommodating legacy protocols, but by violently stripping their transport logic at the ingress boundary and enforcing absolute deterministic execution.