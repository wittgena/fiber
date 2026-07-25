# topos.gateway.server.pypi
## @lineage: void.topos.gateway.server.pypi
## @lineage: topos.edge.gateway.server.pypi
## @lineage: edge.gateway.server.pypi
## @lineage: fiber.gateway.server.pypi
"""
@desc: 
- Consensus-backed PyPI Cache & Relay Server (The Blood-Brain Barrier).
- Acts as a secure membrane preventing compromised upstream packages from entering the internal manifold.
- [EVOLUTION] AuditWarden is now strictly reserved for logging security anomalies (blocks/rejections), 
  preventing "autoimmune" logging of standard operations.
"""
import sys
import json
import asyncio
import hashlib
import re
from aiohttp import web, ClientSession

from topos.gateway.server.mcp import SecureMCPServer
from topos.gateway.ingress.sentinel import get_projector, SecurityContext, MetaRuleDef
from topos.gateway.setting import pypi_settings, ServerRunConfig, tool_get_time

from watcher.kernel.audit.warden import AuditWarden
from watcher.plane.emitter import get_emitter

log = get_emitter("pypi.server", phase="INGRESS")
mcp = SecureMCPServer(name="mcp-brane-membrane", version="1.5")

# Initialize the unified membrane projector
projector = get_projector()

# Volatile state caches. Rules stored here are strictly copies of what the Kernel has authorized.
_artifact_cache = {}
_quarantine_db = {} 

mcp.tool()(tool_get_time)

@mcp.tool()
async def inject_mock_vulnerability(package_name: str, action: str, cve_id: str) -> str:
    """
    @desc: Injects a quarantine rule for a specific package.
    @flow: MUST obtain authorization from the deterministic Kernel before mutating the local volatile state.
    """
    valid_actions = ["block", "tamper_hash"]
    if action not in valid_actions:
        return f"[ERROR] Invalid action. Must be one of {valid_actions}."
    
    is_authorized = await projector.gateway.authorize(
        action_id=f"quarantine_{package_name}",
        action="INJECT_QUARANTINE_RULE",
        payload={"target": package_name, "action": action, "cve": cve_id}
    )
    
    if not is_authorized:
        # [EVOLUTION] Anomaly: A rogue agent or unverified source tried to inject a rule and was blocked.
        AuditWarden.record_anomaly("mcp.quarantine_rejected", f"Kernel rejected quarantine rule for {package_name}")
        return f"[ERROR] Quarantine rule for {package_name} rejected by Kernel Spatial Fence."
    
    _quarantine_db[package_name.lower()] = {"action": action, "cve": cve_id}
    # [EVOLUTION] Success: Do not trigger Warden for normal operational success. Kernel already sealed this.
    log.info(f"[Membrane] Authorized and applied rule for {package_name} ({cve_id})")
    return f"[SUCCESS] Rule applied and sealed for {package_name}."

@mcp.tool()
async def inject_meta_rule(rule_id: str, rule_json: str) -> str:
    """
    @desc: Injects a MetaProjector security rule. 
    @flow: Validates schema and seeks Kernel approval before enforcing the rule dynamically.
    """
    try:
        rule_def = MetaRuleDef.model_validate_json(rule_json)
        
        is_authorized = await projector.gateway.authorize(
            action_id=f"metarule_{rule_id}",
            action="INJECT_META_RULE",
            payload={"rule_id": rule_id, "definition": rule_json}
        )
        if not is_authorized:
            # [EVOLUTION] Anomaly: Kernel rejected a structural rule injection.
            AuditWarden.record_anomaly("mcp.meta_rule_rejected", f"Kernel rejected meta-rule {rule_id}")
            return f"[ERROR] Meta-rule {rule_id} rejected by the WASM Kernel."

        projector.load_rule(rule_id, rule_def)
        # [EVOLUTION] Normal Operation: Removed Warden.
        log.info(f"[Membrane] Rule {rule_id} loaded into Projector.")
        return f"[SUCCESS] Projector Meta-rule {rule_id} injected and authorized."
    except Exception as e:
        return f"[ERROR] Schema validation failed: {str(e)}"

@mcp.tool()
async def clear_cache() -> str:
    """Clears the internal PyPI artifact cache."""
    _artifact_cache.clear()
    # [EVOLUTION] Normal Operation: Cache clearing is not a security anomaly. Removed Warden.
    log.info("[Membrane] Internal PyPI artifact cache cleared via MCP.")
    return "[SUCCESS] Internal PyPI cache cleared."

async def membrane_handler(request: web.Request) -> web.Response:
    """
    @desc: Core HTTP relay handler for PyPI traffic.
    @flow: Request -> Quarantine Check -> Pre-fetch Auth -> Upstream Fetch -> Post-fetch Auth (Hash Check) -> Cache -> Response
    """
    client_ip = request.remote
    path = request.path
    headers = dict(request.headers)
    
    # Extract package nominal name (e.g., from /simple/requests/ or /packages/.../requests-2.31.0.tar.gz)
    match = re.match(r"^/(simple|packages)/.*?([^/]+)/?$", path)
    package_name = match.group(2).lower() if match else ""

    try:
        # 1. Kernel-Authorized Quarantine Block Check
        if package_name and package_name in _quarantine_db:
            policy = _quarantine_db[package_name]
            if policy.get("action") == "block":
                log.warning(f"[Proxy] Blocked by kernel-sealed vulnerability rule: {package_name}")
                # [EVOLUTION] Anomaly: A known vulnerable package attempted to enter the system. Trigger Warden.
                AuditWarden.record_anomaly("proxy.block.cve", f"Blocked {package_name} due to {policy.get('cve')}")
                return web.Response(status=403, text=f"Brane Security Membrane: Blocked ({policy.get('cve')})")

        # 2. Pre-Fetch Security Evaluation (Projector spatial rules)
        ctx = SecurityContext(
            origin_ip=client_ip, 
            auth_header=headers.get('Authorization'),
            envelope_path=path, 
            envelope_method=request.method,
            nominal_name=package_name, 
            topology_version=None, 
            substance_hash=None
        )
        await projector.evaluate_pre_fetch(ctx)

        # 3. Artifact Cache Hit Check
        if path in _artifact_cache:
            cached = _artifact_cache[path]
            return web.Response(body=cached['body'], content_type=cached['content_type'])

        # 4. Fetch from Upstream PyPI
        session = request.app['client_session']
        target_url = f"{pypi_settings.upstream_url}{path}"
        
        async with session.get(target_url, headers={'User-Agent': 'pip/24.0 (Brane Proxy)'}) as resp:
            if resp.status != 200:
                return web.Response(status=resp.status, text=await resp.text())
            content = await resp.read()
            content_type = resp.headers.get('Content-Type', 'application/octet-stream')

        # 5. Post-Fetch Security Evaluation (Substance Hash Integrity Check)
        ctx.substance_hash = hashlib.sha256(content).hexdigest()
        await projector.evaluate_post_fetch(ctx)

        # 6. Cache and Return Artifact
        _artifact_cache[path] = {"content_type": content_type, "body": content}
        return web.Response(body=content, content_type=content_type)

    except web.HTTPForbidden as hf:
        # Gracefully handle explicit blocking triggered by the Projector
        # The Projector itself handles sending tension alerts to the Gateway/Warden.
        return web.Response(status=403, text=str(hf.reason))
    except Exception as e:
        log.error(f"Internal Proxy Membrane Error: {str(e)}")
        return web.Response(status=500, text=f"Internal Server Error: {str(e)}")

# ... [서버 시작 및 생명주기 로직은 이전과 완벽히 동일하므로 생략 없이 원본 유지] ...
# ---------------------------------------------------------
# Server Lifecycle & Infrastructure
# ---------------------------------------------------------

async def mcp_sse_handler(request: web.Request) -> web.Response:
    return await mcp.handle_sse_connection(request)

async def mcp_message_handler(request: web.Request) -> web.Response:
    return await mcp.handle_post_message(request)

async def startup_context(app: web.Application):
    app['client_session'] = ClientSession()
    log.info("Initialized global ClientSession for upstream relays.")

async def cleanup_context(app: web.Application):
    await app['client_session'].close()
    log.info("Closed global ClientSession.")

async def start_sse_server(config: ServerRunConfig):
    app = web.Application()
    app.on_startup.append(startup_context)
    app.on_cleanup.append(cleanup_context)

    # Route definitions
    app.router.add_get('/mcp/sse', mcp_sse_handler)
    app.router.add_post('/mcp/messages', mcp_message_handler)
    app.router.add_route('*', '/{tail:.*}', membrane_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.get("host", "127.0.0.1"), config["port"])
    await site.start()
    
    log.info(json.dumps({"msg": f"🚀 Async Membrane Proxy (SSE + PyPI) Activated on {config.get('host', '127.0.0.1')}:{config['port']}"}), file=sys.stderr)

# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------
def main():
    run_config: ServerRunConfig = {
        "transport": pypi_settings.transport_mode,
        "port": pypi_settings.port,
        "host": pypi_settings.host
    }

    if run_config["transport"] == "sse":
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(start_sse_server(run_config))
            loop.run_forever()
        except KeyboardInterrupt:
            log.info("Server shutting down gracefully (Membrane detached)...")
        finally:
            loop.close()
    else:
        # Fallback to standard I/O for direct process invocation (Disables PyPI HTTP Proxying)
        log.info(json.dumps({"msg": "Running in STDIO mode (PyPI HTTP relay disabled)"}))
        mcp.run()

if __name__ == "__main__":
    main()