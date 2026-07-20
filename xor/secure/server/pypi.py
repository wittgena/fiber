# xor.secure.server.pypi
## @lineage: xphi.xor.secure.server.pypi
import sys
import json
import asyncio
import hashlib
import re
import time
from aiohttp import web, ClientSession

from xor.secure.server.mcp import SecureMCPServer
from bound.proxy.ingress.pypi.rule.projector import projector, SecurityContext, MetaRuleDef
from bound.proxy.setting import pypi_settings, ServerRunConfig, tool_get_time

from watcher.plane.emitter import get_emitter

log = get_emitter("pypi.server")
mcp = SecureMCPServer(name="mcp-brane-membrane", version="1.5")

# 상태 저장소 로컬 내재화
_artifact_cache = {}
_quarantine_db = {} 

def emit_ledger_event(event_type: str, severity: str, details: dict):
    """Gatekeeper 모듈에서 옮겨온 경량 원격측정 로직"""
    event = {
        "timestamp": time.time(),
        "plane": "anchor.membrane",
        "type": event_type,
        "severity": severity,
        "payload": details
    }
    log.info(json.dumps(event), file=sys.stderr)

mcp.tool()(tool_get_time)

@mcp.tool()
async def inject_mock_vulnerability(package_name: str, action: str, cve_id: str) -> str:
    valid_actions = ["block", "tamper_hash"]
    if action not in valid_actions:
        return f"[ERROR] Invalid action. Must be one of {valid_actions}."
    
    _quarantine_db[package_name.lower()] = {"action": action, "cve": cve_id}
    emit_ledger_event("MCP_INJECT", "INFO", {"target": package_name, "action": action, "cve": cve_id})
    return f"[SUCCESS] Mock rule applied for {package_name}."

@mcp.tool()
async def inject_meta_rule(rule_id: str, rule_json: str) -> str:
    try:
        rule_def = MetaRuleDef.model_validate_json(rule_json)
        projector.load_rule(rule_id, rule_def)
        return f"[SUCCESS] Projector Meta-rule {rule_id} injected."
    except Exception as e:
        return f"[ERROR] Schema validation failed: {str(e)}"

@mcp.tool()
async def clear_cache() -> str:
    _artifact_cache.clear()
    emit_ledger_event("CACHE_CLEARED", "INFO", {})
    return "[SUCCESS] Internal PyPI cache cleared."

async def membrane_handler(request: web.Request) -> web.Response:
    client_ip = request.remote
    path = request.path
    headers = dict(request.headers)
    
    match = re.match(r"^/(simple|packages)/.*?([^/]+)/?$", path)
    package_name = match.group(2).lower() if match else ""

    try:
        # 1. Mock 취약점 통제 로직 (구 Gatekeeper 대체)
        if package_name and package_name in _quarantine_db:
            policy = _quarantine_db[package_name]
            if policy.get("action") == "block":
                log.warning(f"[Proxy] Blocked by mock vulnerability rule: {package_name}")
                return web.Response(status=403, text=f"Brane Security: Blocked ({policy.get('cve')})")

        # 2. 실제 보안 엔진(Projector) 평가
        ctx = SecurityContext(
            origin_ip=client_ip, auth_header=headers.get('Authorization'),
            envelope_path=path, envelope_method=request.method,
            nominal_name=package_name, topology_version=None, substance_hash=None
        )
        await projector.evaluate_pre_fetch(ctx)

        # 3. Cache Hit 검사
        if path in _artifact_cache:
            cached = _artifact_cache[path]
            return web.Response(body=cached['body'], content_type=cached['content_type'])

        # 4. Upstream 요청
        session = request.app['client_session']
        target_url = f"{pypi_settings.upstream_url}{path}"
        
        async with session.get(target_url, headers={'User-Agent': 'pip/24.0 (Brane Proxy)'}) as resp:
            if resp.status != 200:
                return web.Response(status=resp.status, text=await resp.text())
            content = await resp.read()
            content_type = resp.headers.get('Content-Type', 'application/octet-stream')

        # 5. 해시 무결성 등 사후 보안 평가
        ctx.substance_hash = hashlib.sha256(content).hexdigest()
        await projector.evaluate_post_fetch(ctx)

        _artifact_cache[path] = {"content_type": content_type, "body": content}
        return web.Response(body=content, content_type=content_type)

    except Exception as e:
        log.error(f"Internal Proxy Error: {str(e)}")
        return web.Response(status=500, text=f"Internal Server Error: {str(e)}")

async def mcp_sse_handler(request: web.Request) -> web.Response:
    return await mcp.handle_sse_connection(request)

async def mcp_message_handler(request: web.Request) -> web.Response:
    return await mcp.handle_post_message(request)

async def startup_context(app: web.Application):
    app['client_session'] = ClientSession()
    log.info("Initialized global ClientSession.")

async def cleanup_context(app: web.Application):
    await app['client_session'].close()
    log.info("Closed global ClientSession.")

async def start_sse_server(config: ServerRunConfig):
    app = web.Application()
    app.on_startup.append(startup_context)
    app.on_cleanup.append(cleanup_context)

    app.router.add_get('/mcp/sse', mcp_sse_handler)
    app.router.add_post('/mcp/messages', mcp_message_handler)
    app.router.add_route('*', '/{tail:.*}', membrane_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.get("host", "127.0.0.1"), config["port"])
    await site.start()
    
    log.info(json.dumps({"msg": f"🚀 Async Membrane Proxy (SSE + PyPI) Activated on {config.get('host', '127.0.0.1')}:{config['port']}"}), file=sys.stderr)

## ------------------------------------------
## Entry Point
## ------------------------------------------
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
            log.info("Server shutting down gracefully...")
        finally:
            loop.close()
    else:
        log.info(json.dumps({"msg": "Running in STDIO mode (PyPI proxy disabled)"}))
        mcp.run()

if __name__ == "__main__":
    main()