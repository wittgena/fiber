# xphi.proxy.pypi.client
## @lineage: bound.adapter.mcp.client.pypi
"""@desc: MCP Client encapsulating protocol handshakes and domain-specific tool executions"""
import subprocess
import atexit
import json
import sys
from pathlib import Path
from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class PypiMCPClient:
    """
    @desc: 
    - A synchronous, high-level client to interact with the PyPI MCP Proxy.
    - Handles transport, JSON-RPC formatting, and protocol lifecycle.
    """
    def __init__(self, executable: str, module: str):
        self._proc = subprocess.Popen(
            [executable, "-m", module],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1
        )
        self._req_id = 1
        self._initialized = False
        atexit.register(self.terminate)

    def terminate(self):
        if self._proc.poll() is None:
            self._proc.terminate()

    def _notify(self, method: str, params: dict = None):
        req = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        self._proc.stdin.write(json.dumps(req) + "\n")
        self._proc.stdin.flush()

    def _call(self, method: str, params: dict = None) -> dict:
        req = {"jsonrpc": "2.0", "id": self._req_id, "method": method, "params": params or {}}
        self._proc.stdin.write(json.dumps(req) + "\n")
        self._proc.stdin.flush()
        self._req_id += 1

        while True:
            line = self._proc.stdout.readline()
            if not line:
                raise RuntimeError("MCP Server closed connection unexpectedly.")
            
            try:
                resp = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            if resp.get("id") == req["id"]:
                if "error" in resp:
                    raise RuntimeError(f"MCP Error: {resp['error']}")
                return resp.get("result", {})

    def initialize_session(self, client_name: str = "SecureBuilder", version: str = "1.0"):
        """@desc: Performs the strict MCP protocol handshake."""
        log.info("📡 Executing MCP Handshake...")
        self._call("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": client_name, "version": version}
        })
        self._notify("notifications/initialized")
        self._initialized = True

    def check_status(self) -> str:
        """@desc: Invokes the status check tool."""
        resp = self._call("tools/call", {"name": "check_proxy_status"})
        return resp.get("content", [{}])[0].get("text", "Unknown")

    def simulate_fetch(self, package_name: str) -> str:
        """@desc: Tests upstream connectivity via the proxy tool."""
        resp = self._call("tools/call", {
            "name": "simulate_package_fetch", 
            "arguments": {"package_name": package_name}
        })
        return resp.get("content", [{}])[0].get("text", "Unknown")

    def run_diagnostics(self):
        """@desc: Executes full pre-flight checks internally."""
        if not self._initialized:
            self.initialize_session()

        log.info("📡 Running pre-flight diagnostics on MCP Proxy...")
        status_msg = self.check_status()
        log.info(f"   ↳ Internal State: {status_msg}")

        fetch_msg = self.simulate_fetch("uvloop")
        log.info(f"   ↳ Upstream Test: {fetch_msg}")

        if "[ERROR]" in fetch_msg:
            log.critical("🚨 MCP Pre-flight failed: Upstream registry unreachable.")
            sys.exit(1)