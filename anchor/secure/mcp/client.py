# anchor.secure.mcp.client
## @lineage: anchor.cli.secure.mcp.client
import subprocess
import atexit
import json
from pathlib import Path
from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class PypiMCPClient:
    """
    @desc: A synchronous, lightweight JSON-RPC client to communicate with the MCP Proxy 
           over standard input/output (stdio transport) without requiring async frameworks.
    """
    def __init__(self, executable: str, module: str):
        self.proc = subprocess.Popen(
            [executable, "-m", module],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1
        )
        self._req_id = 1
        atexit.register(self.terminate)

    def terminate(self):
        if self.proc.poll() is None:
            self.proc.terminate()

    def notify(self, method: str, params: dict = None):
        """@desc: Sends a JSON-RPC notification (no response expected)."""
        req = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {}
        }
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()

    def call(self, method: str, params: dict = None) -> dict:
        """@desc: Sends a JSON-RPC request and blocks until a matching response is received."""
        req = {
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": method,
            "params": params or {}
        }
        self.proc.stdin.write(json.dumps(req) + "\n")
        self.proc.stdin.flush()
        self._req_id += 1

        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("MCP Server closed connection unexpectedly.")
            
            try:
                resp = json.loads(line.strip())
            except json.JSONDecodeError:
                continue

            # Return only when the response ID matches our request ID
            if resp.get("id") == req["id"]:
                if "error" in resp:
                    raise RuntimeError(f"MCP Error: {resp['error']}")
                return resp.get("result", {})
