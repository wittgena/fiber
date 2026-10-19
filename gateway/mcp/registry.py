# fiber.gateway.mcp.registry
import os
import re
import orjson
from typing import Any, Dict, List, Optional

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("registry.quarantine")

_SSE_CONTROL_PREFIXES = ("event:", ":", "id:", "retry:")

class IsolationAdapter:
    def __init__(self, target_id: str = ""):
        self.target_id = target_id
        self._env_key_candidates = self._build_env_candidates(target_id)

    @staticmethod
    def _build_env_candidates(target_id: str) -> List[str]:
        if not target_id:
            return []
        norm = re.sub(r"[^A-Z0-9]+", "_", target_id.upper()).strip("_")
        tokens = [
            t for t in norm.split("_")
            if t not in ("NET", "REMOTE", "LOCAL", "MCP", "SERVER", "WORKER") and not t.isdigit()
        ]
        candidates = [f"MCP_{norm}_API_KEY"]
        if tokens:
            candidates.insert(0, f"{tokens[0]}_API_KEY")
        return candidates

    def translate_ingress(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        for env_key in self._env_key_candidates:
            token = os.environ.get(env_key, "").strip()
            if token:
                safe_payload = dict(payload)
                headers = dict(safe_payload.get("_http_headers") or {})
                headers.setdefault("Authorization", f"Bearer {token}")
                safe_payload["_http_headers"] = headers
                return safe_payload
        return payload

    def translate_egress(self, raw_output: str) -> Dict[str, Any]:
        chunk = raw_output.strip()
        if not chunk:
            raise RuntimeError("IPC Contract Violation: Agent closed stream unexpectedly.")

        if chunk.startswith(_SSE_CONTROL_PREFIXES) or chunk.startswith("data:"):
            data_lines = [
                line.strip()[5:].strip() if line.strip().startswith("data:") else line.strip()
                for line in chunk.splitlines()
                if line.strip() and not line.strip().startswith(_SSE_CONTROL_PREFIXES)
            ]
            if not data_lines:
                return {}
            chunk = "\n".join(data_lines)

        try:
            parsed = orjson.loads(chunk)
            if not isinstance(parsed, dict):
                raise ValueError("Root JSON element must be an object.")
            return parsed
        except Exception:
            log.error(f"[Quarantine] STDOUT Pollution Detected: {chunk}")
            raise RuntimeError(f"IPC Contract Violation: Agent stdout is not valid JSON. (Output: {chunk[:50]}...)")

class AdapterRegistry:
    @staticmethod
    def get_adapter(target_id: str = "") -> IsolationAdapter:
        return IsolationAdapter(target_id=target_id)