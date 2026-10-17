# fiber.dev.worker.finops
import os
import json
import time
import jwt
import asyncio
from typing import Dict, Any

from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol
from xphi.node.space.tunnel.factory import TunnelFactory

class FinopsSystemWorker(AsyncWorkerProtocol):
    def __init__(self):
        super().__init__(agent_name="finops.poc.system")
        self.jwt_secret = os.environ.get("FINOPS_POC_SECRET", "dummy_secret_for_poc")

    async def handle_tools_list(self, req_id: Any):
        tools = [
            {"name": "issue_jwt", "description": "Mint a PoC JWT with budget constraints"},
            {"name": "trigger_kill", "description": "Publish a budget kill-switch signal"}
        ]
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        try:
            if tool_name == "issue_jwt":
                tenant_id = arguments.get("tenant_id", "test_tenant")
                budget_usd = arguments.get("budget_usd", 1.0)
                token = await self.handle_issue_jwt(tenant_id, budget_usd)
                await self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps({"jwt": token})}]})

            elif tool_name == "trigger_kill":
                tenant_id = arguments.get("tenant_id", "test_tenant")
                await self.handle_kill_switch(tenant_id)
                await self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps({"status": "KILL_SIGNAL_SENT"})}]})

            else:
                await self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")

        except Exception as e:
            self.log.error(f"FinOps Tool Execution Error: {e}", exc_info=True)
            await self.send_error(req_id, -32000, str(e))

    async def handle_issue_jwt(self, tenant_id: str, budget_usd: float) -> str:
        payload = {
            "iss": "finops_poc_worker",
            "sub": tenant_id,
            "budget_limit_usd": float(budget_usd),
            "exp": int(time.time()) + 3600
        }
        encoded_jwt = jwt.encode(payload, self.jwt_secret, algorithm="HS256")
        self.log.info(f"Minted PoC JWT for '{tenant_id}' (Budget: ${budget_usd})")
        return encoded_jwt

    async def handle_kill_switch(self, tenant_id: str):
        tunnel = await TunnelFactory.get_default()
        kill_payload = {"type": "update_budget", "tenant_id": tenant_id, "fuel_budget": 0.0}
        await tunnel.publish("finops:control_plane", json.dumps(kill_payload))
        self.log.warning(f"🚨 Published KILL-SWITCH for tenant '{tenant_id}' via Pub/Sub")

def main():
    worker = FinopsSystemWorker()
    try:
        asyncio.run(worker.serve_forever_async())
    except KeyboardInterrupt:
        worker.log.info("FinOps PoC Worker shutting down.")

if __name__ == "__main__":
    main()