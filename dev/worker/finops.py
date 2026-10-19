# fiber.dev.worker.finops
import json
import asyncio
from typing import Dict, Any, Optional

from fiber.gateway.rest.token.gen import FinOpsTokenGenerator, FinOpsOpType
from xphi.arch.contract.protocol.worker import AsyncWorkerProtocol
from xphi.node.space.tunnel.factory import TunnelFactory

DEFAULT_TENANT_ID = "accounting_dept"
SYSTEM_ISSUER = "finops_local_system"

class FinopsSystemWorker(AsyncWorkerProtocol):
    """
    Local FinOps System Control Worker
    Handles standardized FinOps JWT issuance (PROVISION, SYNC_QUOTA, TOP_UP, REVOKE)
    and real-time Pub/Sub budget control-plane signaling via Tunnel Bus.
    """
    def __init__(self):
        super().__init__(agent_name="finops.local.system")
        self.token_gen = FinOpsTokenGenerator(issuer=SYSTEM_ISSUER)

    async def handle_tools_list(self, req_id: Any):
        tools = [
            {
                "name": "issue_jwt",
                "description": "Mint a standardized FinOps JWT with budget constraints, operation mode, and policy version"
            },
            {
                "name": "trigger_kill",
                "description": "Publish a real-time REVOKE kill-switch signal to the FinOps control plane"
            }
        ]
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        try:
            if tool_name == "issue_jwt":
                tenant_id = arguments.get("tenant_id", DEFAULT_TENANT_ID)
                budget_usd = float(arguments.get("budget_usd", 1.0))
                op: FinOpsOpType = arguments.get("op", "SYNC_QUOTA")
                policy_ver = int(arguments.get("policy_ver", 1))
                allocation_id = arguments.get("allocation_id")

                token = await self.handle_issue_jwt(
                    tenant_id=tenant_id,
                    budget_usd=budget_usd,
                    op=op,
                    policy_ver=policy_ver,
                    allocation_id=allocation_id
                )
                await self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps({"jwt": token})}]})

            elif tool_name == "trigger_kill":
                tenant_id = arguments.get("tenant_id", DEFAULT_TENANT_ID)
                policy_ver = arguments.get("policy_ver")
                await self.handle_kill_switch(tenant_id=tenant_id, policy_ver=policy_ver)
                await self.send_response(req_id, {"content": [{"type": "text", "text": json.dumps({"status": "KILL_SIGNAL_SENT"})}]})

            else:
                await self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")

        except Exception as e:
            self.log.error(f"FinOps Tool Execution Error: {e}", exc_info=True)
            await self.send_error(req_id, -32000, str(e))

    async def handle_issue_jwt(
        self,
        tenant_id: str,
        budget_usd: float,
        op: FinOpsOpType = "SYNC_QUOTA",
        policy_ver: int = 1,
        allocation_id: Optional[str] = None
    ) -> str:
        encoded_jwt = self.token_gen.generate_token(
            tenant_id=tenant_id,
            budget_usd=budget_usd,
            op=op,
            policy_ver=policy_ver,
            allocation_id=allocation_id
        )
        self.log.info(
            f"[LocalFinOps] Minted FinOps JWT for '{tenant_id}' "
            f"(Budget: ${budget_usd}, Op: {op}, Ver: v{policy_ver})"
        )
        return encoded_jwt

    async def handle_kill_switch(self, tenant_id: str, policy_ver: Optional[int] = None):
        tunnel = await TunnelFactory.get_default()
        kill_payload: Dict[str, Any] = {
            "type": "update_budget",
            "tenant_id": tenant_id,
            "fuel_budget": 0.0,
            "finops": {
                "op": "REVOKE",
                "budget_usd": 0.0,
                "exchange_rate": 1000
            }
        }
        if policy_ver is not None:
            kill_payload["finops"]["policy_ver"] = int(policy_ver)

        await tunnel.publish("finops:control_plane", json.dumps(kill_payload))
        self.log.warning(f"🚨 [LocalFinOps] Published KILL-SWITCH (REVOKE) for tenant '{tenant_id}' via Pub/Sub")

def main():
    worker = FinopsSystemWorker()
    try:
        asyncio.run(worker.serve_forever_async())
    except KeyboardInterrupt:
        worker.log.info("Local FinOps System Worker shutting down.")


if __name__ == "__main__":
    main()