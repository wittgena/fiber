# fiber.dphi.edge.workflow
import uuid
import httpx
from typing import Any

from fiber.dphi.rpc.client import InternalRpcClient
from xphi.kernel.dphi.fsm.edge import (
    EdgePhaseFSM, StartIntentEvent, PhaseFailedEvent,
    ComputePhaseCompletedEvent, CompliancePhaseCompletedEvent, SettlementPhaseCompletedEvent,
    RunComputePhaseCmd, RunCompliancePhaseCmd, RunSettlementPhaseCmd,
    FinishWorkflowCmd, HaltWorkflowCmd
)
from xphi.kernel.space.topos.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("dphi.workflow.edge")

class CommandMsg(WorkflowMessage):
    def __init__(self, command: Any):
        self.command = command


class EdgeWorkflow(Workflow):
    def __init__(self, fsm: EdgePhaseFSM, client: httpx.AsyncClient, base_url: str):
        super().__init__(name="EDGE_MACRO_WORKFLOW")
        self.fsm = fsm
        self.client = client
        self.base_url = base_url
        self.rpc = InternalRpcClient()

    async def execute(self, start_event: StartIntentEvent):
        log.info(f"\n=== [START] {self.name} ===")
        cmd = self.fsm.apply(start_event)
        self.post_message(CommandMsg(cmd))
        await self.run()

    @step
    async def process_command(self, msg: CommandMsg) -> WorkflowMessage:
        cmd = msg.command
        log.info(f"[Workflow] Executing Phase: {cmd.__class__.__name__}")

        try:
            if isinstance(cmd, FinishWorkflowCmd):
                log.info(f"[SUCCESS] Workflow Finished. Final TX Hash: {cmd.tx_hash}")
                return StopMessage(result=True)
            elif isinstance(cmd, HaltWorkflowCmd):
                return ErrorMessage(cmd.reason)
            elif isinstance(cmd, RunComputePhaseCmd):
                event = await self._run_compute_phase(cmd)
                return CommandMsg(self.fsm.apply(event))
            elif isinstance(cmd, RunCompliancePhaseCmd):
                event = await self._run_compliance_phase(cmd)
                return CommandMsg(self.fsm.apply(event))
            elif isinstance(cmd, RunSettlementPhaseCmd):
                event = await self._run_settlement_phase(cmd)
                return CommandMsg(self.fsm.apply(event))
            else:
                raise ValueError(f"Unknown Command: {cmd}")

        except Exception as e:
            log.error(f"[Workflow] Phase Execution Failed: {str(e)}")
            fallback_cmd = self.fsm.apply(PhaseFailedEvent(reason=str(e)))
            return CommandMsg(fallback_cmd)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        log.error(f"\n[HALTED] {self.name} aborted: {msg.msg}")
        return StopMessage(result=False)


    ## 절차적 API 묶음 실행
    async def _run_compute_phase(self, cmd: RunComputePhaseCmd) -> ComputePhaseCompletedEvent:
        """1단계: 견적 산출, 청구서 발행, 잔고 확인, 샌드박스 실행을 하나로 묶어 처리"""
        intent_payload = {
            "client_id": cmd.client_id,
            "responder_id": "target-node-01",
            "action": cmd.action,
            "max_fuel": cmd.max_fuel,
            "source_code": cmd.source_code,
            "signature": cmd.signature,
            "sig_algo": "ECDSA_SECP256K1"
        }

        ## 1. Quote
        res = await self.client.post(f"{self.base_url}/v1/public/sandbox/quote", json=intent_payload)
        res.raise_for_status()
        cost_usd = res.json().get("estimated_cost_usd", 0.0)

        ## 2. Invoice
        res = await self.client.post(f"{self.base_url}/v1/public/billing/invoice", json={
            "payee_address": "0x000000000000000000000000000000000000dEaD", 
            "amount_usdc": str(cost_usd), 
            "resource_id": f"res_{uuid.uuid4().hex[:8]}"
        })
        res.raise_for_status()

        ## 3. Balance Check
        res = await self.client.get(f"{self.base_url}/v1/public/billing/balance", params={"client_id": cmd.client_id, "asset_type": "fuel"})
        if res.status_code != 200:
            raise RuntimeError("Insufficient UTXO Balance")

        ## 4. Sandbox Execute
        res = await self.client.post(
            f"{self.base_url}/v1/public/sandbox/execute", 
            json=intent_payload, 
            headers={"X-X402-Receipt": "valid_receipt"}
        )
        res.raise_for_status()
        return ComputePhaseCompletedEvent(audit_receipt=res.json(), cost_usd=cost_usd)

    async def _run_compliance_phase(self, cmd: RunCompliancePhaseCmd) -> CompliancePhaseCompletedEvent:
        """2단계: 영수증 검증, OTLP 로깅을 하나로 묶어 처리"""
        res = await self.client.post(f"{self.base_url}/v1/public/audit/verify", json=cmd.audit_receipt)
        res.raise_for_status()

        res = await self.client.post(f"{self.base_url}/v1/public/telemetry/logs", json={
            "resourceLogs": [{"resource": {"attributes": [{"key": "svc", "value": {"stringValue": "dphi"}}]}}]
        }, headers={"X-X402-Receipt": cmd.audit_receipt.get("receipt_id", "none")})
        res.raise_for_status()

        return CompliancePhaseCompletedEvent(otlp_hash=res.headers.get("x-edge-content-hash", "0x_hash"))

    async def _run_settlement_phase(self, cmd: RunSettlementPhaseCmd) -> SettlementPhaseCompletedEvent:
        """3단계: 내부망 P2P 거래, 원장 기록, 외부 정산을 하나로 묶어 처리"""
        # [FIX] RPC 호출 시 내부 파라미터도 client_id로 일치
        res_ex = await self.rpc.call("eco.exchange.order.ingress", {
            "client_id": cmd.client_id, "action": "TRADE", "parameters": {"target_pair": "ETH/USDC", "amount": 100}
        })
        exchange_root = res_ex.get("session", {}).get("topo_id", "0x_ex")
        
        await self.rpc.call("core.ledger.append", {
            "stream_name": "system_audit", 
            "events": [{"action": "SETTLEMENT", "user_id": cmd.client_id, "details": exchange_root}], 
            "verbose": False
        })

        res = await self.client.post(f"{self.base_url}/v1/ext/wallet/pay/x402", json={
            "payee_address": "0x000000000000000000000000000000000000dEaD", "amount_usdc": str(cmd.cost_usd), 
            "resource_id": f"res_{uuid.uuid4().hex[:8]}", "use_ledger": True
        })
        res.raise_for_status()

        return SettlementPhaseCompletedEvent(tx_hash=res.json().get("tx_hash", "0x_cleared"))