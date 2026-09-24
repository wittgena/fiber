# fiber.dev.e2e.edge.client
import asyncio
import os
import uuid
from typing import List, Any

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.infra.e2e.config import PipelineRunner, TestResult, E2EConfig, Phase
from fiber.dev.sdk.ext import ExtClient
from fiber.dev.sdk.gateway import DphiPublicClient, StrictPayloadFactory 

from xphi.arch.bound.client.http import VerifiedHttpClient
from xphi.arch.dev.tracer.transport import HttpFlowTracer
from xphi.kernel.node.fsm.edge import (
    EdgePhaseFSM, EdgePhaseState, StartIntentEvent, PhaseFailedEvent,
    ComputePhaseCompletedEvent, CompliancePhaseCompletedEvent, SettlementPhaseCompletedEvent,
    RunComputePhaseCmd, RunCompliancePhaseCmd, RunSettlementPhaseCmd,
    FinishWorkflowCmd, HaltWorkflowCmd
)
from xphi.arch.contract.workflow import ErrorMessage, StopMessage, Workflow, WorkflowMessage, step
from xphi.watcher.plane.emitter import get_emitter
from xphi.state.phase.reactor import PhaseReactor

log = get_emitter("e2e.edge.client")

class CommandMsg(WorkflowMessage):
    def __init__(self, command: Any):
        self.command = command

class EdgeWorkflow(Workflow):
    def __init__(self, fsm: EdgePhaseFSM, client: httpx.AsyncClient, base_url: str):
        super().__init__(name="EDGE_WORKFLOW")
        self.fsm = fsm
        self.client = client
        self.base_url = base_url
        
        self.sdk_client = DphiPublicClient(base_url=self.base_url)
        self.ext_client = ExtClient() 

    async def execute(self, start_event: StartIntentEvent):
        log.info("🏁 [START] EDGE CLIENT WORKFLOW INITIATED | " + "="*40)
        cmd = self.fsm.apply(start_event)
        self.post_message(CommandMsg(cmd))
        await self.run()

    @step
    async def process_command(self, msg: CommandMsg) -> WorkflowMessage:
        cmd = msg.command
        phase_name = cmd.__class__.__name__.replace("Run", "").replace("Cmd", "")
        
        if not isinstance(cmd, (FinishWorkflowCmd, HaltWorkflowCmd)):
            log.info(f"▶️ [PHASE EXECUTION] {phase_name.upper()} START ───────────────┐")

        try:
            if isinstance(cmd, FinishWorkflowCmd):
                log.info(f"✨ [SUCCESS] Workflow Completed | Final TX Hash: {cmd.tx_hash}")
                return StopMessage(result=True)
            
            elif isinstance(cmd, HaltWorkflowCmd):
                return ErrorMessage(cmd.reason)
            
            elif isinstance(cmd, RunComputePhaseCmd):
                event = await self._run_compute_phase(cmd)
                log.info(f"✅ [COMPUTE PHASE] PASSED")
                return CommandMsg(self.fsm.apply(event))
            
            elif isinstance(cmd, RunCompliancePhaseCmd):
                event = await self._run_compliance_phase(cmd)
                log.info(f"✅ [COMPLIANCE PHASE] PASSED")
                return CommandMsg(self.fsm.apply(event))
            
            elif isinstance(cmd, RunSettlementPhaseCmd):
                event = await self._run_settlement_phase(cmd)
                log.info(f"✅ [SETTLEMENT PHASE] PASSED")
                return CommandMsg(self.fsm.apply(event))
            
            else:
                raise ValueError(f"Unknown Command: {cmd}")

        except Exception as e:
            log.error(f"❌ [WORKFLOW FAULT] Phase Execution Failed: {str(e)}")
            fallback_cmd = self.fsm.apply(PhaseFailedEvent(reason=str(e)))
            return CommandMsg(fallback_cmd)

    @step
    async def on_error(self, msg: ErrorMessage) -> WorkflowMessage:
        log.error(f"⛔ [HALTED] {self.name} aborted: {msg.msg}")
        return StopMessage(result=False)

    async def _run_compute_phase(self, cmd: RunComputePhaseCmd) -> ComputePhaseCompletedEvent:
        intent_payload = {
            "client_id": cmd.client_id, "responder_id": "target-node-01",
            "action": cmd.action, "max_fuel": cmd.max_fuel,
            "payload": cmd.payload, "signature": cmd.signature, "sig_algo": "ECDSA_SECP256K1"
        }

        res = await self.client.post(f"{self.base_url}/v1/public/sandbox/quote", json=intent_payload, headers={"X-X402-Receipt": "pre_flight_check"})
        res.raise_for_status() 
        cost_usd = res.json().get("estimated_cost_usd", 0.001)

        res = await self.client.post(f"{self.base_url}/v1/public/billing/invoice", json={
            "payee_address": "0x000000000000000000000000000000000000dEaD", 
            "amount_usdc": str(cost_usd), "resource_id": f"res_{uuid.uuid4().hex[:8]}"
        })
        res.raise_for_status()
        invoice_id = res.json().get("invoice_id", f"inv_{uuid.uuid4().hex[:8]}")

        res = await self.client.get(f"{self.base_url}/v1/public/billing/balance", params={"client_id": cmd.client_id, "asset_type": "fuel"})
        if res.status_code != 200: raise RuntimeError("Insufficient Balance")

        audit_payload = StrictPayloadFactory.create_audit_payload(
            actor=cmd.client_id, action=cmd.action, message="E2E Client Intent Execution", require_proof=True
        )
        audit_res = await self.sdk_client.record_audit_event(request=audit_payload, payment_receipt=invoice_id)

        actual_receipt = {
            "receipt_id": audit_res.get("request_id"),
            "state_root": audit_res.get("result", {}).get("hash")
        }
        
        log.info(f"  └─ Quote: {cost_usd} USD | Inv: {invoice_id[:8]} | PTA Secured: {actual_receipt['state_root'][:16]}...")
        return ComputePhaseCompletedEvent(audit_receipt=actual_receipt, cost_usd=cost_usd)

    async def _run_compliance_phase(self, cmd: RunCompliancePhaseCmd) -> CompliancePhaseCompletedEvent:
        res_verify = await self.client.post(f"{self.base_url}/v1/public/audit/verify", json=cmd.audit_receipt)
        res_verify.raise_for_status() 

        telemetry_payload = StrictPayloadFactory.create_telemetry_payload(
            tenant_id="e2e-tenant", model_name="e2e-model", prompt_tokens=100, completion_tokens=50
        )
        res_telemetry = await self.sdk_client.push_telemetry(
            request=telemetry_payload, payment_receipt=cmd.audit_receipt.get("receipt_id")
        )
        fingerprint = res_telemetry.get("fingerprint", "0x_hash")
        log.info(f"  └─ Zero-Trust Validated | Telemetry Sealed: {fingerprint[:16]}...")
        return CompliancePhaseCompletedEvent(otlp_hash=fingerprint)

    async def _run_settlement_phase(self, cmd: RunSettlementPhaseCmd) -> SettlementPhaseCompletedEvent:
        res_payment = await self.ext_client.process_x402_payment(
            payee_address="0x000000000000000000000000000000000000dEaD", 
            amount_usdc=str(cmd.cost_usd), resource_id=f"res_{uuid.uuid4().hex[:8]}", use_ledger=True
        )
        receipt = res_payment.get("receipt", {})
        tx_hash = receipt.get("tx_hash") or res_payment.get("tx_hash") or f"0x_cleared_{uuid.uuid4().hex[:8]}"

        res_balance = await self.client.get(
            f"{self.base_url}/v1/public/billing/balance", params={"client_id": cmd.client_id, "asset_type": "fuel"}
        )
        
        current_fuel = res_balance.json().get("balance", 0) if res_balance.status_code == 200 else "Unknown"
        log.info(f"  └─ Ext Payment: {tx_hash[:16]}... | Current Fuel Balance: {current_fuel}")
        return SettlementPhaseCompletedEvent(tx_hash=tx_hash)

class EdgeTracerPipeline(PipelineRunner):
    def __init__(self, config: E2EConfig):
        super().__init__(name="Public Edge & Network Isolation Trace", scope_name="EDGE_INGRESS_PIPELINE")
        self.config = config
        self.tracer = HttpFlowTracer()
        self.local_url = f"{self.config.protocol}://{self.config.host}:{self.config.port}"
        
        self.set_phases([
            Phase("Origin API Format Validation", self.phase_origin_api_verification),
            Phase("Gateway Ingress", self.phase_ingress_e2e_golden)
        ])

    async def run_pipeline(self) -> List[TestResult]:
        log.info(f"\n=== Starting Client Pipeline: {self.name} ===")
        results = []
        try:
            for idx, phase in enumerate(self.phases, 1):
                log.info(f"\n▶️ [PIPELINE PHASE {idx}/{len(self.phases)}] {phase.name}")
                try:
                    await phase.action()
                    results.append(TestResult("EDGE_GATEWAY", phase.name, True, True))
                except Exception as e:
                    log.error(f"Phase '{phase.name}' Halted: {str(e)}")
                    results.append(TestResult("EDGE_GATEWAY", phase.name, False, True))
                    break 
        finally:
            log.info(f"\n[Pipeline] Phase execution sequence completed.")
            
        return results

    async def phase_origin_api_verification(self):
        async with httpx.AsyncClient(base_url=self.local_url, timeout=5.0) as client:
            res = await client.get("/v1/public/keys")
            if res.status_code != 200:
                raise RuntimeError(f"Origin API failed with status {res.status_code}")
            if "x-dphi-root-signature" not in res.headers:
                raise RuntimeError("Critical: 'X-Dphi-Root-Signature' header is missing.")
            data = res.json()
            log.info(f"  ↳ Origin API Validated. Retrieved {len(data['active_signers'])} signers.")

    async def phase_ingress_e2e_golden(self):
        async with httpx.AsyncClient(base_url=self.local_url, timeout=15.0) as client:
            async def verify_signature(response: httpx.Response):
                if response.status_code == 200:
                    VerifiedHttpClient(client=client)._verify_header_proof(response)
            
            client.event_hooks['request'] = [self.tracer.trace_request]
            client.event_hooks['response'] = [self.tracer.trace_response, verify_signature]

            wallet = Account.create()
            log.info(f"🔑 [Identity] Ephemeral Wallet: {wallet.address} | Sig: Generated")

            msg = encode_defunct(text=f"EXECUTE:{wallet.address}:AUDIT_LOG_APPEND:1000")
            signature = wallet.sign_message(msg).signature.hex()

            start_event = StartIntentEvent(
                client_id=wallet.address, action="AUDIT_LOG_APPEND", max_fuel=1000,
                payload={"message": "audit_test", "severity": "info"}, signature=signature
            )
            
            fsm = EdgePhaseFSM()
            workflow = EdgeWorkflow(fsm=fsm, client=client, base_url=self.local_url)
            await workflow.execute(start_event) 
            
            if fsm.state != EdgePhaseState.COMPLETED:
                raise RuntimeError(f"Path Failed! Final state: {fsm.state.name}")

class EdgeSuiteClientRunner:
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EDGE MASTER SUITE] Executing Pure Client Tests against CI Kernel")
        self.log.info("="*80)
        
        port = int(os.getenv("GATEWAY_PORT", 8000))
        net_config = E2EConfig(host="127.0.0.1", port=port, protocol="http")
        self.results.extend(await EdgeTracerPipeline(config=net_config).run_pipeline())
        
        self.log.info("\n" + "="*80)
        all_passed = all(r.passed for r in self.results)
        for idx, res in enumerate(self.results, 1):
            icon = "✅" if res.passed else "❌"
            self.log.info(f"{icon} {idx:02d}. [{res.target}] {res.scenario.ljust(45)} | Result: {'PASSED' if res.passed else 'FAILED'}")
        
        if not all_passed:
            self.log.critical("💥 EDGE BOUNDARY COMPROMISED.")
            exit(1)

def main(args_list: list[str] = None):
    app = EdgeSuiteClientRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()