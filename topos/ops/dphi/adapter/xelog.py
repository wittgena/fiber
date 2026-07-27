# topos.ops.dphi.adapter.xelog
## @lineage: ops.dphi.adapter.xelog
import sys
import asyncio
from typing import Tuple

from topos.ops.dphi.entry import WasmPipelineCLI
from topos.ops.xelog.edge.scheme.runner import E2EScenarioOrchestrator 
from phase.wasm.tracer import WasmTracer
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.xelog")

class E2EWebTesterAdapter:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.orchestrator = E2EScenarioOrchestrator(base_url)
        self.auditors = [] 

    async def execute(self) -> Tuple[bool, str]:
        try:
            log.info("\n[WebTesterAdapter] Starting E2E HTTP Workflows...")
            otlp_root = await self.orchestrator.run_genai_otlp()
            d3fi_root = await self.orchestrator.run_d3fi_trade()
            audit_root = await self.orchestrator.run_audit_trail()
            
            state_roots = {"otlp_root": otlp_root, "d3fi_root": d3fi_root, "audit_root": audit_root}
            if "failed" in state_roots.values():
                self.orchestrator.report()
                return False, "E2E HTTP Scenario Validation Failed (Some roots returned 'failed')"

            await self.orchestrator.run_global_anchor(state_roots)
            self.orchestrator.report()
            if self.orchestrator.fail_count > 0:
                return False, "E2E Errors occurred during execution. Check logs."
                
            return True, ""
        except Exception as e:
            return False, f"E2E WebRunner Critical Exception: {str(e)}"
        finally:
            await self.orchestrator.client.aclose()

class AdvancedPipelineCLI(WasmPipelineCLI):
    async def e2e_pipeline(self):
        """Build -> Ledger Validate -> E2E HTTP Test -> Trace Loop"""
        self.log.info("[CLI] Starting Full E2E Pipeline (Build ➔ Ledger Validate ➔ HTTP E2E ➔ Seal)...")
        
        web_tester = E2EWebTesterAdapter(base_url="http://localhost:8000")
        tracer = WasmTracer(tester=web_tester)
        await tracer.execute()
        if getattr(tracer, 'rupture_confirmed', False):
            self.log.error("[CLI] E2E Pipeline ended in a Rupture/Collapse state.")
            sys.exit(1)
            
        self.log.info("[CLI] E2E Pipeline executed & Lineage Sealed successfully.")

if __name__ == "__main__":
    app = AdvancedPipelineCLI()
    asyncio.run(app.e2e_pipeline())