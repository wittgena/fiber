# bound.exchange.intent.entry
import asyncio
import os
from dataclasses import dataclass
from typing import List

from phase.anchor.config.dphi import dphi_env, NetEnv
from bound.exchange.intent.workflow import ExchangeWorkflow, ScenarioConfig
from receptor.ingress.sentinel import RpcChaosInjector

from kernel.phase.reactor import PhaseReactor
from watcher.plane.emitter import get_emitter

log = get_emitter("exchange.entry")

@dataclass
class TestResult:
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


class ExchangeDomainRunner:
    """Orchestrates the Exchange E2E Pipeline integrating DVM, State Sync, and Attestation."""
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []
        
        is_local_mode = (dphi_env.mode == NetEnv.LOCAL)
        has_real_pkey = os.getenv(dphi_env.agents.alpha.private_key_env_var) is not None
        self.should_simulate = is_local_mode or not has_real_pkey

    async def _run_domain_workflows(self):
        self.log.info("\n▶️ [EXCHANGE DOMAIN] Initiating Pipeline Execution Sequences...")
        if not self.should_simulate:
            self.log.info(f"⚡ [Mode] Real EVM Keys detected. External Ledger Sync (EVM) will target Chain ID: {dphi_env.network.chain_id}")
        else:
            self.log.info("🛡️ [Mode] Executing in Local Simulation (External Ledger Sync Bypassed).")

        scenarios = [
            {
                "config": ScenarioConfig(
                    name="Standard Pipeline (DVM Simulation -> EVM Ledger Sync -> Notary Attestation)",
                    mandate_injector=None,
                    signature_injector=None
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="Ingress Rejection (Expired AP2 Mandate Constraint)",
                    mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                    signature_injector=None
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="Attestation Failure (Invalid Cryptographic Signatures at Export Phase)", 
                    mandate_injector=None,
                    signature_injector=RpcChaosInjector.corrupt_consensus_signatures
                ),
                "expected": True
            }
        ]

        for item in scenarios:
            scenario_config = item["config"]
            expected = item["expected"]
            workflow = ExchangeWorkflow(scenario=scenario_config, simulate_wallet=self.should_simulate)
            is_success = await workflow.start()
            
            self.results.append(TestResult(
                target="EXCHANGE",
                scenario=scenario_config.name,
                success=is_success,
                expected_success=expected
            ))
            await asyncio.sleep(0.5)

    def _print_report(self):
        self.log.info("\n" + "="*80)
        self.log.info("📊 [EXCHANGE DOMAIN EXECUTION REPORT]")
        self.log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res.scenario.ljust(75)} | Result: {status_text}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL PIPELINE SCENARIOS EXECUTED AS EXPECTED.")
        else:
            self.log.critical("💥 PIPELINE EXECUTION FAILED. Inspect structural logs for deviations.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EXCHANGE SUITE] Commencing Exchange Domain Reactor")
        self.log.info("="*80)
        
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = ExchangeDomainRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()