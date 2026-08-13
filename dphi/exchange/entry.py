# dphi.exchange.entry
import asyncio
from dataclasses import dataclass
from typing import List

from dphi.adapter.config.dphi import mock_env
from dphi.exchange.workflow import ExchangeWorkflow, ScenarioConfig
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
    """Orchestrates the Pure Domain Logic E2E Test Suite (D3Fi Exchange)."""
    def __init__(self):
        self.log = log
        self.results: List[TestResult] = []
        
        has_testnet_keys = bool(mock_env.cdp_wallet.api_name and mock_env.cdp_wallet.api_private_key)
        self.should_simulate = not has_testnet_keys

    async def _run_domain_workflows(self):
        self.log.info("\n▶️ [EXCHANGE DOMAIN] Running Core Settlement Scenarios...")
        if not self.should_simulate:
            self.log.info(f"⚡ [Notice] Testnet Keys detected. Workflows will execute LIVE on {mock_env.cdp_wallet.network_id}!")
        else:
            self.log.info("🛡️ [Notice] Workflows will execute in SIMULATION mode.")

        scenarios = [
            {
                "config": ScenarioConfig(
                    name="Golden Path (Pure Core + Valid Notaries)",
                    mandate_injector=None,
                    signature_injector=None
                ),
                "expected": True
            },
            {
                "config": ScenarioConfig(
                    name="Core Rejection: Expired AP2 Mandate",
                    mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                    signature_injector=None
                ),
                "expected": False
            },
            {
                "config": ScenarioConfig(
                    name="Export Forgery: Invalid Notary Attestations", 
                    mandate_injector=None,
                    signature_injector=RpcChaosInjector.corrupt_consensus_signatures
                ),
                "expected": True  # 워크플로우 자체는 완료되나 영수증 내 서명이 오염됨
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
        self.log.info("📊 [EXCHANGE DOMAIN TEST REPORT]")
        self.log.info("="*80)
        
        all_passed = True
        for idx, res in enumerate(self.results, 1):
            status_icon = "✅" if res.passed else "❌"
            status_text = "PASSED" if res.passed else "FAILED"
            if not res.passed: all_passed = False
                
            target_label = f"[{res.target}]"
            self.log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res.scenario.ljust(50)} | Result: {status_text}")
            
        self.log.info("-" * 80)
        if all_passed:
            self.log.info("🎉 ALL DOMAIN SCENARIOS EXECUTED SUCCESSFULLY.")
        else:
            self.log.critical("💥 SOME DOMAIN TESTS FAILED. Check the business logic logs.")
        self.log.info("="*80 + "\n")

    async def execute(self):
        self.log.info("\n" + "="*80)
        self.log.info("🧪 [DPHI EXCHANGE SUITE] Commencing Core Domain Logic Tests")
        self.log.info("="*80)
        
        await self._run_domain_workflows()
        self._print_report()

def main():
    app = ExchangeDomainRunner()
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()