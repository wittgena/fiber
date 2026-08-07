# exchange.suite
import asyncio
from kernel.phase.reactor import KernelReactor
from watcher.plane.emitter import get_emitter

from exchange.workflow import ExchangeWorkflow, ScenarioConfig
from surface.tester.chaos.injector import RpcChaosInjector
from exchange.net.tracer import TracerPipeline, E2EConfig
from exchange.net.config import mock_env

log = get_emitter("exchange.suite")

async def run_scenario_suite():
    log.info("\n" + "="*59)
    log.info("🧪 [DPHI E2E MASTER SUITE] Commencing Full System Tests (Network + Workflow)")
    log.info("="*59)

    results = []
    log.info("\n▶️ [PART 1] Running Network & Chaos Membrane Pipeline...")
    net_config = E2EConfig(host="localhost", port=8000, protocol="http")
    tracer_pipeline = TracerPipeline(config=net_config)
    
    net_success = True
    try:
        await tracer_pipeline.run_pipeline()
    except Exception as e:
        log.error(f"Network Pipeline Halted: {str(e)}")
        net_success = False

    results.append({
        "target": "NET_TEST",
        "scenario": "TracerPipeline (Golden, Negative, Chaos)",
        "success": net_success,
        "expected_success": True
    })

    log.info("\n▶️ [PART 2] Running Domain Workflow Scenarios...")
    
    has_testnet_keys = bool(mock_env.cdp_wallet.api_name and mock_env.cdp_wallet.api_private_key)
    should_simulate = not has_testnet_keys
    
    if not should_simulate:
        log.info(f"⚡ [Notice] Testnet Keys detected. Workflows will execute LIVE on {mock_env.cdp_wallet.network_id}!")
    else:
        log.info(f"🛡️ [Notice] No Testnet Keys. Workflows will execute in SIMULATION mode.")

    scenarios = [
        {
            "config": ScenarioConfig(
                name="Golden Path (Pure Core + Valid Notaries)",
                mandate_injector=None,
                signature_injector=None
            ),
            "expected_workflow_result": True
        },
        {
            "config": ScenarioConfig(
                name="Core Rejection: Expired AP2 Mandate",
                mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                signature_injector=None
            ),
            "expected_workflow_result": False
        },
        {
            "config": ScenarioConfig(
                name="Export Forgery: Invalid Notary Attestations", 
                mandate_injector=None,
                signature_injector=RpcChaosInjector.corrupt_consensus_signatures
            ),
            "expected_workflow_result": True 
        }
    ]

    for item in scenarios:
        scenario = item["config"]
        expected_success = item["expected_workflow_result"]
        
        workflow = ExchangeWorkflow(scenario=scenario, simulate_wallet=should_simulate)
        is_success = await workflow.start()
        
        results.append({
            "target": "WORKFLOW",
            "scenario": scenario.name,
            "success": is_success,
            "expected_success": expected_success
        })
        await asyncio.sleep(0.5)

    log.info("\n" + "="*59)
    log.info("📊 [MASTER TEST SUITE REPORT]")
    log.info("="*80)
    
    all_passed = True
    for idx, res in enumerate(results, 1):
        passed = (res['success'] == res['expected_success'])
        status_icon = "✅" if passed else "❌"
        status_text = "PASSED" if passed else "FAILED"
        if not passed:
            all_passed = False
            
        target_label = f"[{res['target']}]"
        log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res['scenario'].ljust(50)} | Result: {status_text}")
        
    log.info("-" * 59)
    if all_passed:
        log.info("🎉 ALL TESTS (NETWORK & WORKFLOW) EXECUTED SUCCESSFULLY.")
    else:
        log.critical("💥 SOME TESTS FAILED. Check the execution logs for trace details.")
    log.info("="*59 + "\n")

if __name__ == "__main__":
    KernelReactor.ignite(main_coro_func=run_scenario_suite)