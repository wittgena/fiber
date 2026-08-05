# dphi.exchange.suite
import asyncio
from kernel.phase.reactor import KernelReactor
from watcher.plane.emitter import get_emitter

from dphi.exchange.workflow import ExchangeWorkflow, ScenarioConfig
from dphi.exchange.chaos.injector import RpcChaosInjector
from dphi.exchange.net.tracer import TracerPipeline, E2EConfig

log = get_emitter("exchange.suite")

async def run_scenario_suite():
    log.info("\n" + "="*80)
    log.info("🧪 [DPHI E2E MASTER SUITE] Commencing Full System Tests (Network + Workflow)")
    log.info("="*80)

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

    # 리포트를 위해 결과 저장 (target 구분값 추가)
    results.append({
        "target": "NET_TEST",
        "scenario": "TracerPipeline (Golden, Negative, Chaos)",
        "success": net_success,
        "expected_success": True
    })

    log.info("\n▶️ [PART 2] Running Domain Workflow Scenarios...")
    scenarios = [
        ScenarioConfig(
            name="Golden Path (Success)",
            mandate_injector=None,
            signature_injector=None
        ),
        ScenarioConfig(
            name="Expired AP2 Mandate Rejection",
            mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
            signature_injector=None
        ),
        ScenarioConfig(
            name="Byzantine Fault (Corrupted Signature)",
            mandate_injector=None,
            signature_injector=RpcChaosInjector.corrupt_consensus_signatures
        )
    ]

    for scenario in scenarios:
        workflow = ExchangeWorkflow(scenario=scenario, simulate_wallet=True)
        is_success = await workflow.start()
        expected_success = not (scenario.mandate_injector or scenario.signature_injector)
        results.append({
            "target": "WORKFLOW",
            "scenario": scenario.name,
            "success": is_success,
            "expected_success": expected_success
        })
        await asyncio.sleep(0.5)

    log.info("\n" + "="*80)
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
        log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res['scenario'].ljust(45)} | Result: {status_text}")
        
    log.info("-" * 80)
    if all_passed:
        log.info("🎉 ALL TESTS (NETWORK & WORKFLOW) EXECUTED SUCCESSFULLY.")
    else:
        log.critical("💥 SOME TESTS FAILED. Check the execution logs for trace details.")
    log.info("="*80 + "\n")

if __name__ == "__main__":
    KernelReactor.ignite(main_coro_func=run_scenario_suite)