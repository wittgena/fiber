# dphi.exchange.suite
import asyncio
from kernel.phase.reactor import KernelReactor
from watcher.plane.emitter import get_emitter

from dphi.exchange.workflow import ExchangeWorkflow, ScenarioConfig
from dphi.exchange.chaos.injector import RpcChaosInjector
from dphi.exchange.net.tracer import TracerPipeline, E2EConfig
from dphi.exchange.mock.config import mock_env

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

    # 🌟 철학적 정렬: 시나리오의 명칭과 기대 결과(Assertion)를 De-blockchain 구조에 맞게 완벽히 재정의합니다.
    scenarios = [
        {
            "config": ScenarioConfig(
                name="Golden Path (Pure Core + Valid Notaries)",
                mandate_injector=None,
                signature_injector=None
            ),
            "expected_workflow_result": True # 코어 연산 및 정상 공증 포장 완료
        },
        {
            "config": ScenarioConfig(
                name="Core Rejection: Expired AP2 Mandate",
                mandate_injector=RpcChaosInjector.corrupt_ap2_mandate,
                signature_injector=None
            ),
            "expected_workflow_result": False # 💥 코어(Phase 1)가 인텐트를 거부하므로 워크플로우 중단되어야 함
        },
        {
            "config": ScenarioConfig(
                # 기존 "Byzantine Fault" 에서 외부 공증 위조로 명칭 변경
                name="Export Forgery: Invalid Notary Attestations", 
                mandate_injector=None,
                signature_injector=RpcChaosInjector.corrupt_consensus_signatures
            ),
            # 🌟 핵심 정렬: 도장(Signature)이 위조되었더라도, 코어의 관점에서는 연산과 포장을 
            # 무사히 마친 것이므로 워크플로우 자체는 성공(True)으로 끝나야 합니다. 
            # (이 페이로드가 L2 EVM에 던져지면 스마트 컨트랙트가 거절할 것입니다.)
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
        log.info(f"{status_icon} {idx:02d}. {target_label.ljust(12)} {res['scenario'].ljust(50)} | Result: {status_text}")
        
    log.info("-" * 80)
    if all_passed:
        log.info("🎉 ALL TESTS (NETWORK & WORKFLOW) EXECUTED SUCCESSFULLY.")
    else:
        log.critical("💥 SOME TESTS FAILED. Check the execution logs for trace details.")
    log.info("="*80 + "\n")

if __name__ == "__main__":
    KernelReactor.ignite(main_coro_func=run_scenario_suite)