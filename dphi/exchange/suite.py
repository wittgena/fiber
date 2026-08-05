# dphi.exchange.suite
import asyncio
from kernel.phase.reactor import KernelReactor
from watcher.plane.emitter import get_emitter

# 도메인 워크플로우 테스트 임포트
from dphi.exchange.workflow import ExchangeWorkflow, ScenarioConfig
from dphi.exchange.chaos.injector import RpcChaosInjector

# [개선 1] 네트워크 테스트 파이프라인 임포트 추가
from dphi.exchange.net.tracer import TracerPipeline, E2EConfig

log = get_emitter("exchange.suite")

async def run_scenario_suite():
    log.info("\n" + "="*80)
    log.info("🧪 [DPHI E2E MASTER SUITE] Commencing Full System Tests (Network + Workflow)")
    log.info("="*80)

    results = []

    # =========================================================================
    # PART 1: Network & Security Membrane Tests (net.tester)
    # =========================================================================
    log.info("\n▶️ [PART 1] Running Network & Chaos Membrane Pipeline...")
    
    # E2E 설정 인스턴스화
    net_config = E2EConfig(host="localhost", port=8000, protocol="http")
    tracer_pipeline = TracerPipeline(config=net_config)
    
    net_success = True
    try:
        # TracerPipeline은 내부에서 예외를 발생시키므로 try-except로 캡처
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


    # =========================================================================
    # PART 2: Domain Workflow Tests (exchange.workflow)
    # =========================================================================
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
        
        # Injector가 존재한다면 시스템이 공격을 차단(Fail)해야 테스트 성공으로 판정됨
        expected_success = not (scenario.mandate_injector or scenario.signature_injector)
        
        results.append({
            "target": "WORKFLOW",
            "scenario": scenario.name,
            "success": is_success,
            "expected_success": expected_success
        })
        await asyncio.sleep(0.5)


    # =========================================================================
    # PART 3: Comprehensive Test Report
    # =========================================================================
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
            
        # UI/UX 정렬: target을 명시하여 네트워크/워크플로우 결과 구분
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