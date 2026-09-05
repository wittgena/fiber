# fiber.phase.e2e.topos
import sys
import argparse
import logging
import asyncio
from typing import Any, List, Dict

import httpx
from eth_account import Account
from eth_account.messages import encode_defunct

from fiber.dphi.edge.workflow import EdgeWorkflow
from fiber.phase.kernel.plane.topos import ToposOrchestrator, ToposContext

from xphi.kernel.dphi.fsm.edge import EdgePhaseFSM, EdgePhaseState, StartIntentEvent
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("e2e.topos")

# =====================================================================
# 1. Distributed Scene (Black-box & White-box Network Verifier)
# =====================================================================

class ToposDistributedScene:
    def __init__(self, broker: Any = None, context: ToposContext = None):
        self.broker = broker
        self.context = context
        
        self.router = context.router if context else None
        self.base_url = self.router.host_url if self.router else "http://127.0.0.1:8000"
        self.auditors = context.auditors if context else {}
        
        self.fail_count = 0
        self.failed_cases: List[Dict[str, str]] = []
        self.log = get_emitter("scene.topos")

    async def _execute_fsm_workflow(self, scenario_name: str, tamper_signature: bool = False):
        """EdgeWorkflow(FSM)를 사용하여 분산망 횡단 테스트 수행"""
        
        headers = self.router.build_headers() if self.router else {}
        async with httpx.AsyncClient(base_url=self.base_url, timeout=20.0, headers=headers) as client:
            ## 클라이언트(테스터) 자격 증명 생성
            wallet = Account.create()
            client_id = wallet.address
            action = "EXECUTE_PYTHON_DISTRIBUTED"
            max_fuel = 2000000
            source_code = "print('Hello from Distributed Topos Resonance Test')"

            ## 서명 생성 (결함 주입 시 위조)
            if tamper_signature:
                signature = "0x_tampered_invalid_signature_for_topos_testing"
            else:
                sig_text = f"EXECUTE:{client_id}:{action}:{max_fuel}"
                msg = encode_defunct(text=sig_text)
                signature = wallet.sign_message(msg).signature.hex()

            start_event = StartIntentEvent(
                client_id=client_id,
                action=action,
                max_fuel=max_fuel,
                source_code=source_code,
                signature=signature
            )

            ## FSM 기반 클라이언트 워크플로우 인스턴스화
            fsm = EdgePhaseFSM()
            workflow = EdgeWorkflow(fsm=fsm, client=client, base_url=self.base_url)
            
            try:
                await workflow.execute(start_event)
                
                ## 시나리오별 거시 상태(Macro State) 검증
                if not tamper_signature and fsm.state != EdgePhaseState.COMPLETED:
                    raise RuntimeError(f"Golden Path Failed! Final FSM state: {fsm.state.name}")
                    
                if tamper_signature and fsm.state != EdgePhaseState.FAILED:
                    raise RuntimeError(f"Negative Path Failed! Expected FAILED, got: {fsm.state.name}")
                    
            except Exception as e:
                self.fail_count += 1
                self.failed_cases.append({"title": scenario_name, "error": str(e)})
                self.log.error(f"  [SCENARIO HALTED] {scenario_name}: {e}")

    async def phase_distributed_resonance(self):
        """[정상 경로] Gateway -> Redis -> Compute Worker -> Redis -> Gateway의 1-Cycle 왕복 검증"""
        self.log.info("  ▶️ [TEST] Distributed Resonance (Golden Path)")
        await self._execute_fsm_workflow("Distributed Resonance (Golden Path)", tamper_signature=False)

    async def phase_edge_ingress_rejection(self):
        """[결함 경로] 엣지 게이트웨이가 위조된 서명을 쳐내고 인프라가 붕괴하지 않는지 검증"""
        self.log.info("  ▶️ [TEST] Edge Ingress Defense (Negative Path)")
        await self._execute_fsm_workflow("Edge Ingress Defense (Tampered Sig)", tamper_signature=True)
        
        # [개선] Auditor를 이용한 물리/의미론적 상태 단언 (Cross-validation)
        if "state" in self.auditors:
            gateway_state = self.auditors["state"]
            if not gateway_state.is_running:
                msg = f"보안 방어 로직 수행 중 게이트웨이가 크래시 됨! (ExitCode: {gateway_state.exit_code})"
                self.log.error(f"  [FATAL_RUPTURE] {msg}")
                self.fail_count += 1
                self.failed_cases.append({"title": "Edge Ingress Physical State", "error": msg})
            else:
                self.log.info("  └─ Physical Boundary Intact: Gateway survived the faulty ingress ✅")

    async def run_all(self):
        self.log.info("\n=== [START] Executing Topos Distributed Scenes ===")
        
        # 1. 헬스체크: 타겟 클러스터망 접근성 확인
        try:
            headers = self.router.build_headers() if self.router else {}
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{self.base_url}/keys", headers=headers, timeout=3.0)
                if res.status_code != 200:
                    raise ConnectionError(f"Gateway returned HTTP {res.status_code}")
            self.log.info("  └─ Topos Gateway Connectivity: Confirmed 🟢")
        except Exception as e:
            self.log.error(f"  └─ Topos Gateway Connectivity: Failed 🔴 ({e})")
            self.fail_count += 1
            self.failed_cases.append({"title": "Cluster Health Check", "error": str(e)})
            return # 연결 안 되면 후속 테스트 중단

        # 2. 독립된 시나리오 순차 실행
        await self.phase_distributed_resonance()
        await self.phase_edge_ingress_rejection()
        
        # 3. 결과 정리
        if self.fail_count == 0:
            self.log.info("=== [DONE] All Topos Scenes Passed Successfully ===")
        else:
            self.log.warning(f"=== [DONE] Topos Scenes Completed with {self.fail_count} Failures ===")


# =====================================================================
# 2. Topos Orchestrator Flow (E2E Runner)
# =====================================================================

class ToposFlow:
    def __init__(self, mode: str = "dev", suites: List[str] = None, keep_workspace: bool = False):
        self.mode = mode
        self.suites = suites or []
        self.keep_workspace = keep_workspace

    async def test(self):
        log.info(f"\n[PHASE 1] Initializing Topos Orchestrator in [{self.mode.upper()}] mode")
        
        broker = None 

        # [개선] ToposOrchestrator 사용
        controller = ToposOrchestrator(
            target_name="dphi-topos-sandbox",
            mode=self.mode,
            timeout=120,
            suites={"distributed_fsm": ToposDistributedScene} 
        )
        controller.keep_workspace = self.keep_workspace
        
        success, err_msg = await controller.execute(broker=broker)
        
        log.info("\n" + "="*75)
        log.info("🚀 TOPOS CLUSTER PIPELINE EXECUTION REPORT 🚀".center(75))
        log.info("="*75)
        
        if success:
            log.info(f"🟢 [SUCCESS] All Topology Declarative Tests PASSED.")
            log.info("="*75 + "\n")
        else:
            log.critical(f"🔴 [FAILED] Topos Test execution terminated with errors.")
            for line in err_msg.split('\n'):
                if line.strip():
                    log.critical(f"📝 {line}")
                
            if hasattr(controller, 'suite_runners') and controller.suite_runners:
                log.info("\n" + "🔥"*37)
                log.info("🚨 DETAILED TOPOS FAILURE TRACES 🚨".center(75))
                log.info("🔥"*37)
                
                for suite_name, runner in controller.suite_runners.items():
                    fail_cnt = getattr(runner, 'fail_count', 0)
                    failed_cases = getattr(runner, 'failed_cases', [])
                    
                    if fail_cnt > 0 or failed_cases:
                        log.info(f"\n❌ [SUITE: {suite_name.upper()}] ➔ {fail_cnt} Test(s) Failed")
                        for idx, fc in enumerate(failed_cases, 1):
                            title = fc.get('title', 'Unknown Test Case')
                            err = fc.get('error', 'No error details provided')
                            log.info(f"  └─ {idx}. {title}")
                            log.info(f"     [Reason] {err}\n")
                log.info("="*75 + "\n")
            sys.exit(1)

    async def run(self):
        await self.test()


# =====================================================================
# 3. Standard Entrypoint
# =====================================================================

def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="DPHI Topology (Docker Compose) E2E Orchestrator")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev", help="Execution mode")
    parser.add_argument("--keep-workspace", action="store_true", help="Preserve compose files after test")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    
    args, _ = parser.parse_known_args(args_list)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal execution logging is ENABLED.")

    app = ToposFlow(
        mode=args.mode,
        keep_workspace=args.keep_workspace
    )
    PhaseReactor.ignite(main_coro_func=app.run)

if __name__ == "__main__":
    main()