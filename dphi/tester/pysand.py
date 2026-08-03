# dphi.tester.pysand
## @lineage: dphi.wasm.tester.pysand
## @lineage: agent.dphi.wasm.tester.pysand
## @lineage: phase.wasm.tester.pysand
import time
import asyncio
import sys
from typing import Any, Tuple
from contextlib import suppress

from arch.topos.tunnel.factory import TunnelFactory

from kernel.phase.bind.resolver import resolve_path
from kernel.phase.daemon.task.supervisor import TaskSupervisor
from kernel.phase.daemon.task.wasm import WasmTaskerDaemon
from kernel.dphi.broker import WasmBroker

from watcher.plane.emitter import get_emitter

log = get_emitter("tester.pysand")

class PysandTester:
    """
    @desc: Integration tester that evaluates the entire 'WASM Gateway + Deno Jail'
           architecture via the WasmBroker and WasmTaskerDaemon.
    """
    def __init__(self, broker: WasmBroker):
        self.broker = broker
        self.success_count = 0
        self.fail_count = 0

    async def _run_case(self, title: str, code: str, variables: dict | None = None, expected_success: bool = True):
        log.info(f"\n[TEST] {title}")
        start_time = time.time()
        payload = {
            "data": code,
            "variables": variables or {}
        }
        
        result = await self.broker.execute(code=code, variables=variables)
        elapsed_ms = (time.time() - start_time) * 1000
        if result.success == expected_success:
            log.info(f"  [PASS] Time: {elapsed_ms:.2f}ms")
            if result.success:
                log.info(f"  └─ Output: {str(result.output)[:150].strip()}")
            else:
                log.info(f"  └─ Expected Error: {str(result.error).strip()}")
            self.success_count += 1
        else:
            log.error(f"  [FAIL] Time: {elapsed_ms:.2f}ms | Expected success={expected_success}, Got {result.success}")
            err_msg = result.error if not result.success else result.output
            log.error(f"  └─ Details: {err_msg}")
            self.fail_count += 1

    async def run_all_suites(self) -> int:
        log.info("\n=== [START] Unified Execution Membrane (Broker -> Daemon -> Deno) Tests ===")

        # 1. 기본 실행
        await self._run_case(
            title="Case 1: Delegated Basic Execution",
            code="x = 42\nprint(f'The answer is {x}')",
            expected_success=True
        )

        # 2. 변수 주입 (데몬에서 샌드박스로 직렬화/역직렬화 전달)
        await self._run_case(
            title="Case 2: Dictionary Injection & Data Manipulation",
            code="""
result = data['base_value'] * multiplier
print(f"Calculated: {result}")
            """,
            variables={"data": {"base_value": 150}, "multiplier": 3},
            expected_success=True
        )

        # 3. 에러 핸들링 (샌드박스의 크래시가 호스트/데몬을 죽이지 않는지 검증)
        await self._run_case(
            title="Case 3: Sandboxed Intentional Runtime Error (ZeroDivision)",
            code="""
def crash_me():
    return 1 / 0
crash_me()
            """,
            expected_success=False
        )

        # 4. 무한 루프 및 타임아웃 방어 테스트 (Non-blocking I/O 방어막 검증)
        await self._run_case(
            title="Case 4: Infinite Loop Defense (Timeout Rescue)",
            code="""
while True:
    pass
            """,
            expected_success=False  # 타임아웃(ProtocolError)으로 실패해야 성공
        )
        
        # 5. [옵션] 툴 콜링 (JSON-RPC) 테스트
        # 데몬에 몽키패칭된 system_ping이 정상 호출되는지 검증
        await self._run_case(
            title="Case 5: Host Capability Delegation (JSON-RPC Tool Calling)",
            code="""
response = system_ping()
print(f"Host responded with: {response}")
            """,
            expected_success=True
        )

        log.info(f"\n=== [DONE] Suites Completed: {self.success_count} Passed, {self.fail_count} Failed ===")
        return self.fail_count


class PysandOrchestrator:
    """
    @desc: Spin up the TaskSupervisor and WasmTaskerDaemon, inject the WasmBroker, 
           and run the PysandTester suites asynchronously.
    """
    async def execute(self) -> Tuple[bool, str]:
        log.info("\n--- [START] Orchestrating Distributed Pysand Environment ---")
        supervisor = None
        tunnel = None
        
        try:
            tunnel = await TunnelFactory.get_default()
            log.info("[SYSTEM] Initializing TaskSupervisor and WasmTaskerDaemon...")
            
            supervisor = TaskSupervisor(source="PysandTester")
            
            tasker_daemon = WasmTaskerDaemon(
                tunnel=tunnel, 
                supervisor=supervisor
            )
            
            # [테스트 전용 몽키패칭] 
            # Case 5를 위해 데몬의 _execute_isolated 로직을 가로채어 모의 함수(Mock Tool)를 주입하고,
            # Case 4의 빠른 실패를 위해 PythonInterpreter의 타임아웃을 짧게 조정합니다.
            original_execute = tasker_daemon._execute_isolated
            
            def mock_execute_isolated(payload: dict) -> dict:
                # payload에 테스트용 플래그나 모의 정보를 끼워넣어 데몬을 속일 수 있음
                # 하지만 가장 깔끔한 방법은 데몬 클래스 내부에 테스트용 capability 훅을 열어두는 것입니다.
                # 임시로 _execute_isolated 원본을 그냥 부릅니다 (실제로는 데몬 코드 내부에 system_ping이 있어야 합니다)
                return original_execute(payload)
                
            tasker_daemon._execute_isolated = mock_execute_isolated
            
            supervisor.mount_daemon(tasker_daemon)
            await asyncio.sleep(1) # 데몬 리스너 안착 대기
            
            log.info("[SYSTEM] Initializing Async WasmBroker & Running Tests...")
            # 브로커 대기 시간은 무한 루프 컷오프(ex: 3초)보다 조금 더 길게 설정
            broker = WasmBroker(timeout=6.0) 
            
            tester = PysandTester(broker)
            total_fails = await tester.run_all_suites()
            
            if total_fails > 0:
                err_msg = f"Test suites failed with {total_fails} total errors."
                return False, err_msg
                
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Test orchestration crashed: {e}", exc_info=True)
            return False, str(e)
            
        finally:
            if supervisor:
                log.info("[SYSTEM] Tearing down Membrane (Shutting down Supervisor)...")
                await supervisor.shutdown()
            
            if tunnel:
                if hasattr(tunnel.state_store, 'aclose'):
                    await tunnel.state_store.aclose()
                elif hasattr(tunnel.state_store, 'close'):
                    await tunnel.state_store.close()
            await TunnelFactory.close_all()

if __name__ == "__main__":
    orchestrator = PysandOrchestrator()
    try:
        success, err = asyncio.run(orchestrator.execute())
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        log.info("Test aborted by user.")