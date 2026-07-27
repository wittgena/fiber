# swarm.mesh.scheme.runtime
import asyncio
from abc import ABC, abstractmethod
from typing import Any

from watcher.dphi.broker import WasmBroker
from watcher.plane.emitter import get_emitter
from arch.xor.parser.block.contract import Contract, CoherenceState
from swarm.mesh.executor import WasmExecutor, TaskContext

log = get_emitter("scheme.runtime")

class RuntimeSchemeRunner(ABC):
    """
    @desc: WasmExecutor를 심장(Engine)으로 삼아, 불변 Contract 스트림을 구독하고 
           하위 Scheme(Recovery/Syzygy)에 이벤트를 라우팅하는 런타임 오퍼레이터.
    """
    def __init__(self, broker: WasmBroker):
        self.broker = broker
        self.executor = WasmExecutor(broker)
        self.is_running = False

    async def watch_and_react(self, initial_context: TaskContext):
        """
        [핵심 개선] 단발성 실행이 아닌, Executor의 스트림을 무한히 관측하는 루프
        """
        self.is_running = True
        log.info(f"[RuntimeRunner] Activated pattern for task: {initial_context.task_type}")
        
        # Executor가 방출하는 불변 Contract 스트림에 감응
        async for contract in self.executor.execute_stream(initial_context):
            if not self.is_running:
                break
            
            # 파생된 Scheme(Recovery/Syzygy)의 구체적인 패턴 로직으로 라우팅
            await self.on_contract_emitted(contract)

    @abstractmethod
    async def on_contract_emitted(self, contract: Contract):
        """하위 Scheme 클래스들이 반드시 구현해야 하는 감응(React) 인터페이스"""
        pass

    def stop(self):
        self.is_running = False

# ---------------------------------------------------------
# 2. Recovery Scheme: Contract 상태에 반응하는 런타임 복구 패턴
# ---------------------------------------------------------
class AutonomousRecoveryScheme(RuntimeSchemeRunner):
    """
    @desc: Contract 스트림을 지켜보다가 FRAGMENTED(결함)가 감지되면 
           XOR 복원 및 DAG Rebase를 주입하는 런타임 패턴.
    """
    def __init__(self, broker: WasmBroker):
        super().__init__(broker)
        # 서명 키 등 복구에 필요한 도구 초기화 (생략)

    async def on_contract_emitted(self, contract: Contract):
        # 1. 정상 상태일 때는 관측만 하고 개입하지 않음
        if contract.state == CoherenceState.STREAMING:
            log.trace(f"[RecoveryObserver] Node streaming normally. Topos: {contract.topos_id}")
            return
            
        # 2. 크래시(OOM, 파티션) 발생 시 복구 트리거 가동
        if contract.state == CoherenceState.FRAGMENTED:
            log.warning(f"[RecoveryObserver] Anomaly detected! Phase lost at Topos: {contract.topos_id}")
            await self._trigger_parity_recovery(contract)
            
        # 3. 작업이 정상 완료되었을 때의 후처리
        elif contract.state == CoherenceState.COHERENT and contract.kind == "EXECUTION_COMPLETE":
            log.info(f"[RecoveryObserver] Task finalized coherently. Nexus: {contract.nexus_id}")

    async def _trigger_parity_recovery(self, failed_contract: Contract):
        """기존의 _step2, _step3 로직이 이곳으로 통합되어 이벤트 발생 시 호출됨"""
        log.info(f"--- Initiating Parity Recovery for Nexus {failed_contract.nexus_id} ---")
        
        # 복구를 위한 새로운 TaskContext 생성 후 Executor에 재주입 (Feed-back)
        recovery_context = TaskContext(
            task_type="verify_parity",
            payload={"nexus_id": failed_contract.nexus_id, "topos_id": failed_contract.topos_id}
        )
        
        # 복구 프로세스 자체도 Executor를 통해 불변 Contract로 방출됨
        async for recovery_contract in self.executor.execute_stream(recovery_context):
            if recovery_contract.state == CoherenceState.COHERENT:
                log.info("Recovery and Rebase successful. Sealing to ledger.")
                # 원장(Ledger) 기록 로직 수행
                break

class SyzygyResonanceScheme(RuntimeSchemeRunner):
    """
    @desc: 망 분리(Split) 이후 패리티 충돌(Parity Delta)을 감지하고 
           소수파(Minority)를 Void로 격리하는 합의 패턴.
    """
    async def on_contract_emitted(self, contract: Contract):
        # 특정 위상 충돌 이벤트(kind)를 구독
        if contract.kind == "TOPOLOGY_DRIFT_DETECTED":
            log.warning(f"[SyzygyObserver] Drift detected at Phase: {contract.phase_id}")
            await self._seal_void_nexus(contract)

    async def _seal_void_nexus(self, contract: Contract):
        # 소수파 격리 로직 수행
        pass