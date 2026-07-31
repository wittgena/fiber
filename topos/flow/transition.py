# topos.flow.transition
import asyncio
from enum import Enum
from typing import List, Optional, Dict, Any

from arch.contract.event.next import next_id
from watcher.plane.emitter import get_emitter

log = get_emitter("topos.transition")

class EdgeFlow(Enum):
    ZERO = "0"           # 구조적 정체성의 공백 (Void)
    COLLAPSED = "Φ⁻"     # 붕괴됨: 재결속 전 성찰 필요
    COHERENT = "Φ⁺"      # 일관된 판단: Dominium 앵커링 가능
    FRAGMENTED = "Φᶠ"    # 파편화된 기억: 실패했으나 재시도를 위해 보존됨
    DOMINIUM = "Ψᴰ"      # 앵커링된 최종 상태

class FlowTransition:
    def __init__(self, origin: str = "0"):
        self.id: str = next_id()
        self.origin: str = origin
        
        self.edge: EdgeFlow = EdgeFlow.ZERO
        self.reflective: bool = True
        self.reversible: bool = True
        
        self.memory: List[Dict[str, Any]] = []
        self.anchored_target: Optional[str] = None
        
        self.future: Optional[asyncio.Future] = None
        self._reset_future()

    def __repr__(self) -> str:
        return f"<PhaseNode Ψ({self.id}) | Origin: {self.origin} | State: {self.edge.value}>"

    def _reset_future(self) -> None:
        """@desc: 새로운 위상 진입을 위해 비동기 퓨처를 초기화합니다."""
        if self.future and not self.future.done():
            self.future.cancel()
        self.future = asyncio.Future()

    def record(self, message: str, state_change: Optional[EdgeFlow] = None) -> None:
        log_entry = {"event": message, "previous_state": self.edge.value}
        if state_change:
            log_entry["new_state"] = state_change.value
            self.edge = state_change
        self.memory.append(log_entry)

    def bind(self, target_phase: EdgeFlow) -> None:
        if self.edge == EdgeFlow.COLLAPSED and not self.reflective:
            raise ValueError("Collapsed node requires reflection before rebinding.")
        
        self.record(f"Bound to phase {target_phase.value}", state_change=target_phase)

    def threshold_test(self, lmbda: float, tau: float) -> bool:
        if lmbda < tau:
            self.record(f"Threshold failed: λ({lmbda}) < τ({tau})", state_change=EdgeFlow.FRAGMENTED)
            return False
        
        self.record(f"Threshold passed: λ({lmbda}) >= τ({tau})", state_change=EdgeFlow.COHERENT)
        return True

    def reach_dominium(self, resource_address: str) -> None:
        """@desc: Tracker의 역할 흡수 - 일관성 확보 시 최종 상태로 앵커링하고 Await Lock을 해제합니다."""
        if self.threshold_test(lmbda=1.0, tau=0.5):
            self.anchored_target = resource_address
            self.record(f"Anchored Dominium to {resource_address}", state_change=EdgeFlow.DOMINIUM)
            
            if self.future and not self.future.done():
                self.future.set_result(True)
        else:
            raise PermissionError(f"Cannot reach Dominium. Threshold test failed.")

    def fracture_topology(self, lmbda: float, tau: float, force_collapse: bool = False) -> None:
        """@desc: 에러나 타임아웃 발생 시 위상을 강제 붕괴(Collapse)시키고 Await Lock을 해제합니다."""
        self.threshold_test(lmbda=lmbda, tau=tau)
        
        if force_collapse:
            self.bind(EdgeFlow.COLLAPSED)
            
        if self.future and not self.future.done():
            self.future.set_result(False)

    def evaluate_tension(self, tension_grad: float, max_tau: float) -> None:
        if tension_grad > max_tau and self.reversible:
            self.unbind_and_reset()

    def unbind_and_reset(self) -> None:
        self.edge = EdgeFlow.ZERO
        self.anchored_target = None
        self._reset_future()
        self.record("Reversible exit declared. Returned to 0.")

    def retry(self, new_lmbda: float, tau: float) -> None:
        if self.edge != EdgeFlow.FRAGMENTED:
            self.record("Retry aborted: Node is not in a fragmented state.")
            return
            
        self.record("Attempting recursive rebinding...")
        self.threshold_test(new_lmbda, tau)

    async def await_convergence(self, timeout: float = 600.0) -> str:
        try:
            success = await asyncio.wait_for(self.future, timeout=timeout)
            trace_output = "\n".join([f"[{m.get('new_state', m.get('previous_state', '0'))}] {m['event']}" for m in self.memory])
            
            if not success:
                trace_output += "\n[⚠️ SYSTEM COLLAPSED] Execution fractured before reaching dominium. Trace aborted."
                
            return trace_output
            
        except asyncio.TimeoutError:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.origin}] Phase state resolution timed out. Topology Collapsed.")
            return "Execution failed: Timeout reached without convergence."
        except Exception as e:
            self.fracture_topology(lmbda=0.0, tau=1.0, force_collapse=True)
            log.error(f"[{self.origin}] Fatal anomaly detected in convergence wait: {e}")
            return f"Execution failed: {e}"