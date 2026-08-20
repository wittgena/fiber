# dphi.node.contract.runtime.ator
## @lineage: phase.contract.runtime.ator
## @lineage: meta.phase.runtime
import asyncio
import json
from typing import Dict, Any, List

from arch.model.phase.flow import PhaseFlow, FlowState
from kernel.dphi.broker import DphiBroker
from watcher.plane.emitter import get_logger

log = get_logger("ator.runtime")

class AtorRuntime:
    def __init__(self, entry: str, nodes: Dict[str, Any], runtime_node: Any):
        self.entry = entry
        self.nodes = nodes
        self.engine = runtime_node
        
        self.psi_queue = asyncio.Queue()
        self._tasks: List[asyncio.Task] = []
        self._is_active = False
        self.broker = DphiBroker()

    async def _process_queue_loop(self):
        log.info("[RuntimeAtor] Ribosome active. Waiting for mRNA (Ψ) injection...")
        while self._is_active and getattr(self.engine, 'running', True):
            try:
                item = await self.psi_queue.get()
                if not isinstance(item, tuple) or len(item) != 2:
                    self.psi_queue.task_done()
                    continue

                node_name, ctx = item
                
                if node_name == "UGA":  # 종결 코돈
                    log.info(f"[RuntimeAtor] Closure (UGA) Reached. Protein fully folded.")
                    self.psi_queue.task_done()
                    continue

                # ==========================================================
                # [WASM 감응 (Resonance)] 
                # 파이썬은 더 이상 스스로 상태를 변경하지 않음. WASM에 붕괴를 요청.
                # ==========================================================
                payload = ctx.state.get("materialization_seed", {})
                if not payload:
                    payload = self._construct_transition_payload(node_name, ctx)

                log.debug(f"[RuntimeAtor] Passing intent to WASM Kernel for Collapse...")
                res_raw = await self.broker.execute("execute_transition", payload)
                res = json.loads(res_raw)

                if not res.get("is_authorized"):
                    log.error(f"[RuntimeAtor] WASM Boundary rejected transition: {res.get('error_msg')}")
                    # 감응: 거부당했을 때의 파열음(Fracture)을 처리
                    await self._handle_fracture(ctx)
                    self.psi_queue.task_done()
                    continue

                # ==========================================================
                # [WASM의 흔적(Residue) 읽기 및 위상 텐션 체감]
                # WASM이 수학적으로 계산한 잔여물(Residue)을 통해 현재 위상의 상태를 '체감'합니다.
                # ==========================================================
                final_root = res.get("final_root", {})
                residues = res.get("all_residues", [])
                
                ctx.state["phase_root"] = final_root
                
                for residue in residues:
                    kind = residue.get("kind")
                    msg = residue.get("msg")
                    
                    if kind == "TRANSITION":
                        log.info(f"  [Resonance] Topology Mutated: {msg}")
                    elif kind == "ERROR":
                        log.error(f"  [Resonance] Physics Error in WASM: {msg}")
                    elif kind == "WARN":
                        log.warning(f"  [Resonance] Tension Detected: {msg}")

                # 텐션 평가 (WASM의 evaluate_tension 호출을 통한 수학적 단편화 체크)
                tension_res_raw = await self.broker.execute("evaluate_tension", f"{node_name}|previous_state")
                tension_res = json.loads(tension_res_raw)
                
                if tension_res.get("state") == "Fragmented":
                    log.warning(f"[RuntimeAtor] 🌪️ High Topological Tension detected (Lambda: {tension_res.get('lambda')}). Triggering Auto-Alignment.")
                    # 감응: 텐션이 너무 높으면 스스로 구조를 재정렬하는 로직 트리거
                    
                # 다음 노드로 유전자 체인 전달
                next_node = self._determine_next_node(final_root, node_name)
                await self.psi_queue.put((next_node, ctx))
                self.psi_queue.task_done()

            except Exception as e:
                log.error(f"Error during RNA folding execution: {e}", exc_info=True)
                self.psi_queue.task_done()
                
    def _construct_transition_payload(self, node_name, ctx):
        # 일반적인 런타임 진행 시 WASM에 던질 규격
        return {
            "intent_action": f"execute_{node_name}",
            "intent_payload": {},
            "evolution_ctx": {
                "phase_root": ctx.state.get("phase_root", {"name": "root", "kind": "CORE", "children": {}}),
                "external_rules": []
            }
        }
        
    def _determine_next_node(self, final_root, current_node):
        # 최종 붕괴된 트리(WASM output)의 형상을 보고 다음 행선지를 결정
        return "UGA" if current_node == "projection" else "evaluator" # (예시)

    def attach(self):
        self._is_active = True
        controller_task = asyncio.create_task(self._process_queue_loop())
        self._tasks.append(controller_task)

    async def detach(self):
        self._is_active = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()