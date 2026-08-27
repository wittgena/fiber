# fiber.kernel.ator.runtime
from __future__ import annotations
import asyncio
import argparse
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, model_validator, field_validator

from xphi.arch.contract.discovery import discover_modules
from xphi.arch.contract.event.psi import PsiCarrier, PsiEvent
from xphi.kernel.space.bind.resolver import find_current_self

from xphi.kernel.phase.runtime.executor.dynamics import DynamicsExecutor
from xphi.kernel.phase.runtime.node import NodeRuntime
from xphi.kernel.phase.runtime.flow.cont import LoopCarrier

from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import LedgerAuthAdapter
from xphi.kernel.dphi.adapter.ator import AtorAdapter, ToposSignal, ManifoldState, NodeRole, ToposActionType
from xphi.watcher.plane.emitter import get_emitter

log_rt = get_emitter("ator.runtime")
log_boot = get_emitter("sphere", phase="BOOT")


# =========================================================================
# 1. SPHERE BLUEPRINT SCHEMAS (Strict Typing Applied)
# =========================================================================

class ComponentSpec(BaseModel):
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)

class AtorSpec(BaseModel):
    type: str = "topos.ator"
    id: str
    initial_state: str = NodeRole.NORMAL.value  # [REFACTOR] Enum 기반 기본값 강제
    params: Dict[str, Any] = Field(default_factory=dict)

class RuntimeSpec(BaseModel):
    seed: int = 42
    max_ticks: int = 1000
    sleep_interval: float = 0.05
    dt: float = 0.1

    @field_validator('dt')
    @classmethod
    def check_dt(cls, v):
        if v <= 0:
            raise ValueError("[Config Error] 'runtime.dt' must be greater than 0.")
        return v

class SphereConfig(BaseModel):
    system_type: str
    runtime: RuntimeSpec
    kernel: ComponentSpec
    field: ComponentSpec
    watcher: ComponentSpec
    regime: ComponentSpec
    ators: List[AtorSpec] = Field(default_factory=list)

    @model_validator(mode='after')
    def auto_hydrate_ators(self) -> 'SphereConfig':
        """Ator 목록이 비어있으면 Field Size에 맞춰 자동 생성"""
        if not self.ators:
            size = self.field.params.get("size", 0)
            if size <= 0:
                raise ValueError("[Config Error] 'field.params.size' must be > 0 to generate topology.")
            
            hydrated = []
            for i in range(size):
                # [REFACTOR] 하드코딩된 문자열 대신 Enum 사용
                state = NodeRole.REFLECTOR.value if i % 10 == 0 else NodeRole.NORMAL.value
                hydrated.append(AtorSpec(
                    id=f"node_{i}",
                    initial_state=state,
                    params={"reflector_boost": 0.5, "attractor_gain": 1.2}
                ))
            self.ators = hydrated
        return self


# =========================================================================
# 2. PREDEFINED SYSTEM PROFILES
# =========================================================================

DEFAULT_SPHERE_CONFIG = SphereConfig(
    system_type="DUAL_RESONANCE_ATTRACTOR",
    runtime=RuntimeSpec(seed=99, max_ticks=1000, sleep_interval=0.05, dt=0.1),
    kernel=ComponentSpec(type="kernel.resonance", params={
        "alpha": 0.4,
        "kuramoto_params": {"global_coupling": 1.2},
        "ator_params": {"trust_radius": 1.0, "repulsion_factor": 0.2}
    }),
    field=ComponentSpec(type="node.network", params={"size": 30, "init_phase_range": [0.0, 6.28], "omega_range": [0.2, 0.5]}),
    watcher=ComponentSpec(type="kernel.singularity", params={"candidate_limit": 10.0, "rupture_limit": 30.0}),
    regime=ComponentSpec(type="node.regime")
)

NONLINEAR_SPHERE_CONFIG = SphereConfig(
    system_type="NONLINEAR_PREDICTION_ATTRACTOR",
    runtime=RuntimeSpec(seed=42, max_ticks=2000, sleep_interval=0.05, dt=0.1),
    kernel=ComponentSpec(type="kernel.fitzhugh", params={"global_coupling": 1.5, "fh_epsilon": 0.05}),
    field=ComponentSpec(type="node.network", params={"size": 30, "init_phase_range": [0.0, 3.14], "omega_range": [0.1, 0.4]}),
    watcher=ComponentSpec(type="watcher.avalanche", params={"cascade_ratio": 0.25}),
    regime=ComponentSpec(type="regime.cooling", params={"cooling_factor": 0.3})
)


# =========================================================================
# 3. RUNTIME ORCHESTRATION & BROKER TRANSDUCTION
# =========================================================================

class AtorRuntime:
    """
    @spec: Topological Orchestrator bridging Discrete States (state.rs) and WASM Broker.
    @role: Interprets ToposActions, manages state evolution cycles, and executes FFI securely.
    """
    def __init__(self, entry: str, nodes: Dict[str, Any], runtime_node: Any):
        self.entry = entry
        self.nodes = nodes
        self.engine = runtime_node
        
        self.phase_queue = asyncio.Queue()
        self._tasks: List[asyncio.Task] = []
        self._is_active = False
        self.broker = DphiBroker()

    async def _invoke_ffi(self, endpoint: str, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Handles Canonical Serialization, Signing, and Broker Execution compactly."""
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        signed_payload = {
            "payload": payload_dict,
            "signature": LedgerAuthAdapter.get_instance().sign_payload(canonical_bytes),
            "pubkey": LedgerAuthAdapter.get_signer_pubkey()
        }
        res_raw = await self.broker.execute(endpoint, json.dumps(signed_payload))
        return json.loads(res_raw.output)

    async def _orchestrate_phase_flow(self):
        log_rt.info("[RuntimeAtor] Orchestrator active. Awaiting topological stimuli (Ψ)...")
        while self._is_active and getattr(self.engine, 'running', True):
            try:
                item = await self.phase_queue.get()
                if not isinstance(item, tuple) or len(item) != 2:
                    self.phase_queue.task_done()
                    continue

                action_type, ctx = item
                
                match action_type:
                    case "SATURATED":
                        log_rt.info("[RuntimeAtor] Phase Saturation Reached. Topology stable.")

                    case "EVOLVE_STATE":
                        evo_ctx = StateAdapter.build_evolution_context(
                            ctx.get("phase_root", {}), 
                            ctx.get("external_rules", [])
                        )
                        
                        res = await self._invoke_ffi("process_evolution", evo_ctx)
                        final_root = res.get("final_root", {})
                        
                        ctx["phase_root"] = final_root
                        self._apply_residues(res.get("all_residues", []))
                        
                        next_node = self._determine_next_phase(final_root)
                        await self.phase_queue.put((next_node, ctx))

                self.phase_queue.task_done()

            except Exception as e:
                log_rt.error(f"[RuntimeAtor] Topological execution fractured: {e}", exc_info=True)
                self.phase_queue.task_done()

    async def _actuate_topos_action(self, raw_action: Dict[str, Any], ctx: Dict[str, Any]):
        action = AtorAdapter.parse_topos_action(raw_action)
        
        # [REFACTOR] 문자열을 Enum 객체로 변환하여 Type-Safe하게 match-case 처리
        action_enum = ToposActionType(action.get("action"))
        
        match action_enum:
            case ToposActionType.EMIT_COLLAPSE:
                log_rt.warning("  [Actuator] 💥 Collapse threshold breached! Triggering State Inversion.")
                await self.phase_queue.put(("EVOLVE_STATE", ctx))
            case ToposActionType.EMIT_PULSE:
                log_rt.info(f"  [Actuator] 💓 Systemic Pulse Emitted: {action.get('pulse_id')}")
            case ToposActionType.EMIT_PROJECTION:
                log_rt.debug(f"  [Actuator] 🌌 Projection Vector Formed: {action.get('vector_id')} from {action.get('parent_id')}")
            case ToposActionType.EMIT_INVERSION:
                log_rt.info(f"  [Actuator] 🌀 Reentry Inversion Triggered: Count {action.get('count')} for {action.get('parent_id')}")

    def _apply_residues(self, residues: List[Dict[str, Any]]):
        for residue in residues:
            msg = residue.get("msg")
            match residue.get("kind"):
                case "TRANSITION":
                    log_rt.info(f"  [Resonance] Topology Mutated (SYMLINK -> CORE): {msg}")
                case "ERROR":
                    log_rt.error(f"  [Resonance] Physics Error in WASM: {msg}")
                case "WARN":
                    log_rt.warning(f"  [Resonance] Boundary Tension: {msg}")
                
    def _determine_next_phase(self, final_root: Dict[str, Any]) -> str:
        return "SATURATED" if final_root.get("name") == "stable_root" else "EVOLVE_STATE"

    def attach(self):
        self._is_active = True
        controller_task = asyncio.create_task(self._orchestrate_phase_flow())
        self._tasks.append(controller_task)

    async def detach(self):
        self._is_active = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


# =========================================================================
# 4. BOOTSTRAP ORCHESTRATION (Sphere Loader)
# =========================================================================

async def boot_sequence(injected_config: Optional[SphereConfig] = None):
    """
    @desc: Universal System Bootstrap Sequence
    @injected_config: 동적으로 검증 완료된 SphereConfig 객체 주입
    """
    log_boot.info("[Boot] Discovering and mounting topological components...")
    discover_modules(find_current_self())
    
    active_config: SphereConfig = injected_config if injected_config is not None else DEFAULT_SPHERE_CONFIG
    config_dict = active_config.model_dump() 
    
    log_boot.info("[Boot] Assembling Native Dynamics Executor...")
    watcher_xe = DynamicsExecutor(config_dict=config_dict)
    
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=active_config.runtime.max_ticks, 
        interval=active_config.runtime.sleep_interval
    )
    
    node = NodeRuntime(executor=loop_xe)
    
    async def boot_clock():
        await asyncio.sleep(2.0)
        sys_type = active_config.system_type
        log_boot.info(f">>> Injecting {sys_type} Boot Pulse... <<<")
        
        seed_event = PsiEvent(
            event_id=f"boot-tick-{sys_type.lower()}", parent_id=None, source_id="system.boot",
            scope="GLOBAL", tick=1, phase_id=0,
            carrier=PsiCarrier(kind="TICK", tag="SEED", payload={}), 
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    
    log_boot.info(f"Watcher Node launching {active_config.system_type} System...")
    await node.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ator Dynamics Runtime & Sphere Boot Sequence")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["default", "nonlinear"], 
        default="default", 
        help="Select the sphere configuration mode"
    )
    args = parser.parse_args()

    if args.mode == "nonlinear":
        log_boot.info("Preparing Nonlinear Dynamics Simulation Sphere...")
        target_config = NONLINEAR_SPHERE_CONFIG
    else:
        log_boot.info("Preparing Default Dual Resonance Sphere...")
        target_config = DEFAULT_SPHERE_CONFIG

    try:
        asyncio.run(boot_sequence(injected_config=target_config))
    except KeyboardInterrupt:
        log_boot.info(f"{args.mode.capitalize()} System gracefully shutting down.")