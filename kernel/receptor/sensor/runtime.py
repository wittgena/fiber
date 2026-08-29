# fiber.kernel.receptor.sensor.runtime
## @lineage: fiber.receptor.sensor.runtime
## @lineage: fiber.kernel.ator.runtime
from __future__ import annotations
import asyncio
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, model_validator, field_validator

from xphi.arch.contract.discovery import discover_modules
from xphi.arch.event.psi import PsiCarrier, PsiEvent
from xphi.arch.contract.registry.unified import contract, registry
from xphi.kernel.phase.runtime.executor.base import BaseExecutor

from xphi.kernel.space.bind.resolver import find_current_self
from xphi.kernel.phase.runtime.node import NodeRuntime
from xphi.kernel.phase.runtime.flow.cont import LoopCarrier, XeCont
from xphi.kernel.daemon.base import AbstractDaemon
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.sign import LedgerAuthAdapter
from xphi.kernel.dphi.adapter.ator import AtorAdapter, ToposSignal, ManifoldState, NodeRole, ToposActionType
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("ator.runtime")


# =========================================================================
# @phase.1: System Configurations & Topology Specs
# =========================================================================
class ComponentSpec(BaseModel):
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)

class AtorSpec(BaseModel):
    type: str = "topos.ator"
    id: str
    initial_state: str = NodeRole.NORMAL.value
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
        if not self.ators:
            size = self.field.params.get("size", 0)
            if size <= 0:
                raise ValueError("[Config Error] 'field.params.size' must be > 0 to generate topology.")
            
            hydrated = []
            for i in range(size):
                state = NodeRole.REFLECTOR.value if i % 10 == 0 else NodeRole.NORMAL.value
                hydrated.append(AtorSpec(
                    id=f"node_{i}",
                    initial_state=state,
                    params={"reflector_boost": 0.5, "attractor_gain": 1.2}
                ))
            self.ators = hydrated
        return self


# =========================================================================
# @phase.2: Default System Blueprints
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


# =========================================================================
# @phase.3: Integrated Dynamics Executor (Absorbed)
# =========================================================================
class IntegratedDynamicsExecutor(BaseExecutor):
    """
    데몬 내부에서 토폴로지 바운드를 감싸고 XeCont 캐리어로 생명주기를 전달하는 경량 실행기.
    """
    def __init__(self, bound: Any):
        super().__init__()
        self.bound = bound
        self._xe = XeCont(bound=self.bound, ex="dynamics.init", origin="system.boot")

    @property
    def phase_id(self) -> int:
        return getattr(self._xe, 'phase_id', 0)

    @property
    def states(self) -> Dict[str, Any]:
        if hasattr(self.bound, 'states'):
            return self.bound.states
        return getattr(self._xe, 'states', {})

    async def execute(self, psi: Any) -> List[Any]:
        return await self._xe.execute(psi)


# =========================================================================
# @phase.4: Dynamics Daemon (Unified Topology Control)
# =========================================================================
@contract.daemon("ator_dynamics")
class AtorDynamicsDaemon(AbstractDaemon):
    """
    위상장 동역학(Topological Dynamics) 커널을 조립/실행하고 생명주기를 통제하는 메인 데몬.
    """
    def __init__(self, ctx):
        super().__init__("AtorDynamicsDaemon")
        self.ctx = ctx
        self.broker = DphiBroker.get_instance()
        self.config: SphereConfig = ctx.get("sphere_config", DEFAULT_SPHERE_CONFIG)
        
        self.phase_queue = asyncio.Queue()
        self._tasks: List[asyncio.Task] = []
        self.node_engine: Optional[NodeRuntime] = None

    def _build_topology(self) -> Any:
        """기존 SystemBuilder 로직의 내재화. 레지스트리를 통해 컴포넌트를 엮어 Bound를 생성."""
        cfg_dict = self.config.model_dump()
        
        kernel = registry.create_component(cfg_dict.get("kernel", {}))
        field = registry.create_component(cfg_dict.get("field", {}))
        watcher = registry.create_component(cfg_dict.get("watcher", {}))
        regime = registry.create_component(cfg_dict.get("regime", {}))
        
        ators = []
        for ator_cfg in cfg_dict.get("ators", []):
            ator = registry.create_component(
                ator_cfg, 
                ator_id=ator_cfg.get("id"),
                state=ator_cfg.get("initial_state", "NORMAL")
            )
            ators.append(ator)

        # 조립된 컴포넌트들을 Field(네트워크 토폴로지)에 바인딩
        field.bind_kernel(kernel)
        field.bind_ators(ators)
        field.bind_watcher(watcher)
        field.bind_regime(regime)

        return field

    async def _invoke_ffi(self, endpoint: str, payload_dict: Dict[str, Any]) -> Dict[str, Any]:
        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        signed_payload = {
            "payload": payload_dict,
            "signature": LedgerAuthAdapter.get_instance().sign_payload(canonical_bytes),
            "pubkey": LedgerAuthAdapter.get_signer_pubkey()
        }
        res_raw = await self.broker.execute(endpoint, json.dumps(signed_payload))
        return json.loads(res_raw.output)

    async def _orchestrate_phase_flow(self):
        log.info(f"[{self.name}] Orchestrator active. Awaiting topological stimuli (Ψ)...")
        while self.running:
            try:
                item = await self.phase_queue.get()
                if not isinstance(item, tuple) or len(item) != 2:
                    self.phase_queue.task_done()
                    continue

                action_type, ctx_data = item
                match action_type:
                    case "SATURATED":
                        log.info(f"[{self.name}] Phase Saturation Reached. Topology stable.")
                    case "EVOLVE_STATE":
                        evo_ctx = StateAdapter.build_evolution_context(
                            ctx_data.get("phase_root", {}), 
                            ctx_data.get("external_rules", [])
                        )
                        
                        res = await self._invoke_ffi("process_evolution", evo_ctx)
                        final_root = res.get("final_root", {})
                        
                        ctx_data["phase_root"] = final_root
                        self._apply_residues(res.get("all_residues", []))
                        
                        next_node = "SATURATED" if final_root.get("name") == "stable_root" else "EVOLVE_STATE"
                        await self.phase_queue.put((next_node, ctx_data))

                self.phase_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[{self.name}] Topological execution fractured: {e}", exc_info=True)
                self.phase_queue.task_done()

    def _apply_residues(self, residues: List[Dict[str, Any]]):
        for residue in residues:
            msg = residue.get("msg")
            match residue.get("kind"):
                case "TRANSITION":
                    log.info(f"  [Resonance] Topology Mutated (SYMLINK -> CORE): {msg}")
                case "ERROR":
                    log.error(f"  [Resonance] Physics Error in WASM: {msg}")
                case "WARN":
                    log.warning(f"  [Resonance] Boundary Tension: {msg}")

    async def _inject_boot_pulse(self):
        await asyncio.sleep(2.0)
        sys_type = self.config.system_type
        log.info(f">>> Injecting {sys_type} Boot Pulse... <<<")
        
        seed_event = PsiEvent(
            event_id=f"boot-tick-{sys_type.lower()}", 
            parent_id=None, 
            source_id="system.boot",
            scope="GLOBAL", 
            tick=1, 
            phase_id=0,
            carrier=PsiCarrier(kind="TICK", tag="SEED", payload={}), 
            context={"phase": "loop", "domain": "watcher"}
        )
        if self.node_engine and hasattr(self.node_engine, 'bus'):
            await self.node_engine.bus.publish(seed_event)

    async def run(self):
        log.info(f"[{self.name}] Autonomous Dynamics Daemon Started. (Node ID: {self.ctx.node_id})")
        log.info(f"[{self.name}] Discovering and mounting topological components...")
        
        discover_modules(find_current_self())
        
        log.info(f"[{self.name}] Assembling Integrated Dynamics Topology...")
        
        # 1. 내부에서 직접 토폴로지 빌드
        bound = self._build_topology()
        
        # 2. 내재화된 Executor에 바운딩
        watcher_xe = IntegratedDynamicsExecutor(bound)
        
        # 3. 런타임 캐리어 및 노드 엔진 준비
        loop_xe = LoopCarrier(
            xe=watcher_xe, 
            max_ticks=self.config.runtime.max_ticks, 
            interval=self.config.runtime.sleep_interval
        )
        self.node_engine = NodeRuntime(executor=loop_xe)

        orchestrator_task = asyncio.create_task(self._orchestrate_phase_flow())
        boot_task = asyncio.create_task(self._inject_boot_pulse())
        node_task = asyncio.create_task(self.node_engine.start())
        
        self._tasks.extend([orchestrator_task, boot_task, node_task])
        
        log.info(f"[{self.name}] System '{self.config.system_type}' active and awaiting pulses.")

        try:
            while self.running:
                await asyncio.sleep(1.0)
                if node_task.done():
                    log.error(f"[{self.name}] NodeRuntime exited unexpectedly.")
                    break
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancellation signal received from Node Supervisor.")
        finally:
            log.info(f"[{self.name}] Detaching and cleaning up internal tasks...")
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()
            log.info(f"[{self.name}] Daemon evaporated cleanly.")