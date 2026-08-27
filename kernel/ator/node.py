# fiber.kernel.ator.node
from __future__ import annotations
import ast
import json
import math
import random
from typing import Optional, List, Dict, Any

from xphi.arch.contract.registry.unified import contract 
from xphi.arch.contract.event.bus import AsyncEventBus
from xphi.arch.contract.event.psi import PsiCarrier, PsiEvent
from xphi.arch.contract.interface import IPhaseField, ICriticalDetector, ISystemRegime, IPhaseAtor, IDynamicsKernel
from xphi.arch.model.phase.flow import PhaseFlow, Transduction

from xphi.kernel.dphi.adapter.state import StateAdapter
from xphi.kernel.dphi.adapter.ator import AtorAdapter, NodeState, KernelDelta, NodeRole
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.dphi.method import DphiMethod
from fiber.kernel.ator.config import KernelConfig
from xphi.watcher.plane.emitter import get_emitter

log_node = get_emitter("contract.node")
log_sensor = get_emitter("contract.sensor")

# =========================================================================
# 1. NODE DYNAMICS & FIELD MANIFOLDS
# =========================================================================

@contract.ator("node.network", role="field")
class NodeNetwork(IPhaseField):
    """Φ: Network-based phase field (Unified Field Container)"""
    def __init__(self, **kwargs):
        self.size = kwargs.get("size", 10)
        self.init_phase_range = kwargs.get("init_phase_range", [0.0, 1.0])
        self.omega_range = kwargs.get("omega_range", [0.8, 1.2])

        self.kernel = None
        self.watcher = None
        self.regime = None
        self.ators = []

        self._states: Dict[str, NodeState] = {}
        self.pressure: float = 0.0
        self.topology: int = 1

        if kwargs.get("standalone", False):
            self._initialize_standalone()

    def _initialize_standalone(self):
        for i in range(self.size):
            self._states[str(i)] = AtorAdapter.build_node_state(
                phase=random.uniform(*self.init_phase_range) * math.pi * 2,
                omega=random.uniform(*self.omega_range),
                state=NodeRole.NORMAL.value,  # [REFACTOR] Enum 사용
                tension=0.0
            )

    def bind_kernel(self, kernel): self.kernel = kernel
    def bind_watcher(self, watcher): self.watcher = watcher
    def bind_regime(self, regime): self.regime = regime
    
    def bind_ators(self, ators):
        self.ators = ators
        for a in ators:
            phase_val = random.uniform(*self.init_phase_range) * math.pi * 2
            omega_val = random.uniform(*self.omega_range)
            state_val = getattr(a, "initial_state", NodeRole.NORMAL.value) # [REFACTOR] Enum 사용
            
            self._states[a.ator_id] = AtorAdapter.build_node_state(
                phase=phase_val, omega=omega_val, state=state_val, tension=0.0
            )

    def update_node_state(self, node_id: str, new_state: str) -> None:
        if node_id in self._states: 
            self._states[node_id].state = new_state

    def set_tension(self, node_id: str, tension: float) -> None:
        if node_id in self._states: 
            self._states[node_id].tension = tension

    def get_state(self) -> Dict[str, NodeState]:
        return self._states

    def compute_gradient(self) -> Dict[str, float]:
        return {node_id: data.tension for node_id, data in self._states.items()}

    async def evolve(self, dt: float, broker: DphiBroker) -> None:
        """[REFACTOR] FFI 호출을 위한 비동기 처리 및 broker 주입"""
        if not self.kernel: return
        
        # WASM에 O(N^2) 수학 연산 일괄 위임
        deltas: Dict[str, KernelDelta] = await self.kernel.compute_step(self._states, dt, broker)
        
        total_tension = 0.0
        for node_id, delta in deltas.items():
            state = self._states[node_id]
            state.phase = AtorAdapter.Dynamics.evolve_phase(state.phase, delta.d_phase)
            state.tension = AtorAdapter.Dynamics.evolve_tension(state.tension, delta.target_tension, dt, accumulate=True)
            
            if delta.velocity is not None:
                state.velocity = delta.velocity
            if delta.recovery is not None:
                state.recovery = delta.recovery
            if delta.is_spiking is not None:
                state.is_spiking = delta.is_spiking

            total_tension += state.tension
            
        self.pressure = total_tension / max(1, len(self._states))
        if hasattr(self.kernel, 'render_state'):
            visual = self.kernel.render_state(self._states)
            print(f"\r[Phase Field] {visual} | Pressure: {self.pressure:.2f}/17.0 ", end="", flush=True)

    async def absorb(self, batch_payload: List[Dict[str, Any]], broker: DphiBroker):
        await self.evolve(dt=0.1, broker=broker)

    def evaluate(self) -> str:
        if self.watcher:
            trigger = self.watcher.evaluate(self, history=[], current_tick=0)
            if trigger and getattr(trigger.carrier, 'kind', '') == "RUPTURE":
                return "DEPOSIT"
        return "SATURATE"

    def commit(self):
        if self.regime:
            self.regime.modify_field(self)
        self.topology += 1


@contract.ator("topos.ator", role="ator")
class ToposAtor(IPhaseAtor):
    def __init__(self, ator_id: str, reflector_boost: float = 0.5, attractor_gain: float = 1.2, state: str = NodeRole.NORMAL.value):
        self._id = ator_id
        self._state = state
        self.reflector_boost = reflector_boost
        self.attractor_gain = attractor_gain
        self.log = get_emitter(name=f"node.{ator_id}", phase="STABLE")

    @property
    def ator_id(self) -> str: return self._id
    @property
    def state(self) -> str: return self._state
    def set_state(self, new_state: str) -> None: self._state = new_state

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        my_data: NodeState = field.get_state()[self._id]
        
        # [REFACTOR] 하드코딩 문자열을 Enum 값으로 교체
        if self._state == NodeRole.REFLECTOR.value:
            my_data.phase = AtorAdapter.Dynamics.apply_reflector(my_data.phase, self.reflector_boost)
            my_data.tension = 0.0 
            
            inject_carrier = PsiCarrier(kind="INJECT", tag="NETWORK", payload={"tension": 1.0})
            inject_event = PsiEvent(
                event_id=f"inject-{self._id}-{event.tick}", parent_id=event.event_id, 
                source_id=self._id, scope="NETWORK", tick=event.tick,
                carrier=inject_carrier, context={"phase": "loop", "domain": "watcher"}
            )
            await bus.publish(inject_event)
            
        elif self._state == NodeRole.ATTRACTOR.value:
            my_data.omega = AtorAdapter.Dynamics.apply_attractor(my_data.omega, self.attractor_gain)


@contract.ator("node.regime", role="regime")
class NodeRegime(ISystemRegime):
    def __init__(self, **kwargs):
        self.params = kwargs

    def modify_field(self, field: IPhaseField) -> None:
        states: Dict[str, NodeState] = field.get_state()
        for node_id, data in states.items():
            data.tension = 0.0
            # [REFACTOR] 하드코딩 문자열을 Enum 값으로 교체
            if data.state == NodeRole.NORMAL.value:
                data.phase = random.uniform(0, 2 * math.pi)
            elif data.state == NodeRole.REFLECTOR.value:
                data.phase = 0.0  

        if hasattr(field, 'pressure'):
            field.pressure = 0.0
            
        log_node.info("[Regime] Field collapsed and reformed. Tension reset to 0.0")

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        return event if event.context.get("epoch") == "new" else event


@contract.ator("ator.reflector")
class AtorReflector(Transduction):
    def transduce(self, flow: PhaseFlow, ator_node: Any) -> PhaseFlow:
        raw = flow.payload.get("raw_input", {})
        file_path = raw.get("source_path")
        task_data = raw.get("task")

        if not file_path:
            raise KeyError("Inversion Point (source_path) missing in raw_input")

        log_node.info(f"  [Reflect] Extracting Topological Blueprint from source: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        topology = self._extract_phi(tree)

        children = {
            k: StateAdapter.build_symlink_node(name=k, ref_target=v.get("type", "unknown"))
            for k, v in topology.items()
        }
        phase_root = StateAdapter.build_core_node(name="ator_bootstrap_root", content="init", children=children)
        evolution_ctx = StateAdapter.build_evolution_context(phase_root=phase_root)
        
        materialization_seed = {
            "evolution_ctx": evolution_ctx,
            "topology": topology,       
            "task": task_data,
            "meta_context": flow.payload 
        }
        return self._close(materialization_seed, flow, ator_node)

    def _extract_phi(self, tree: ast.AST) -> Dict[str, Any]:
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in ("PHI", "XPHI"):
                        return ast.literal_eval(node.value)
        raise ValueError("Topological Blueprint (PHI or XPHI) not found in source")


# =========================================================================
# 2. SENSOR KERNELS (Φ-Dynamics Substrate) -> ALL OFFLOADED TO WASM
# =========================================================================

class BaseWasmKernel(IDynamicsKernel):
    """모든 물리/수학 커널이 공유하는 FFI 통신 파이프라인. 커널별 파라미터만 분리하여 WASM에 전달합니다."""
    def __init__(self, kernel_type: str, config: KernelConfig, extra_params: Dict[str, Any] = None):
        self.kernel_type = kernel_type
        self.config = config
        self.params = self.config.model_dump(exclude={"type"})
        if extra_params:
            self.params.update(extra_params)

    async def compute_step(self, states: Dict[str, NodeState], dt: float, broker: DphiBroker) -> Dict[str, KernelDelta]:
        payload = AtorAdapter.Dynamics.build_dynamics_ffi_payload(
            states=states, kernel_type=self.kernel_type, params=self.params, dt=dt
        )
        # 고비용 O(N^2) 연산이므로 Tier.SYSTEM 할당 보장
        res = await broker.invoke(DphiMethod.PROCESS_FIELD_DYNAMICS, json.dumps(payload), tier="SYSTEM")
        
        if res.success:
            return AtorAdapter.Dynamics.parse_dynamics_result(json.loads(res.output))
        else:
            log_sensor.error(f"[WASM Kernel Error] Execution failed: {res.error}")
            return {}

    def render_state(self, states: Dict[str, NodeState]) -> str:
        hypotheses = ['🟦', '🟩', '🟨', '🟥']
        visual = [hypotheses[int((s.phase / (2 * math.pi)) * 4) % 4] for s in states.values()]
        avg_tension = sum(s.tension for s in states.values()) / max(1, len(states))
        return f"Dissonance: {avg_tension:.2f} | {''.join(visual)}"


@contract.ator("sensor.ator", role="kernel")
class SensorAtor(BaseWasmKernel):
    def __init__(self, **kwargs):
        config = kwargs.get("config") or KernelConfig(**kwargs)
        extra = {
            "trust_radius": kwargs.get("trust_radius", 1.0),
            "repulsion_factor": kwargs.get("repulsion_factor", 0.2)
        }
        super().__init__("kernel.resonance", config, extra)

@contract.ator("sensor.kuramoto", role="kernel")
class SensorKuramoto(BaseWasmKernel):
    def __init__(self, **kwargs):
        config = kwargs.get("config") or KernelConfig(**kwargs)
        super().__init__("kernel.kuramoto", config)

    def render_state(self, states: Dict[str, NodeState]) -> str:
        chars = ['🌑', '🌘', '🌗', '🌖', '🌕', '🌔', '🌓', '🌒']
        visual = [chars[int((s.phase / (2 * math.pi)) * 8) % 8] for s in states.values()]
        avg_tension = sum(s.tension for s in states.values()) / max(1, len(states))
        return f"Tension: {avg_tension:.2f} | {''.join(visual)}"

@contract.ator("sensor.kuramoto_inertia", role="kernel")
class SensorKuramotoInertia(BaseWasmKernel):
    def __init__(self, **kwargs):
        config = kwargs.get("config") or KernelConfig(**kwargs)
        super().__init__("kernel.kuramoto_inertia", config)

@contract.ator("sensor.fitzhugh", role="kernel")
class SensorFitzHughNagumo(BaseWasmKernel):
    def __init__(self, **kwargs):
        config = kwargs.get("config") or KernelConfig(**kwargs)
        extra = {"a": 0.7, "b": 0.8} # 하드코딩 파라미터 유지
        super().__init__("kernel.fitzhugh", config, extra)

@contract.ator("sensor.sakaguchi", role="kernel")
class SensorSakaguchi(BaseWasmKernel):
    def __init__(self, **kwargs):
        config = kwargs.get("config") or KernelConfig(**kwargs)
        super().__init__("kernel.sakaguchi", config)


# =========================================================================
# 3. WATCHERS & REGIMES 
# =========================================================================

@contract.ator("sensor.watcher.kinetic", role="watcher")
class SensorKineticWatcher(ICriticalDetector):
    def __init__(self, accel_threshold: float = 8.0):
        self.accel_threshold = accel_threshold

    def evaluate(self, field: IPhaseField, history: List[Any], current_tick: int) -> Optional[PsiEvent]:
        states: Dict[str, NodeState] = field.get_state()
        total_momentum = sum(data.tension for data in states.values()) / max(1, len(states))
        
        if total_momentum >= self.accel_threshold:
            log_sensor.warning(f"[KineticWatcher] Systemic Momentum Surge: {total_momentum:.2f}")
            carrier = PsiCarrier(kind="TAIL_RISK", tag="3_SIGMA_OVERSHOOT", payload={"momentum": total_momentum})
            return PsiEvent(
                event_id=f"kinetic-{current_tick}", parent_id=None, source_id="sensor.watcher.kinetic",
                scope="SYSTEMIC", tick=current_tick, carrier=carrier, context={"risk_level": "PRE_HEATING"}
            )
        return None

@contract.ator("sensor.regime.cooling", role="regime")
class SensorCoolingRegime(ISystemRegime):
    def __init__(self, **kwargs):
        self.cooling_factor = kwargs.get("cooling_factor", 0.5)

    def modify_field(self, field: IPhaseField) -> None:
        states: Dict[str, NodeState] = field.get_state()
        for node_id, data in states.items():
            data.tension *= self.cooling_factor
            if data.velocity is not None:
                data.velocity *= self.cooling_factor
            if data.recovery is not None:
                data.recovery = 0.0
                data.is_spiking = False
                
        log_sensor.info(f"[CoolingRegime] Field tension and momentum reduced by {self.cooling_factor * 100}%.")

    def constrain_ator(self, ator: IPhaseAtor) -> None: 
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        return event if event.carrier.kind != "TAIL_RISK" else None