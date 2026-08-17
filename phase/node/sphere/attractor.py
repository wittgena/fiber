# phase.node.sphere.attractor
## @lineage: dphi.node.sphere.attractor
"""
@desc: Native Dual Resonance Attractor & Universal Boot Engine
@flow: Config Validation -> Topology Assembly -> Pulse Injection -> Autonomous Simulation
"""
import asyncio
from typing import Dict, Any, Optional

from arch.contract.discovery import discover_modules
from arch.contract.event.psi import PsiCarrier, PsiEvent
from kernel.bind.resolver import find_current_self

from kernel.phase.runtime.executor.dynamics import DynamicsExecutor
from kernel.phase.runtime.node import NodeRuntime
from kernel.phase.runtime.flow.cont import LoopCarrier
from watcher.plane.emitter import get_emitter

log = get_emitter("sphere.attractor", phase="BOOT")

# 기본 듀얼 레조넌스 설정 (Kuramoto x Ator)
DEFAULT_SPHERE_CONFIG: Dict[str, Any] = {
    "system_type": "DUAL_RESONANCE_ATTRACTOR",
    "runtime": { "seed": 99, "max_ticks": 1000, "sleep_interval": 0.05, "dt": 0.1 },
    "kernel": { 
        "type": "kernel.resonance", 
        "params": { 
            "alpha": 0.4,
            "kuramoto_params": { "global_coupling": 1.2 },
            "ator_params": { "trust_radius": 1.0, "repulsion_factor": 0.2 }
        } 
    },
    "field": { 
        "type": "node.network",
        "params": { "size": 30, "init_phase_range": [0.0, 6.28], "omega_range": [0.2, 0.5] } 
    },
    "watcher": { 
        "type": "kernel.singularity",
        "params": { "candidate_limit": 10.0, "rupture_limit": 30.0 } 
    },
    "regime": { 
        "type": "node.regime",
        "params": {} 
    },
    "ators": []
}

def validate_sphere_config(config: Dict[str, Any]) -> None:
    """@desc: 런타임 조립 전 필수 토폴로지 구성요소가 선언되어 있는지 검증"""
    required_components = ["runtime", "kernel", "field", "watcher", "regime"]
    for comp in required_components:
        if comp not in config:
            raise ValueError(f"[Config Error] Missing required topological component: '{comp}'")
        
        if comp != "runtime" and "type" not in config[comp]:
            raise ValueError(f"[Config Error] Component '{comp}' must specify a 'type' identifier.")
            
    if "size" not in config["field"].get("params", {}):
        raise ValueError("[Config Error] 'field.params.size' is required to generate the network topology.")
        
    if config["runtime"].get("dt", 0) <= 0:
        raise ValueError("[Config Error] 'runtime.dt' must be greater than 0.")
        
    log.info("[Validator] Configuration topology successfully verified.")


async def boot_sequence(injected_config: Optional[Dict[str, Any]] = None):
    """
    @desc: Universal System Bootstrap Sequence
    @injected_config: 외부에서 주입 가능한 동적 설정. 없으면 DEFAULT_SPHERE_CONFIG 사용.
    """
    log.info("[Boot] Discovering and mounting topological components...")
    discover_modules(find_current_self())
    
    active_config = injected_config if injected_config is not None else DEFAULT_SPHERE_CONFIG
    validate_sphere_config(active_config)
    
    # 설정에 Ator 맵이 없다면 필드 사이즈에 맞춰 동적 생성
    if not active_config.get("ators"):
        field_size = active_config["field"]["params"]["size"]
        active_config["ators"] = [
            {
                "type": "topos.ator", 
                "id": f"node_{i}", 
                "initial_state": "REFLECTOR" if i % 10 == 0 else "NORMAL", # 10% 극단주의자
                "params": {"reflector_boost": 0.5, "attractor_gain": 1.2}
            }
            for i in range(field_size)
        ]

    log.info("[Boot] Assembling Native Dynamics Executor...")
    watcher_xe = DynamicsExecutor(config_dict=active_config)
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=active_config["runtime"]["max_ticks"], 
        interval=active_config["runtime"]["sleep_interval"]
    )
    
    node = NodeRuntime(executor=loop_xe)
    
    async def boot_clock():
        await asyncio.sleep(2.0)
        sys_type = active_config.get('system_type', 'System')
        log.info(f">>> Injecting {sys_type} Boot Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id=f"boot-tick-{sys_type.lower()}", parent_id=None, source_id="system.boot",
            scope="GLOBAL", tick=1, carrier=seed_carrier, phase_id=0,
            context={"phase": "loop", "domain": "watcher"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    
    sys_type = active_config.get("system_type", "Autonomous")
    log.info(f"Watcher Node launching {sys_type} System...")
    await node.start()

if __name__ == "__main__":
    try:
        # 단독 실행 시 기본 설정(Dual Resonance)으로 구동
        asyncio.run(boot_sequence())
    except KeyboardInterrupt:
        log.info("System gracefully shutting down.")