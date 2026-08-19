# phase.node.sphere.nonlinear
## @lineage: dphi.node.sphere.nonlinear
"""
@desc: Nonlinear Tail Risk Prediction Attractor Boot Sequence
@flow: Defines Advanced Config -> Injects into Universal Boot Engine
"""
import asyncio
from phase.node.sphere.attractor import boot_sequence
from watcher.plane.emitter import get_emitter

log = get_emitter("sphere.nonlinear", phase="BOOT")

"""NONLINEAR CONFIGURATION (Injection Payload)"""
NONLINEAR_SPHERE_CONFIG = {
    "system_type": "NONLINEAR_PREDICTION_ATTRACTOR",
    "runtime": { 
        "seed": 42, 
        "max_ticks": 2000, 
        "sleep_interval": 0.05, 
        "dt": 0.1 
    },
    "kernel": { 
        # 피츠휴-나구모 연쇄 발화 모델 주입
        "type": "kernel.fitzhugh", 
        "params": { 
            "global_coupling": 1.5, 
            "fh_epsilon": 0.05 
        } 
    },
    "field": { 
        "type": "node.network",
        "params": { 
            "size": 30, 
            "init_phase_range": [0.0, 3.14], # 좁은 위상에서 시작하여 포화 유도
            "omega_range": [0.1, 0.4] 
        } 
    },
    "watcher": { 
        # 25%의 노드가 동시 발화(Spike) 시 파열 선고
        "type": "watcher.avalanche",
        "params": { "cascade_ratio": 0.25 } 
    },
    "regime": { 
        # 데몬 개입 시 시장의 텐션을 30% 쿨다운
        "type": "regime.cooling",
        "params": { "cooling_factor": 0.3 } 
    },
    # Ators는 Base Engine이 Field Size(30)에 맞춰 자동 생성하도록 비워둠
    "ators": []
}

if __name__ == "__main__":
    log.info("Preparing Nonlinear Dynamics Simulation Sphere...")
    try:
        # Base Engine에 비선형 Config를 주입(Inject)하여 기동
        asyncio.run(boot_sequence(injected_config=NONLINEAR_SPHERE_CONFIG))
    except KeyboardInterrupt:
        log.info("Nonlinear System gracefully shutting down.")