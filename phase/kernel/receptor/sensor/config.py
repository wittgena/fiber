# fiber.phase.kernel.receptor.sensor.config
## @lineage: fiber.kernel.receptor.sensor.config
## @lineage: fiber.receptor.sensor.config
## @lineage: fiber.kernel.ator.config
import math
import json
from typing import Dict, Any, List
from pydantic import BaseModel, Field

class TransitionPolicy(BaseModel):
    rupture_to: str = "ATTRACTOR"
    reset_tension: bool = True

class SensorConfig(BaseModel):
    type: str = "kuramoto"
    global_coupling: float = 0.8
    dissipation_rate: float = 0.95
    inertia_mass: float = 1.0         
    inertia_friction: float = 0.1     
    fh_epsilon: float = 0.08          
    frustration_alpha: float = 0.2    

class AtorConfig(BaseModel):
    base_coupling: float = 0.5
    reflector_phase_boost: float = 0.5
    attractor_gain: float = 1.5

class FieldConfig(BaseModel):
    size: int = 20
    init_phase_range: list = Field(default_factory=lambda: [0.0, 2 * math.pi])
    omega_range: list = Field(default_factory=lambda: [0.1, 0.3])

class WatcherConfig(BaseModel):
    rupture_limit: float = 4.0

class RuntimeConfig(BaseModel):
    dt: float = 0.1
    max_ticks: int = 100
    sleep_interval: float = 0.1
    seed: int = 42 

class SystemConfig(BaseModel):
    kernel: SensorConfig = Field(default_factory=SensorConfig)
    ator: AtorConfig = Field(default_factory=AtorConfig)
    field: FieldConfig = Field(default_factory=FieldConfig)
    watcher: WatcherConfig = Field(default_factory=WatcherConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    policy: TransitionPolicy = Field(default_factory=TransitionPolicy)

    @classmethod
    def from_json(cls, json_str: str) -> 'SystemConfig':
        return cls.from_dict(json.loads(json_str))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SystemConfig':
        return cls(
            kernel=SensorConfig(**data.get("kernel", {})),
            ator=AtorConfig(**data.get("ator", {})),
            field=FieldConfig(**data.get("field", {})),
            watcher=WatcherConfig(**data.get("watcher", {})),
            runtime=RuntimeConfig(**data.get("runtime", {})),
            policy=TransitionPolicy(**data.get("policy", {}))
        )