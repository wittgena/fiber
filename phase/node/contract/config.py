# meta.phase.contract.config
## @lineage: phase.contract.config
## @lineage: phase.node.config
import math
import json
from dataclasses import dataclass, asdict
from pydantic import BaseModel, Field
from typing import List, Dict, Any

# @dataclass
class TransitionPolicy(BaseModel):
    """Meta-transition rule: event → state reconfiguration"""
    rupture_to: str = "ATTRACTOR"
    reset_tension: bool = True

# @dataclass
class KernelConfig(BaseModel):
    """Φ-dynamics parameters: global coupling / dissipation"""
    type: str = "kuramoto"
    global_coupling: float = 0.8
    dissipation_rate: float = 0.95

# @dataclass
class AtorConfig(BaseModel):
    """ψ-behavior parameters: local transduction rules"""
    base_coupling: float = 0.5
    reflector_phase_boost: float = 0.5
    attractor_gain: float = 1.5

# @dataclass
class FieldConfig(BaseModel):
    """Φ-initial condition: phase manifold distribution"""
    size: int = 20
    init_phase_range: list = Field(default_factory=lambda: [0.0, 2 * math.pi])
    omega_range: list = Field(default_factory=lambda: [0.1, 0.3])

# @dataclass
class WatcherConfig(BaseModel):
    """∂Φ-threshold: critical surface (rupture boundary)"""
    rupture_limit: float = 4.0

# @dataclass
class RuntimeConfig(BaseModel):
    """τ-control: temporal resolution and execution horizon"""
    dt: float = 0.1
    max_ticks: int = 100
    sleep_interval: float = 0.1
    seed: int = 42 

# @dataclass
class SystemConfig(BaseModel):
    """external configuration manifold (injectable structure)"""
    kernel: KernelConfig = Field(default_factory=KernelConfig)
    ator: AtorConfig = Field(default_factory=AtorConfig)
    field: FieldConfig = Field(default_factory=FieldConfig)
    watcher: WatcherConfig = Field(default_factory=WatcherConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    policy: TransitionPolicy = Field(default_factory=TransitionPolicy)

    @classmethod
    def from_json(cls, json_str: str) -> 'ToposConfig':
        """external → structured manifold"""
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ToposConfig':
        """mapping: partial injection with defaults"""
        return cls(
            kernel=KernelConfig(**data.get("kernel", {})),
            ator=AtorConfig(**data.get("ator", {})),
            field=FieldConfig(**data.get("field", {})),
            watcher=WatcherConfig(**data.get("watcher", {})),
            runtime=RuntimeConfig(**data.get("runtime", {})),
            policy=TransitionPolicy(**data.get("policy", {}))
        )

    def to_json(self) -> str:
        """observable configuration state"""
        return json.dumps(asdict(self), indent=2)

    def apply_patch(self, patch_data: Dict[str, Any]) -> 'ToposConfig':
        current_data = self.model_dump()
        for key, value in patch_data.items():
            if isinstance(value, dict) and key in current_data:
                current_data[key].update(value)
            else:
                current_data[key] = value
                
        return KernelConfig.model_validate(current_data)