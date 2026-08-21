# bound.observer.tension
## @lineage: dphi.observer.tension
import os
import math
import hashlib
from collections import deque
from enum import Enum
from dataclasses import dataclass

class TensionPhase(Enum):
    NORMAL = 1
    PRE_HEATING = 2
    RUPTURE = 3

@dataclass
class RiskPolicy:
    base_friction_bps: float = 15.0       
    max_allocation_pct: float = 0.20      
    sigma_preheat_threshold: float = 2.0  
    sigma_rupture_threshold: float = 3.5  

class TensionGradientObserver:
    def __init__(self, policy: RiskPolicy, memory_size: int = 720):
        self.policy = policy
        self.spread_history = deque(maxlen=memory_size)
        self.stress_history = deque(maxlen=memory_size)
        
    def evaluate_tension(self, spread_yield: float, accumulated_stress: float) -> tuple[TensionPhase, float]:
        self.spread_history.append(spread_yield)
        self.stress_history.append(accumulated_stress)
        
        if len(self.stress_history) < 100:
            return TensionPhase.NORMAL, 0.0
            
        mean_stress = sum(self.stress_history) / len(self.stress_history)
        variance = sum((x - mean_stress) ** 2 for x in self.stress_history) / len(self.stress_history)
        std_dev = math.sqrt(variance) if variance > 0 else 0.0001
        
        current_z_score = (accumulated_stress - mean_stress) / std_dev
        
        if current_z_score >= self.policy.sigma_rupture_threshold:
            return TensionPhase.RUPTURE, current_z_score
        elif current_z_score >= self.policy.sigma_preheat_threshold:
            return TensionPhase.PRE_HEATING, current_z_score
        
        return TensionPhase.NORMAL, current_z_score

def get_source_hash() -> str:
    """Extracts the SHA-256 hash of the pure source code to prove execution identity."""
    path = os.path.abspath(__file__)
    if path.endswith('.pyc'):
        path = path[:-1]
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()