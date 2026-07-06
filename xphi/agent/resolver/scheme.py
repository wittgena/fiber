# xphi.agent.resolver.scheme
## @lineage: xphi.agent.scheme.resolver
"""@desc: Selects execution schemes dynamically based on injected lists."""
import random
from typing import List, Tuple

class SchemeResolver:
    """Provides methods to resolve and select schemes dynamically from injected data."""
    def __init__(self, internal_schemes: List[str], external_schemes: List[str]):
        self.internal_schemes = internal_schemes
        self.external_schemes = external_schemes

    def select(self, is_external: bool = False) -> Tuple[str, str]:
        """Returns (Pool Name, Selected Scenario) based on the chosen mode."""
        if is_external:
            if not self.external_schemes:
                raise ValueError("External schemes list is empty or not provided.")
            return "EXTERNAL", random.choice(self.external_schemes)
        
        if not self.internal_schemes:
            raise ValueError("Internal schemes list is empty or not provided.")
        return "INTERNAL", random.choice(self.internal_schemes)