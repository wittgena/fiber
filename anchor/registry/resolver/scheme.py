# anchor.registry.resolver.scheme
import random
from typing import Dict, Any, Tuple

class SchemeResolver:
    """
    @desc: Resolves dynamic execution schemes based on injected scheme maps.
    @note: Dependency Inverted. This module does NOT depend on higher-level agent domains.
    """
    def __init__(self, schemes: Dict[Any, Any]):
        if not schemes:
            raise ValueError("Schemes dictionary cannot be empty.")
        self.schemes = schemes

    def select(self, category: Any) -> Tuple[str, Any]:
        if category not in self.schemes:
            raise ValueError(f"Invalid scheme category: {category}")
            
        target = self.schemes[category]
        selected_scheme = random.choice(target) if isinstance(target, list) else target
        category_name = category.value if hasattr(category, 'value') else str(category)
        return category_name, selected_scheme