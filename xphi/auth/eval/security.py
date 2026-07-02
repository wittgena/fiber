# xphi.auth.eval.security
## @lineage: xphi.xor.security.risk
from __future__ import annotations
from enum import Enum
from rich.text import Text

_RISK_ORDER = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}

class SecurityRisk(str, Enum):
    UNKNOWN = "UNKNOWN"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"

    @property
    def description(self) -> str:
        """Get a human-readable description of the risk level."""
        descriptions = {
            SecurityRisk.LOW: (
                "Low risk - Safe operation with minimal security impact"
            ),
            SecurityRisk.MEDIUM: (
                "Medium risk - Moderate security impact, review recommended"
            ),
            SecurityRisk.HIGH: (
                "High risk - Significant security impact, confirmation required"
            ),
            SecurityRisk.UNKNOWN: ("Unknown risk - Risk level could not be determined"),
        }
        return descriptions.get(self, "Unknown risk level")

    def __str__(self) -> str:
        return self.name

    def get_color(self) -> str:
        """Get the color for displaying this risk level in Rich text."""
        color_map = {
            SecurityRisk.LOW: "green",
            SecurityRisk.MEDIUM: "yellow",
            SecurityRisk.HIGH: "red",
            SecurityRisk.UNKNOWN: "white",
        }
        return color_map.get(self, "white")

    @property
    def visualize(self) -> Text:
        """Return Rich Text representation of this risk level."""
        content = Text()
        content.append(
            "Predicted Security Risk: ",
            style="bold",
        )
        content.append(
            f"{self.value}\n\n",
            style=f"bold {self.get_color()}",
        )
        return content

    def is_riskier(self, other: SecurityRisk, reflexive: bool = True) -> bool:
        if self.value == SecurityRisk.UNKNOWN or other.value == SecurityRisk.UNKNOWN:
            raise ValueError("Cannot compare unknown risk levels.")

        return _RISK_ORDER[self.value] > _RISK_ORDER[other.value] or (
            reflexive and self == other
        )

    def _check_comparable(self, other: object) -> int | None:
        if not isinstance(other, SecurityRisk):
            return None
        if self == SecurityRisk.UNKNOWN or other == SecurityRisk.UNKNOWN:
            raise ValueError("Cannot compare unknown risk levels.")
        return _RISK_ORDER[other.value]

    def __lt__(self, other: object) -> bool:
        other_ord = self._check_comparable(other)
        if other_ord is None:
            return NotImplemented
        return _RISK_ORDER[self.value] < other_ord

    def __gt__(self, other: object) -> bool:
        other_ord = self._check_comparable(other)
        if other_ord is None:
            return NotImplemented
        return _RISK_ORDER[self.value] > other_ord

    def __le__(self, other: object) -> bool:
        other_ord = self._check_comparable(other)
        if other_ord is None:
            return NotImplemented
        return _RISK_ORDER[self.value] <= other_ord

    def __ge__(self, other: object) -> bool:
        other_ord = self._check_comparable(other)
        if other_ord is None:
            return NotImplemented
        return _RISK_ORDER[self.value] >= other_ord
