# engine.driver.security.confirm
## @lineage: engine.security.confirm
## @lineage: phi.security.confirm
## @lineage: agent.atoa.event.security.confirm
## @lineage: atoa.event.security.confirm
## @lineage: swarm.atoa.event.security.confirm
## @lineage: atoa.secure.security.confirm
from abc import ABC, abstractmethod
from pydantic import field_validator

from engine.driver.security.eval import SecurityRisk
from arch.xor.surge.disc import DiscMixin

class ConfirmationPolicyBase(DiscMixin, ABC):
    @abstractmethod
    def should_confirm(self, risk: SecurityRisk = SecurityRisk.UNKNOWN) -> bool:
        """Determine if an action with the given risk level requires confirmation"""

class AlwaysConfirm(ConfirmationPolicyBase):
    def should_confirm(
        self,
        risk: SecurityRisk = SecurityRisk.UNKNOWN,  # noqa: ARG002
    ) -> bool:
        return True


class NeverConfirm(ConfirmationPolicyBase):
    def should_confirm(
        self,
        risk: SecurityRisk = SecurityRisk.UNKNOWN,  # noqa: ARG002
    ) -> bool:
        return False


class ConfirmRisky(ConfirmationPolicyBase):
    threshold: SecurityRisk = SecurityRisk.HIGH
    confirm_unknown: bool = True

    @field_validator("threshold")
    def validate_threshold(cls, v: SecurityRisk) -> SecurityRisk:
        if v == SecurityRisk.UNKNOWN:
            raise ValueError("Threshold cannot be UNKNOWN")
        return v

    def should_confirm(self, risk: SecurityRisk = SecurityRisk.UNKNOWN) -> bool:
        if risk == SecurityRisk.UNKNOWN:
            return self.confirm_unknown

        return risk.is_riskier(self.threshold)
