# atoa.gov.security.confirm
## @lineage: agent.gov.security.confirm
## @lineage: atoa.security.confirm
## @lineage: gov.security.confirm
## @lineage: gov.policy.security.confirm
from abc import ABC, abstractmethod
from pydantic import field_validator

from atoa.gov.security.eval import SecurityRisk
from arch.topos.bound.surge.disc import DiscMixin

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
