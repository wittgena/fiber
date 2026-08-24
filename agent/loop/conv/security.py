# agent.loop.conv.security
## @lineage: agent.runtime.conv.security
## @lineage: agent.llm.security.analyzer
from abc import ABC, abstractmethod
from typing import ClassVar, Any

from fiber.agent.llm.driver.event.action import ActionEvent
from xphi.arch.model.conv.security.eval import SecurityRisk

from xphi.arch.model.conv.event import Event
from xphi.arch.model.surge.disc import DiscMixin
from xphi.watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class SecurityAnalyzerBase(DiscMixin, ABC):
    @abstractmethod
    def security_risk(self, action: ActionEvent) -> SecurityRisk:
        pass

    def analyze_event(self, event: Event) -> SecurityRisk | None:
        if isinstance(event, ActionEvent):
            return self.security_risk(event)
        return None

    def should_require_confirmation(
        self, risk: SecurityRisk, confirmation_mode: bool = False
    ) -> bool:
        if risk == SecurityRisk.HIGH:
            return True
        elif risk == SecurityRisk.UNKNOWN and not confirmation_mode:
            return True
        elif confirmation_mode:
            return True
        else:
            return False

    def analyze_pending_actions(
        self, pending_actions: list[ActionEvent]
    ) -> list[tuple[ActionEvent, SecurityRisk]]:
        analyzed_actions = []
        for action_event in pending_actions:
            try:
                risk = self.security_risk(action_event)
                analyzed_actions.append((action_event, risk))
                logger.debug(f"Action {action_event} analyzed with risk level: {risk}")
            except Exception as e:
                logger.error(f"Error analyzing action {action_event}: {e}")
                analyzed_actions.append((action_event, SecurityRisk.HIGH))

        return analyzed_actions