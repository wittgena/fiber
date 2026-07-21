# atoa.gov.action.eval
## @lineage: gov.action.eval
## @lineage: gov.engine.action.eval
## @lineage: agent.loop.handler.eval
## @lineage: agent.handler.loop.eval
from abc import ABC, abstractmethod
import json
from typing import TYPE_CHECKING, Any
from dataclasses import dataclass, field

from atoa.disc.event.llm.action import ActionEvent
from atoa.disc.event.llm.message import MessageEvent
from atoa.disc.event.llm.observation import ObservationEvent, AgentErrorEvent, UserRejectObservation
from atoa.gov.action.step import StepContext, StepHandler

from eco.call.action.message import Message, TextContent

from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)

class EvalReflector(StepHandler):
    """
    @desc: Structural Reflection Manifold / Cognitive Evaluator
    @role: 
    - Intercept the execution trajectory post-observation to evaluate goal convergence, structural degradation (errors), and local entropy (tension).
    - Inject a virtual cognitive overlay to force a paradigm shift or graceful termination.
    """
    
    REFLECTION_IDENTIFIER = "[SYSTEM_OVERLAY: EVALUATE_TRAJECTORY]"

    def handle(self, agent, conversation, on_event, on_token, context) -> bool:
        logger.debug("## @phase.evaluate: Transiting through EvalReflector Manifold")
        state = conversation.state
        
        if not self._should_trigger_reflection(state.events):
            return False
            
        logger.info("[OVERLAY] Topological anomalies validated. Injecting cognitive reflection overlay.")
        reflection_overlay = MessageEvent(
            source="user",
            llm_message=Message(
                role="assistant",
                content=[TextContent(text=(
                    f"{self.REFLECTION_IDENTIFIER} "
                    "Evaluate the current topological state against the terminal objective. "
                    "If convergence is achieved, initiate graceful termination. "
                    "If the trajectory is fractured or local entropy is rising, project an alternative paradigm."
                ))]
            ),
        )
        on_event(reflection_overlay)
        return False

    def _should_trigger_reflection(self, events: list) -> bool:
        if not events:
            return False
            
        last_event = events[-1]
        is_valid_obs_type = isinstance(last_event, (ObservationEvent, AgentErrorEvent, UserRejectObservation))
        if not is_valid_obs_type:
            return False

        ## @condition.rupture: Force immediate trajectory realignment for absolute systemic faults
        if isinstance(last_event, (AgentErrorEvent, UserRejectObservation)):
            logger.debug("[RUPTURE] Explicit systemic fault or constraint rejection detected. Forcing structural reflection.")
            return True

        ## @condition.phase_lock: Prevent recursive reflection loops by verifying recent overlay injections
        recent_action = None
        for event in reversed(events[:-1]):
            if isinstance(event, MessageEvent):
                text_contents = [c.text for c in event.llm_message.content if isinstance(c, TextContent)]
                if any(self.REFLECTION_IDENTIFIER in t for t in text_contents):
                    logger.debug("[SKIP] Reflection overlay already active in the current topological phase. Applying cooldown.")
                    return False
            
            if isinstance(event, ActionEvent):
                recent_action = event
                break

        ## @condition.degradation: Isolate raw semantic output to scan for structural degradation signatures (soft errors)
        obs_obj = getattr(last_event, "observation", None)
        
        if obs_obj:
            obs_text = getattr(obs_obj, "result", str(obs_obj)).lower()
            
            if "error:" in obs_text or "failed to" in obs_text or "exception:" in obs_text:
                logger.debug("[DEGRADATION] Semantic fault signatures detected in the observation space. Forcing reflection.")
                return True

        ## @condition.entropy: Evaluate the tension level of the preceding action to prevent localized collapse
        if recent_action and recent_action.action:
            tension_level = getattr(recent_action.action, "tension_level", 0)
            if isinstance(tension_level, int) and tension_level >= 3:
                logger.debug(f"[TENSION] High local entropy detected (Level: {tension_level}). Triggering paradigm evaluation.")
                return True

        return False