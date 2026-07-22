# atoa.call.action.tool.handle
## @lineage: agent.call.action.tool.handle
## @lineage: gov.conv.action.tool.handle
"""
@desc: Table-driven Dynamic Tool Generation Factory (Universal Registry)
@flow: Single Source of Truth for all state-manipulation tools.
"""
import json
from pathlib import Path
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from rich.text import Text

from eco.call.disc.action import Action, Observation
if TYPE_CHECKING:
    from atoa.agent.disc.base.conv import ProtoConv

from atoa.call.action.factory import MessageIntent, TopologicalIntent, CoreAction, ActionProxy, build_action
from arch.xor.parser.action import ActionSchemaCompiler

def _handle_finish(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    state = getattr(conv, "state", None) if conv else None
    msg = f"Task marked as finished: {action.summary}" if getattr(action, "summary", None) else "Task marked as finished."
    if state: state.execution_status = ConverStatus.FINISHED
    return ObsClass.from_text(text=msg)

def _handle_think(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    return ObsClass.from_text(text="Your thought has been logged.")

def _handle_lang(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    state = getattr(conv, "state", None) if conv else None
    msg = "Message successfully delivered to the user."
    if state:
        if action.intent == MessageIntent.CLARIFY:
            state.execution_status = ConverStatus.WAITING_FOR_USER
            msg += " System paused. Waiting for user input..."
        elif action.intent == MessageIntent.SUMMARY:
            state.execution_status = ConverStatus.FINISHED
            msg += " Task marked as complete. Terminating execution loop."
    return ObsClass.from_text(text=msg)

def _handle_bridge(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    from atoa.agent.disc.event.llm.observation import ObservationEvent
    from arch.contract.event.next import next_id

    state = getattr(conv, "state", None) if conv else None
    msg = "Bridge initiated."
    kwargs = {"routing_intent": action.intent, "requires_halt": True}
    
    if state:
        is_critical = action.tension_level and action.tension_level >= 4
        if is_critical or action.intent != TopologicalIntent.REPLAN:
            msg = f"Execution Halted. System taking control for routing: {action.intent.value.upper()}"
            state.execution_status = ConverStatus.NEEDS_REPLAN
            state.agent_state = {
                **state.agent_state,
                "pending_route": {
                    "intent": action.intent.value,
                    "tension": action.tension_level,
                    "target_aspects": action.target_aspects,
                    "original_thought": action.thought
                }
            }
        else:
            msg = "System Note: Intent logged. Continuing current loop."
            kwargs["requires_halt"] = False
            if hasattr(state, "inject_virtual_event"):
                bridge_id = f"bridge-{next_id()}"
                overlay_msg = f"## @topos.intent: {action.intent.value}"
                if action.target_aspects: overlay_msg += f" | @target.aspects: {', '.join(action.target_aspects)}"
                if action.tension_level: overlay_msg += f" | @cognitive.tension: {action.tension_level}/5"
                
                state.inject_virtual_event(ObservationEvent(
                    id=bridge_id, action_id="system-orchestrator", tool_name="bridge",
                    tool_call_id=f"virtual-call-{next_id()}",
                    observation=ObsClass.from_text(text=overlay_msg, routing_intent=action.intent, requires_halt=False)
                ))
                msg += "\n(Phase hints have been securely overlaid on your context.)"
    return ObsClass.from_text(text=msg, **kwargs)

def _handle_signal(action: Any, conv: "ProtoConv | None", ObsClass: type[Observation]) -> Observation:
    from atoa.agent.disc.status import ConverStatus
    state = getattr(conv, "state", None) if conv else None
    msg = (
        f"[Semantic Telemetry 📡] Broadcasted to '{action.channel}'.\n"
        f"Target Audience: {action.audience.upper()}\n"
        f"Translation: {action.semantic_translation}"
    )
    if action.requires_consensus and state:
        state.execution_status = ConverStatus.WAITING_FOR_USER
        msg += "\n[Status] System paused. Waiting for human consensus (Merge/Approval)."
    return ObsClass.from_text(text=msg)

HANDLERS: Dict[str, Callable] = {
    CoreAction.FINISH.value: _handle_finish,
    CoreAction.THINK.value: _handle_think,
    CoreAction.LANG.value: _handle_lang,
    CoreAction.BRIDGE.value: _handle_bridge,
    CoreAction.SIGNAL.value: _handle_signal,
}

VISUALIZERS: Dict[str, Callable[[Any], Text]] = {
    CoreAction.FINISH.value: lambda act: Text(f"🏁 Finish: {getattr(act, 'summary', 'Task Complete')}", style="bold green"),
    CoreAction.THINK.value: lambda act: Text(f"🤔 Thinking: \n{act.thought}", style="italic white"),
    CoreAction.LANG.value: lambda act: Text(f"💬 Lang [{act.intent.value.upper()}]: \n{act.message}", style="cyan"),
    CoreAction.BRIDGE.value: lambda act: Text(f"🌉 Bridge [{act.intent.value.upper()}]\n{act.thought}", style="cyan"),
    CoreAction.SIGNAL.value: lambda act: Text(f"📡 Signal [{act.channel}]: {act.semantic_translation}", style="bold magenta"),
}