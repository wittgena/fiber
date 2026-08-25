# runtime.action.factory
## @lineage: agent.loop.conv.action.factory
## @lineage: agent.runtime.conv.action.factory
from collections.abc import Callable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from pydantic import Field, create_model
from rich.text import Text

from fiber.dphi.space.action.action import Action, Observation
from fiber.dphi.space.action.executor import ActionExecutor
from fiber.dphi.space.action.builder import ActionAnnotations, ActionDefinition
from fiber.runtime.command.protocol import ProtoConv

class MessageIntent(str, Enum):
    REPORT = "report"
    CLARIFY = "clarify"
    SUMMARY = "summary"

class TopologicalIntent(str, Enum):
    REPLAN = "replan"
    ESCALATE = "escalate"
    OPTIMIZE_PROMPT = "optimize_prompt"
    DELEGATE_TASK = "delegate_task"
    DEBUG = "debug"

class CoreAction(str, Enum):
    """@desc: Action Name Resolver"""
    FINISH = "finish"
    THINK = "think"
    LANG = "lang"
    BRIDGE = "bridge"
    SIGNAL = "signal"

    @classmethod
    def is_safe_cognitive(cls, tool_name: str) -> bool:
        return tool_name in {cls.FINISH, cls.THINK, cls.LANG, cls.BRIDGE}

ActionHandler = Callable[[Any, "ProtoConv | None", type[Observation]], Observation]
Visualizer = Callable[[Any], Text]

class ActionProxy:
    def __init__(self, tool_cls: type[ActionDefinition], tool_name: str, description: str, action_type: type, obs_type: type, executor: ActionExecutor, annotations: ActionAnnotations):
        self._tool_cls = tool_cls
        self._tool_name = tool_name
        self._description = description
        self._action_type = action_type
        self._obs_type = obs_type
        self._executor = executor
        self._annotations = annotations

    def create(self, conv_state=None, **params) -> Sequence[ActionDefinition]:
        if params:
            raise ValueError(f"{self._tool_name}Tool doesn't accept parameters")
        
        tool = self._tool_cls(
            description=self._description,
            action_type=self._action_type,
            observation_type=self._obs_type,
            executor=self._executor,
            annotations=self._annotations
        )
        return [tool]


def build_action(
    name: str,
    description: str,
    fields: dict[str, tuple[type, Any]],
    handler: ActionHandler,
    action_visualizer: Optional[Visualizer] = None,
    obs_fields: Optional[dict[str, tuple[type, Any]]] = None,
    hide_observation: bool = False,
    annotations: Optional[ActionAnnotations] = None,
) -> ActionProxy:
    
    ## Action 합성
    action_name = f"{name.capitalize()}Action"
    DynamicAction = create_model(action_name, __module__=__name__, __base__=Action, **fields)
    if action_visualizer:
        DynamicAction.visualize = property(lambda self: action_visualizer(self))

    ## Observation 합성
    obs_name = f"{name.capitalize()}Observation"
    DynamicObservation = create_model(obs_name, __module__=__name__, __base__=Observation, **(obs_fields or {}))
    if hide_observation:
        DynamicObservation.visualize = property(lambda self: Text())

    ## Executor 합성 (호환성을 위해 유지는 하되, 실제 실행 의존도는 낮춤)
    class DynamicExecutor(ActionExecutor):
        def __call__(self, action: DynamicAction, conversation: "ProtoConv | None" = None) -> Observation:
            return handler(action, conversation, DynamicObservation)
            
    DynamicExecutor.__name__ = f"{name.capitalize()}Executor"
    DynamicExecutor.__module__ = __name__

    ## [NEW] Tool 자체에 실행 컨텍스트 바인딩 (직렬화 유실 방지)
    def _execute_tool(self, action: DynamicAction, conversation: "ProtoConv | None" = None) -> Observation:
        return handler(action, conversation, DynamicObservation)

    ## ActionDefinition 서브클래스 런타임 동적 생성
    tool_class_name = f"{name.capitalize()}Tool"
    
    @classmethod
    def _stub_create(cls, *args, **kwargs):
        raise NotImplementedError("Dynamic tools are instantiated via Proxy.")

    DynamicToolClass = type(
        tool_class_name,
        (ActionDefinition,), 
        {"create": _stub_create, "__call__": _execute_tool}
    )

    return ActionProxy(
        tool_cls=DynamicToolClass,
        tool_name=name,
        description=description,
        action_type=DynamicAction,
        obs_type=DynamicObservation,
        executor=DynamicExecutor(),
        annotations=annotations or ActionAnnotations(
            readOnlyHint=True, destructiveHint=False,
            idempotentHint=True, openWorldHint=False,
        )
    )