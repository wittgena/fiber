# gov.action.definition
## @lineage: atoa.disc.action.definition
import threading
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any, Tuple, ClassVar, TypeVar, Self, TYPE_CHECKING
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Callable, Sequence
from pydantic import Field, create_model, PrivateAttr

from pydantic import Field, ConfigDict, computed_field, field_validator, field_serializer, BaseModel, model_validator
from pydantic.json_schema import SkipJsonSchema
from openai.types.responses import FunctionToolParam

from atoa.mesh.schema.action import Action, Observation, Schema
from atoa.mesh.secure.security.eval import SecurityRisk

from eco.tenant.switch.params import ChatCompletionToolParam, ChatCompletionToolParamFunctionChunk

from gov.disc.tool import Tool
from gov.action.executor import ActionExecutor, ExecutableTool, ActionT, ObservationT

from arch.topos.bound.surge.model import SurgeBaseModel
from arch.topos.bound.surge.disc import DiscMixin, kind_of, get_known_concrete_subclasses
from watcher.plane.emitter import get_logger

def camel_to_snake(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

class ActionAnnotations(SurgeBaseModel):
    title: str = ""
    readOnlyHint: bool = False
    destructiveHint: bool = False
    idempotentHint: bool = False
    openWorldHint: bool = False

class DeclaredResources(SurgeBaseModel):
    keys: Tuple[str, ...] = ()
    declared: bool = False

class ActionDefinition[ActionT, ObservationT](DiscMixin, ABC):
    """The central abstraction for all system tools. Acts as an adapter and executor proxy."""
    
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, arbitrary_types_allowed=True)
    _default_name: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._default_name = camel_to_snake(cls.__name__).removesuffix("_tool")

    name: str = Field(default="")
    description: str
    action_type: type[Action] = Field(repr=False)
    observation_type: type[Observation] | None = Field(default=None, repr=False)
    annotations: ActionAnnotations | None = None
    meta: dict[str, Any] | None = None
    # executor: SkipJsonSchema[ActionExecutor | None] = Field(default=None, repr=False, exclude=True)
    _executor: ActionExecutor | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _resolve_tool_name(self) -> Self:
        """
        Factory에서 name을 주입했다면 그대로 유지하고, 
        주입 없이 생성된 정적 도구라면 _default_name을 Fallback으로 사용합니다.
        """
        if not self.name:
            # frozen=True 모델이므로 object.__setattr__를 통해 안전하게 초기화 우회
            object.__setattr__(self, "name", getattr(self.__class__, "_default_name", "unknown"))
        return self

    @classmethod
    @abstractmethod
    def create(cls, *args, **kwargs) -> Sequence[Self]:
        raise NotImplementedError("ActionDefinition subclasses must implement .create()")

    @computed_field(return_type=str, alias="title")
    @property
    def title(self) -> str:
        if self.annotations and self.annotations.title:
            return self.annotations.title
        return self.name

    @field_serializer("action_type")
    def _ser_action_type(self, t: type[Action]) -> str:
        return kind_of(t)

    @field_serializer("observation_type")
    def _ser_observation_type(self, t: type[Observation] | None) -> str | None:
        return None if t is None else kind_of(t)

    @field_validator("action_type", mode="before")
    @classmethod
    def _val_action_type(cls, v):
        if isinstance(v, str):
            return Action.resolve_kind(v)
        assert isinstance(v, type) and issubclass(v, Action), (
            f"action_type must be a subclass of Action, but got {type(v)}"
        )
        return v

    @field_validator("observation_type", mode="before")
    @classmethod
    def _val_observation_type(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            v = Observation.resolve_kind(v)
        assert isinstance(v, type) and issubclass(v, Observation), (
            f"observation_type must be a subclass of Observation, but got {type(v)}"
        )
        return v

    def set_executor(self, executor: ActionExecutor) -> Self:
        return self.model_copy(update={"executor": executor})

    def as_executable(self) -> ExecutableTool:
        if self.executor is None:
            raise NotImplementedError(f"Tool '{self.name}' has no executor attached.")
        return self  # type: ignore[return-value]

    def declared_resources(self, action: Action) -> DeclaredResources:  # noqa: ARG002
        return DeclaredResources(keys=(), declared=False)

    def action_from_arguments(self, arguments: dict[str, Any]) -> Action:
        return self.action_type.model_validate(arguments)

    @property
    def executor(self) -> ActionExecutor | None:
        return self._executor

    def set_executor(self, executor: ActionExecutor) -> Self:
        self._executor = executor
        return self
        
    def as_executable(self) -> ExecutableTool:
        if self._executor is None:
            raise NotImplementedError(f"Tool '{self.name}' has no executor attached.")
        return self

    def __call__(self, action: ActionT, conversation: "Conv | None" = None) -> Observation:
        if self.executor is None:
            raise NotImplementedError(f"Tool '{self.name}' has no executor attached.")

        result = self._executor(action, conversation)
        if self.observation_type:
            if isinstance(result, self.observation_type):
                return result
            return self.observation_type.model_validate(result)
        else:
            if isinstance(result, Observation):
                return result
            elif isinstance(result, BaseModel):
                return Observation.model_validate(result.model_dump())
            elif isinstance(result, dict):
                return Observation.model_validate(result)
            raise TypeError("Output must be dict or BaseModel when no output schema is defined.")

    def to_mcp_tool(
        self,
        input_schema: dict[str, Any] | None = None,
        output_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return ToolFormatter.as_mcp(self, input_schema, output_schema)

    def _get_schema(
        self,
        add_security_risk_prediction: bool = False,
        action_type: type[Schema] | None = None,
    ) -> dict[str, Any]:
        return ToolFormatter.get_schema(self, add_security_risk_prediction, action_type)

    def to_openai_tool(
        self,
        add_security_risk_prediction: bool = False,
        action_type: type[Schema] | None = None,
    ) -> ChatCompletionToolParam:
        return ToolFormatter.as_openai(self, add_security_risk_prediction, action_type)

    def to_responses_tool(
        self,
        add_security_risk_prediction: bool = False,
        action_type: type[Schema] | None = None,
    ) -> FunctionToolParam:
        return ToolFormatter.as_responses(self, add_security_risk_prediction, action_type)

    @classmethod
    def resolve_kind(cls, kind: str) -> type:
        for subclass in get_known_concrete_subclasses(cls):
            if subclass.__name__ == kind:
                return subclass

        possible_kinds = [subclass.__name__ for subclass in get_known_concrete_subclasses(cls)]
        possible_kinds_str = ", ".join(sorted(possible_kinds)) if possible_kinds else "none"
        error_msg = (
            f"Unexpected kind '{kind}' for {cls.__name__}. "
            f"Expected one of: {possible_kinds_str}. "
        )
        raise ValueError(error_msg)

class ToolFormatter:
    """Translates ActionDefinitions into vendor-specific API schemas (MCP, OpenAI, etc.)."""
    
    @staticmethod
    def get_schema(
        tool: 'ActionDefinition', 
        add_security_risk_prediction: bool = False, 
        action_type: type[Schema] | None = None
    ) -> dict[str, Any]:
        act_type = action_type or tool.action_type
        add_risk = add_security_risk_prediction and (
            tool.annotations is None or (not tool.annotations.readOnlyHint)
        )
        if add_risk:
            act_type = ActionSchemaBuilder.with_risk(act_type)

        act_type = ActionSchemaBuilder.with_summary(act_type)
        return act_type.to_mcp_schema()

    @staticmethod
    def as_mcp(
        tool: 'ActionDefinition', 
        input_schema: dict[str, Any] | None = None, 
        output_schema: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        out = {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": input_schema or tool.action_type.to_mcp_schema(),
        }
        if tool.annotations:
            out["annotations"] = tool.annotations
        if tool.meta is not None:
            out["_meta"] = tool.meta

        derived_output = (
            output_schema if output_schema is not None
            else (tool.observation_type.to_mcp_schema() if tool.observation_type else None)
        )
        if derived_output is not None:
            out["outputSchema"] = derived_output
        return out

    @staticmethod
    def as_openai(
        tool: 'ActionDefinition', 
        add_security_risk_prediction: bool = False, 
        action_type: type[Schema] | None = None
    ) -> ChatCompletionToolParam:
        return ChatCompletionToolParam(
            type="function",
            function=ChatCompletionToolParamFunctionChunk(
                name=tool.name,
                description=tool.description,
                parameters=ToolFormatter.get_schema(tool, add_security_risk_prediction, action_type)
            ),
        )

    @staticmethod
    def as_responses(
        tool: 'ActionDefinition', 
        add_security_risk_prediction: bool = False, 
        action_type: type[Schema] | None = None
    ) -> FunctionToolParam:
        return {
            "type": "function",
            "name": tool.name,
            "description": tool.description,
            "parameters": ToolFormatter.get_schema(tool, add_security_risk_prediction, action_type),
            "strict": False,
        }

class ActionSchemaBuilder:
    """Dynamically builds and caches extended Action schemas (Summary, Risk)."""
    _cache_risk: dict[type, type] = {}
    _cache_summary: dict[type, type] = {}
    _lock = threading.Lock()

    @classmethod
    def with_risk(cls, action_type: type[Schema]) -> type[Schema]:
        with cls._lock:
            if action_type in cls._cache_risk:
                return cls._cache_risk[action_type]

            target_name = f"{action_type.__name__}WithRisk"
            for sub in action_type.__subclasses__():
                if sub.__name__ == target_name:
                    cls._cache_risk[action_type] = sub
                    return sub

            new_type = create_model(
                target_name,
                __module__=action_type.__module__,
                __base__=action_type,
                security_risk=(SecurityRisk, Field(description="The LLM's assessment of the safety risk of this action."))
            )
            cls._cache_risk[action_type] = new_type
            return new_type

    @classmethod
    def with_summary(cls, action_type: type[Schema]) -> type[Schema]:
        if "summary" in action_type.model_fields:
            return action_type

        with cls._lock:
            if action_type in cls._cache_summary:
                return cls._cache_summary[action_type]

            target_name = f"{action_type.__name__}WithSummary"
            for sub in action_type.__subclasses__():
                if sub.__name__ == target_name:
                    cls._cache_summary[action_type] = sub
                    return sub

            new_type = create_model(
                target_name,
                __module__=action_type.__module__,
                __base__=action_type,
                summary=(str | None, Field(
                    default=None,
                    description=(
                        "A concise summary (approximately 10 words) describing what "
                        "this specific action does. Focus on the key operation and target. "
                        "Example: 'List all Python files in current directory'"
                    )
                ))
            )
            cls._cache_summary[action_type] = new_type
            return new_type