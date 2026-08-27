# fiber.dphi.model.ext.llm.handle.template
## @lineage: dphi.model.ext.llm.handle.template
## @lineage: phase.client.model.llm.handle.template
## @lineage: phase.client.ext.llm.handle.template
## @lineage: bound.client.ext.llm.handle.template
## @lineage: ator.client.ext.llm.handle.template
## @lineage: bound.eco.agent.llm.handle.template
## @lineage: eco.bound.agent.llm.handle.template
## @lineage: bound.agent.llm.handle.template
## @lineage: ext.router.llm.handle.template
## @lineage: router.llm.handle.template
## @lineage: engine.router.llm.handle.template
## @lineage: engine.eco.llm.handle.template
## @lineage: runtime.engine.eco.llm.handle.template
## @lineage: eco.llms.handle.template
## @lineage: eco.llms.template
from abc import ABC, abstractmethod
from enum import Enum
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from typing_extensions import Annotated

from fiber.dphi.model.mapper.pydantic import (
    Field,
    WithJsonSchema,
    PlainSerializer,
    SerializeAsAny,
)

from fiber.dphi.model.ext.llm.model.types.block import ChatMessage, MessageRole
from fiber.dphi.model.mapper.pydantic import BaseModel, ConfigDict
from fiber.dphi.model.ext.llm.model.base import BaseLLM
from fiber.dphi.model.util import get_template_vars, format_string
from fiber.dphi.model.ext.types.base import BaseOutputParser

def default_messages_to_prompt(messages: Sequence[ChatMessage]) -> str:
    """Convert messages to a prompt string."""
    string_messages = []
    for message in messages:
        role = message.role
        content = message.content
        string_message = f"{role.value}: {content}"

        additional_kwargs = message.additional_kwargs
        if additional_kwargs:
            string_message += f"\n{additional_kwargs}"
        string_messages.append(string_message)

    string_messages.append(f"{MessageRole.ASSISTANT.value}: ")
    return "\n".join(string_messages)


def prompt_to_messages(prompt: str) -> List[ChatMessage]:
    """Convert a string prompt to a sequence of messages."""
    return [ChatMessage(role=MessageRole.USER, content=prompt)]

AnnotatedCallable = Annotated[
    Callable,
    WithJsonSchema({"type": "string"}),
    WithJsonSchema({"type": "string"}),
    PlainSerializer(lambda x: f"{x.__module__}.{x.__name__}", return_type=str),
]

class PromptType(str, Enum):
    SUMMARY = "summary"
    TREE_INSERT = "insert"
    TREE_SELECT = "tree_select"
    TREE_SELECT_MULTIPLE = "tree_select_multiple"
    QUESTION_ANSWER = "text_qa"
    REFINE = "refine"
    KEYWORD_EXTRACT = "keyword_extract"
    QUERY_KEYWORD_EXTRACT = "query_keyword_extract"
    SCHEMA_EXTRACT = "schema_extract"
    TEXT_TO_SQL = "text_to_sql"
    TEXT_TO_GRAPH_QUERY = "text_to_graph_query"
    TABLE_CONTEXT = "table_context"
    KNOWLEDGE_TRIPLET_EXTRACT = "knowledge_triplet_extract"
    SIMPLE_INPUT = "simple_input"
    PANDAS = "pandas"
    JSON_PATH = "json_path"
    SINGLE_SELECT = "single_select"
    MULTI_SELECT = "multi_select"
    VECTOR_STORE_QUERY = "vector_store_query"
    SUB_QUESTION = "sub_question"
    SQL_RESPONSE_SYNTHESIS = "sql_response_synthesis"
    SQL_RESPONSE_SYNTHESIS_V2 = "sql_response_synthesis_v2"
    CONVERSATION = "conversation"
    DECOMPOSE = "decompose"
    CHOICE_SELECT = "choice_select"
    CUSTOM = "custom"
    RANKGPT_RERANK = "rankgpt_rerank"


class BasePromptTemplate(BaseModel, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    metadata: Dict[str, Any]
    template_vars: List[str]
    kwargs: Dict[str, str]
    output_parser: Optional[BaseOutputParser]
    template_var_mappings: Optional[Dict[str, Any]] = Field(
        default_factory=dict,  # type: ignore
        description="Template variable mappings (Optional).",
    )
    function_mappings: Optional[Dict[str, AnnotatedCallable]] = Field(
        default_factory=dict,  # type: ignore
        description=(
            "Function mappings (Optional). This is a mapping from template "
            "variable names to functions that take in the current kwargs and "
            "return a string."
        ),
    )

    def _map_template_vars(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """For keys in template_var_mappings, swap in the right keys."""
        template_var_mappings = self.template_var_mappings or {}
        return {template_var_mappings.get(k, k): v for k, v in kwargs.items()}

    def _map_function_vars(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        function_mappings = self.function_mappings or {}
        new_kwargs = {}
        for k, v in function_mappings.items():
            new_kwargs[k] = v(**kwargs)

        for k, v in kwargs.items():
            if k not in new_kwargs:
                new_kwargs[k] = v

        return new_kwargs

    def _map_all_vars(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        new_kwargs = self._map_function_vars(kwargs)
        return self._map_template_vars(new_kwargs)

    @abstractmethod
    def partial_format(self, **kwargs: Any) -> "BasePromptTemplate": ...

    @abstractmethod
    def format(self, llm: Optional[BaseLLM] = None, **kwargs: Any) -> str: ...

    @abstractmethod
    def format_messages(
        self, llm: Optional[BaseLLM] = None, **kwargs: Any
    ) -> List[ChatMessage]: ...

    @abstractmethod
    def get_template(self, llm: Optional[BaseLLM] = None) -> str: ...

class PromptTemplate(BasePromptTemplate):
    template: str

    def __init__(
        self,
        template: str,
        prompt_type: str = PromptType.CUSTOM,
        output_parser: Optional[BaseOutputParser] = None,
        metadata: Optional[Dict[str, Any]] = None,
        template_var_mappings: Optional[Dict[str, Any]] = None,
        function_mappings: Optional[Dict[str, Callable]] = None,
        **kwargs: Any,
    ) -> None:
        if metadata is None:
            metadata = {}
        metadata["prompt_type"] = prompt_type

        template_vars = get_template_vars(template)

        super().__init__(
            template=template,
            template_vars=template_vars,
            kwargs=kwargs,
            metadata=metadata,
            output_parser=output_parser,
            template_var_mappings=template_var_mappings,
            function_mappings=function_mappings,
        )

    def partial_format(self, **kwargs: Any) -> "PromptTemplate":
        output_parser = self.output_parser
        self.output_parser = None
        prompt = deepcopy(self)
        prompt.kwargs.update(kwargs)
        prompt.output_parser = output_parser
        self.output_parser = output_parser
        return prompt

    def format(
        self,
        llm: Optional[BaseLLM] = None,
        completion_to_prompt: Optional[Callable[[str], str]] = None,
        **kwargs: Any,
    ) -> str:
        """Format the prompt into a string."""
        del llm  # unused
        all_kwargs = {
            **self.kwargs,
            **kwargs,
        }

        mapped_all_kwargs = self._map_all_vars(all_kwargs)
        prompt = format_string(self.template, **mapped_all_kwargs)

        if self.output_parser is not None:
            prompt = self.output_parser.format(prompt)

        if completion_to_prompt is not None:
            prompt = completion_to_prompt(prompt)

        return prompt

    def format_messages(
        self, llm: Optional[BaseLLM] = None, **kwargs: Any
    ) -> List[ChatMessage]:
        """Format the prompt into a list of chat messages."""
        del llm  # unused
        prompt = self.format(**kwargs)
        return prompt_to_messages(prompt)

    def get_template(self, llm: Optional[BaseLLM] = None) -> str:
        return self.template

class ChatPromptTemplate(BasePromptTemplate):  # type: ignore[no-redef]
    message_templates: List[ChatMessage]

    def __init__(
        self,
        message_templates: Sequence[ChatMessage],
        prompt_type: str = PromptType.CUSTOM,
        output_parser: Optional[BaseOutputParser] = None,
        metadata: Optional[Dict[str, Any]] = None,
        template_var_mappings: Optional[Dict[str, Any]] = None,
        function_mappings: Optional[Dict[str, Callable]] = None,
        **kwargs: Any,
    ):
        if metadata is None:
            metadata = {}
        metadata["prompt_type"] = prompt_type

        template_vars = []
        for message_template in message_templates:
            template_vars.extend(message_template.get_template_vars())

        super().__init__(
            message_templates=message_templates,
            kwargs=kwargs,
            metadata=metadata,
            output_parser=output_parser,
            template_vars=template_vars,
            template_var_mappings=template_var_mappings,
            function_mappings=function_mappings,
        )

    @classmethod
    def from_messages(
        cls,
        message_templates: Union[List[Tuple[str, str]], List[ChatMessage]],
        **kwargs: Any,
    ) -> "ChatPromptTemplate":
        """From messages."""
        if isinstance(message_templates[0], tuple):
            message_templates = [
                ChatMessage.from_str(role=role, content=content)  # type: ignore[arg-type]
                for role, content in message_templates
            ]
        return cls(message_templates=message_templates, **kwargs)  # type: ignore[arg-type]

    def partial_format(self, **kwargs: Any) -> "ChatPromptTemplate":
        prompt = deepcopy(self)
        prompt.kwargs.update(kwargs)
        return prompt

    def format(
        self,
        llm: Optional[BaseLLM] = None,
        messages_to_prompt: Optional[Callable[[Sequence[ChatMessage]], str]] = None,
        **kwargs: Any,
    ) -> str:
        del llm  # unused
        messages = self.format_messages(**kwargs)

        if messages_to_prompt is not None:
            return messages_to_prompt(messages)

        return default_messages_to_prompt(messages)

    def format_messages(
        self, llm: Optional[BaseLLM] = None, **kwargs: Any
    ) -> List[ChatMessage]:
        del llm  # unused
        """Format the prompt into a list of chat messages."""
        all_kwargs = {
            **self.kwargs,
            **kwargs,
        }
        mapped_all_kwargs = self._map_all_vars(all_kwargs)

        messages: List[ChatMessage] = []
        for message_template in self.message_templates:
            messages.append(message_template.format_vars(**mapped_all_kwargs))

        if self.output_parser is not None:
            messages = self.output_parser.format_messages(messages)

        return messages

    def get_template(self, llm: Optional[BaseLLM] = None) -> str:
        return default_messages_to_prompt(self.message_templates)