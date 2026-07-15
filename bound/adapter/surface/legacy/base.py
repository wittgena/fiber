# bound.adapter.surface.legacy.base
## @lineage: bound.surface.legacy.base
## @lineage: anchor.provider.legacy.base
## @lineage: anchor.surface.model.client.base
## @lineage: anchor.surface.model.llm.base
## @lineage: anchor.surface.model.types.base
## @lineage: anchor.surface.model.legacy.base
## @lineage: bound.adapter.legacy.llm.base
## @lineage: anchor.surface.legacy.llm.base
from typing import TYPE_CHECKING, Any, Optional, Union
from openai._models import BaseModel as OpenAIObject
from pydantic import BaseModel, ConfigDict
import httpx

from anchor.registry.model.config.resolver import config
if TYPE_CHECKING:
    from bound.transport.stream.wrapper import StreamWrapper
    from bound.adapter.surface.legacy.types import ModelResponse, TextCompletionResponse

class PydanticObjectBase(BaseModel):
    def json(self, **kwargs):  # type: ignore
        try:
            return self.model_dump(**kwargs)  # noqa
        except Exception:
            return self.dict(**kwargs)

    def fields_set(self):
        try:
            return self.model_fields_set  # noqa
        except Exception:
            # if using pydantic v1
            return self.__fields_set__

    model_config = ConfigDict(protected_namespaces=())

class BaseOpenAIResponse(BaseModel):
    model_config = ConfigDict(extra="allow", protected_namespaces=())

    def __getitem__(self, key):
        return self.__dict__[key]

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def __contains__(self, key):
        return key in self.__dict__

    def items(self):
        return self.__dict__.items()

class HiddenParams(OpenAIObject):
    original_response: Optional[Union[str, Any]] = None
    model_id: Optional[str] = None  # used in Router for individual deployments
    api_base: Optional[str] = None  # returns api base used for making completion call
    _response_ms: Optional[float] = None
    response_cost: Optional[float] = None
    model_config = ConfigDict(extra="allow", protected_namespaces=())

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def json(self, **kwargs):  # type: ignore
        try:
            return self.model_dump()  # noqa
        except Exception:
            # if using pydantic v1
            return self.dict()

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        data["_response_ms"] = self._response_ms
        return data

class BaseLLM:
    _client_session: Optional[httpx.Client] = None

    def process_response(
        self,
        model: str,
        response: httpx.Response,
        model_response: "ModelResponse",
        stream: bool,
        logging_obj: Any,
        optional_params: dict,
        api_key: str,
        data: Union[dict, str],
        messages: list,
        print_verbose,
        encoding,
    ) -> Union["ModelResponse", "CustomStreamWrapper"]:
        return model_response

    def process_text_completion_response(
        self,
        model: str,
        response: httpx.Response,
        model_response: "TextCompletionResponse",
        stream: bool,
        logging_obj: Any,
        optional_params: dict,
        api_key: str,
        data: Union[dict, str],
        messages: list,
        print_verbose,
        encoding,
    ) -> Union["TextCompletionResponse", "CustomStreamWrapper"]:
        return model_response

    def create_client_session(self):
        if config.client_session:
            _client_session = config.client_session
        else:
            _client_session = httpx.Client()
        return _client_session

    def create_aclient_session(self):
        if config.aclient_session:
            _aclient_session = config.aclient_session
        else:
            _aclient_session = httpx.AsyncClient()
        return _aclient_session

    def __exit__(self):
        if hasattr(self, "_client_session") and self._client_session is not None:
            self._client_session.close()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "_aclient_session"):
            await self._aclient_session.aclose()  # type: ignore

    def validate_environment(self, *args, **kwargs) -> Optional[Any]:
        return None

    def completion(self, *args, **kwargs) -> Any:
        return None

    def embedding(self, *args, **kwargs) -> Any:
        return None
