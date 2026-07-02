# anchor.provider.legacy.openai.config
## @lineage: anchor.surface.model.client.openai.config
## @lineage: anchor.surface.model.openai.config
## @lineage: anchor.surface.model.types.openai.config
## @lineage: anchor.surface.model.legacy.openai.config
## @lineage: bound.adapter.legacy.llm.openai.config
## @lineage: anchor.surface.legacy.llm.openai.config
## @lineage: anchor.surface.legacy.openai.openai
## @lineage: bound.inter.llms.openai.openai
import time
import types
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Literal,
    NamedTuple,
    Optional,
    Union,
    cast,
)
from urllib.parse import urlparse
import httpx
if TYPE_CHECKING:
    from aiohttp import ClientSession

import openai
from openai import AsyncOpenAI, OpenAI
from openai.types.beta.assistant_deleted import AssistantDeleted
from openai.types.file_deleted import FileDeleted
from pydantic import BaseModel
from typing_extensions import overload

from bound.channel.config.resolver import config
from bound.channel.config.constants import DEFAULT_MAX_RETRIES
from bound.channel.client.response.iterator import BaseModelResponseIterator
from bound.channel.config.base import BaseConfig
from anchor.surface.exception import BaseLLMException
from bound.channel.compat.switch.params import ModelResponse, ModelResponseStream
from anchor.provider.types import ProviderTypes
from anchor.provider.legacy.types import EmbeddingResponse, ImageResponse, LiteLLMBatch
from anchor.provider.legacy.openai.types import *

from xphi.scope.plane.delegator import Logging as LiteLLMLoggingObj
from bound.channel.client.response.converter import convert_to_model_response_object
from bound.transport.stream.wrapper import CustomStreamWrapper
from anchor.provider.legacy.base import BaseLLM
from anchor.provider.legacy.openai.base import (
    BaseOpenAILLM,
    OpenAIError,
    drop_params_from_unprocessable_entity_error,
)

from watcher.plane.emitter import get_emitter

log = get_emitter("llms.openai")
CustomLogger = Any
FileContentProvider = Literal["openai", "azure", "vertex_ai", "bedrock", "hosted_vllm", "anthropic", "manus"]

class FileContentStreamingResult(NamedTuple):
    stream_iterator: Union[Iterator[bytes], AsyncIterator[bytes]]
    headers: Dict[str, str]


class MistralEmbeddingConfig:
    """
    Reference: https://docs.mistral.ai/api/#operation/createEmbedding
    """

    def __init__(
        self,
    ) -> None:
        locals_ = locals().copy()
        for key, value in locals_.items():
            if key != "self" and value is not None:
                setattr(self.__class__, key, value)

    @classmethod
    def get_config(cls):
        return {
            k: v
            for k, v in cls.__dict__.items()
            if not k.startswith("__")
            and not isinstance(
                v,
                (
                    types.FunctionType,
                    types.BuiltinFunctionType,
                    classmethod,
                    staticmethod,
                ),
            )
            and v is not None
        }

    def get_supported_openai_params(self):
        return [
            "encoding_format",
        ]

    def map_openai_params(self, non_default_params: dict, optional_params: dict):
        for param, value in non_default_params.items():
            if param == "encoding_format":
                optional_params["encoding_format"] = value
        return optional_params


class OpenAIConfig(BaseConfig):
    frequency_penalty: Optional[int] = None
    function_call: Optional[Union[str, dict]] = None
    functions: Optional[list] = None
    logit_bias: Optional[dict] = None
    max_completion_tokens: Optional[int] = None
    max_tokens: Optional[int] = None
    n: Optional[int] = None
    presence_penalty: Optional[int] = None
    stop: Optional[Union[str, list]] = None
    temperature: Optional[int] = None
    top_p: Optional[int] = None
    response_format: Optional[dict] = None

    def __init__(
        self,
        frequency_penalty: Optional[int] = None,
        function_call: Optional[Union[str, dict]] = None,
        functions: Optional[list] = None,
        logit_bias: Optional[dict] = None,
        max_completion_tokens: Optional[int] = None,
        max_tokens: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[int] = None,
        stop: Optional[Union[str, list]] = None,
        temperature: Optional[int] = None,
        top_p: Optional[int] = None,
        response_format: Optional[dict] = None,
    ) -> None:
        locals_ = locals().copy()
        for key, value in locals_.items():
            if key != "self" and value is not None:
                setattr(self.__class__, key, value)

    @classmethod
    def get_config(cls):
        return super().get_config()

    def get_supported_openai_params(self, model: str) -> list:
        return config.openAIGPTConfig.get_supported_openai_params(model=model)

    def _map_openai_params(
        self, non_default_params: dict, optional_params: dict, model: str
    ) -> dict:
        supported_openai_params = self.get_supported_openai_params(model)
        for param, value in non_default_params.items():
            if param in supported_openai_params:
                optional_params[param] = value
        return optional_params

    def _transform_messages(
        self, messages: List[AllMessageValues], model: str
    ) -> List[AllMessageValues]:
        return messages

    def map_openai_params(
        self,
        non_default_params: dict,
        optional_params: dict,
        model: str,
        drop_params: bool,
    ) -> dict:
        return config.openAIGPTConfig.map_openai_params(
            non_default_params=non_default_params,
            optional_params=optional_params,
            model=model,
            drop_params=drop_params,
        )

    def get_error_class(
        self, error_message: str, status_code: int, headers: Union[dict, httpx.Headers]
    ) -> BaseLLMException:
        return OpenAIError(
            status_code=status_code,
            message=error_message,
            headers=headers,
        )

    def transform_request(
        self,
        model: str,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        headers: dict,
    ) -> dict:
        messages = self._transform_messages(messages=messages, model=model)
        return {"model": model, "messages": messages, **optional_params}

    def transform_response(
        self,
        model: str,
        raw_response: httpx.Response,
        model_response: ModelResponse,
        logging_obj: LiteLLMLoggingObj,
        request_data: dict,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        encoding: Any,
        api_key: Optional[str] = None,
        json_mode: Optional[bool] = None,
    ) -> ModelResponse:
        logging_obj.post_call(original_response=raw_response.text)
        logging_obj.model_call_details["response_headers"] = raw_response.headers
        final_response_obj = cast(
            ModelResponse,
            convert_to_model_response_object(
                response_object=raw_response.json(),
                model_response_object=model_response,
                hidden_params={"headers": raw_response.headers},
                _response_headers=dict(raw_response.headers),
            ),
        )

        return final_response_obj

    def validate_environment(
        self,
        headers: dict,
        model: str,
        messages: List[AllMessageValues],
        optional_params: dict,
        litellm_params: dict,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
    ) -> dict:
        return {
            "Authorization": f"Bearer {api_key}",
            **headers,
        }

    def get_model_response_iterator(
        self,
        streaming_response: Union[Iterator[str], AsyncIterator[str], ModelResponse],
        sync_stream: bool,
        json_mode: Optional[bool] = False,
    ) -> Any:
        return OpenAIChatCompletionResponseIterator(
            streaming_response=streaming_response,
            sync_stream=sync_stream,
            json_mode=json_mode,
        )


class OpenAIChatCompletionResponseIterator(BaseModelResponseIterator):
    def chunk_parser(self, chunk: dict) -> ModelResponseStream:
        """
        {'choices': [{'delta': {'content': '', 'role': 'assistant'}, 'finish_reason': None, 'index': 0, 'logprobs': None}], 'created': 1735763082, 'id': 'a83a2b0fbfaf4aab9c2c93cb8ba346d7', 'model': 'mistral-large', 'object': 'chat.completion.chunk'}
        """
        try:
            return ModelResponseStream(**chunk)
        except Exception as e:
            raise e

class OpenAIMultiModal(BaseOpenAILLM):
    def __init__(self) -> None:
        super().__init__()

    def _set_dynamic_params_on_client(
        self,
        client: Union[OpenAI, AsyncOpenAI],
        organization: Optional[str] = None,
        max_retries: Optional[int] = None,
    ):
        if organization is not None:
            client.organization = organization
        if max_retries is not None:
            client.max_retries = max_retries

    def _get_openai_client(
        self,
        is_async: bool,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        api_version: Optional[str] = None,
        timeout: Union[float, httpx.Timeout] = httpx.Timeout(None),
        max_retries: Optional[int] = DEFAULT_MAX_RETRIES,
        organization: Optional[str] = None,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Optional[Union[OpenAI, AsyncOpenAI]]:
        client_initialization_params: Dict = locals()
        if client is None:
            if not isinstance(max_retries, int):
                raise OpenAIError(
                    status_code=422,
                    message="max retries must be an int. Passed in value: {}".format(
                        max_retries
                    ),
                )
            cached_client = self.get_cached_openai_client(
                client_initialization_params=client_initialization_params,
                client_type="openai",
            )

            if cached_client:
                if isinstance(cached_client, OpenAI) or isinstance(
                    cached_client, AsyncOpenAI
                ):
                    return cached_client
            if is_async:
                _new_client: Union[OpenAI, AsyncOpenAI] = AsyncOpenAI(
                    api_key=api_key,
                    base_url=api_base,
                    http_client=OpenAIChatCompletion._get_async_http_client(
                        shared_session=shared_session
                    ),
                    timeout=timeout,
                    max_retries=max_retries,
                    organization=organization,
                )
            else:
                _new_client = OpenAI(
                    api_key=api_key,
                    base_url=api_base,
                    http_client=OpenAIChatCompletion._get_sync_http_client(),
                    timeout=timeout,
                    max_retries=max_retries,
                    organization=organization,
                )

            ## SAVE CACHE KEY
            self.set_cached_openai_client(
                openai_client=_new_client,
                client_initialization_params=client_initialization_params,
                client_type="openai",
            )
            return _new_client

        else:
            self._set_dynamic_params_on_client(
                client=client,
                organization=organization,
                max_retries=max_retries,
            )
            return client

    async def aimage_generation(
        self,
        prompt: str,
        data: dict,
        model_response: ModelResponse,
        timeout: float,
        logging_obj: Any,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        client=None,
        max_retries=None,
        organization: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        response = None
        try:
            openai_aclient = self._get_openai_client(
                is_async=True,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )

            if headers:
                data["extra_headers"] = headers
            response = await openai_aclient.images.generate(**data, timeout=timeout)  # type: ignore
            stringified_response = response.model_dump()
            ## LOGGING
            logging_obj.post_call(
                input=prompt,
                api_key=api_key,
                additional_args={"complete_input_dict": data},
                original_response=stringified_response,
            )
            return convert_to_model_response_object(response_object=stringified_response, model_response_object=model_response, response_type="image_generation")  # type: ignore
        except Exception as e:
            ## LOGGING
            logging_obj.post_call(
                input=prompt,
                api_key=api_key,
                original_response=str(e),
            )
            raise e

    def image_generation(
        self,
        model: Optional[str],
        prompt: str,
        timeout: float,
        optional_params: dict,
        logging_obj: Any,
        api_key: Optional[str] = None,
        api_base: Optional[str] = None,
        model_response: Optional[ImageResponse] = None,
        client=None,
        aimg_generation=None,
        organization: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> ImageResponse:
        data = {}
        try:
            data = {"model": model, "prompt": prompt, **optional_params}
            max_retries = data.pop("max_retries", 2)
            if not isinstance(max_retries, int):
                raise OpenAIError(status_code=422, message="max retries must be an int")

            if aimg_generation is True:
                return self.aimage_generation(data=data, prompt=prompt, logging_obj=logging_obj, model_response=model_response, api_base=api_base, api_key=api_key, timeout=timeout, client=client, max_retries=max_retries, organization=organization, headers=headers)  # type: ignore

            openai_client: OpenAI = self._get_openai_client(  # type: ignore
                is_async=False,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )

            ## LOGGING
            logging_obj.pre_call(
                input=prompt,
                api_key=openai_client.api_key,
                additional_args={
                    "headers": {"Authorization": f"Bearer {openai_client.api_key}"},
                    "api_base": openai_client._base_url._uri_reference,
                    "acompletion": True,
                    "complete_input_dict": data,
                },
            )

            ## COMPLETION CALL
            if headers:
                data["extra_headers"] = headers
            _response = openai_client.images.generate(**data, timeout=timeout)  # type: ignore

            response = _response.model_dump()
            ## LOGGING
            logging_obj.post_call(
                input=prompt,
                api_key=api_key,
                additional_args={"complete_input_dict": data},
                original_response=response,
            )
            return convert_to_model_response_object(response_object=response, model_response_object=model_response, response_type="image_generation")  # type: ignore
        except OpenAIError as e:
            ## LOGGING
            logging_obj.post_call(
                input=prompt,
                api_key=api_key,
                additional_args={"complete_input_dict": data},
                original_response=str(e),
            )
            raise e
        except Exception as e:
            ## LOGGING
            logging_obj.post_call(
                input=prompt,
                api_key=api_key,
                additional_args={"complete_input_dict": data},
                original_response=str(e),
            )
            if hasattr(e, "status_code"):
                raise OpenAIError(
                    status_code=getattr(e, "status_code", 500), message=str(e)
                )
            else:
                raise OpenAIError(status_code=500, message=str(e))

    def audio_speech(
        self,
        model: str,
        input: str,
        voice: str,
        optional_params: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        organization: Optional[str],
        project: Optional[str],
        max_retries: int,
        timeout: Union[float, httpx.Timeout],
        aspeech: Optional[bool] = None,
        client=None,
        shared_session: Optional["ClientSession"] = None,
    ) -> HttpxBinaryResponseContent:
        if aspeech is not None and aspeech is True:
            return self.async_audio_speech(
                model=model,
                input=input,
                voice=voice,
                optional_params=optional_params,
                api_key=api_key,
                api_base=api_base,
                organization=organization,
                project=project,
                max_retries=max_retries,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )  # type: ignore

        openai_client = self._get_openai_client(
            is_async=False,
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            client=client,
            shared_session=shared_session,
        )

        response = cast(OpenAI, openai_client).audio.speech.create(
            model=model,
            voice=voice,  # type: ignore
            input=input,
            **optional_params,
        )
        return HttpxBinaryResponseContent(response=response.response)

    async def async_audio_speech(
        self,
        model: str,
        input: str,
        voice: str,
        optional_params: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        organization: Optional[str],
        project: Optional[str],
        max_retries: int,
        timeout: Union[float, httpx.Timeout],
        client=None,
        shared_session: Optional["ClientSession"] = None,
    ) -> HttpxBinaryResponseContent:
        openai_client = cast(
            AsyncOpenAI,
            self._get_openai_client(
                is_async=True,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                client=client,
                shared_session=shared_session,
            ),
        )

        response = await openai_client.audio.speech.create(
            model=model,
            voice=voice,  # type: ignore
            input=input,
            **optional_params,
        )
        return HttpxBinaryResponseContent(response=response.response)

class OpenAIFilesAPI(BaseLLM):
    """
    OpenAI methods to support for batches
    - create_file()
    - retrieve_file()
    - list_files()
    - delete_file()
    - file_content()
    - update_file()
    """

    def __init__(self) -> None:
        super().__init__()

    def get_openai_client(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        _is_async: bool = False,
    ) -> Optional[Union[OpenAI, AsyncOpenAI]]:
        received_args = locals()
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = None
        if client is None:
            data = {}
            for k, v in received_args.items():
                if k == "self" or k == "client" or k == "_is_async":
                    pass
                elif k == "api_base" and v is not None:
                    data["base_url"] = v
                elif v is not None:
                    data[k] = v
            if _is_async is True:
                openai_client = AsyncOpenAI(**data)
            else:
                openai_client = OpenAI(**data)  # type: ignore
        else:
            openai_client = client

        return openai_client

    async def acreate_file(
        self,
        create_file_data: CreateFileRequest,
        openai_client: AsyncOpenAI,
    ) -> OpenAIFileObject:
        response = await openai_client.files.create(**create_file_data)  # type: ignore[arg-type]
        return OpenAIFileObject(**response.model_dump())

    def create_file(
        self,
        _is_async: bool,
        create_file_data: CreateFileRequest,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ) -> Union[OpenAIFileObject, Coroutine[Any, Any, OpenAIFileObject]]:
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.acreate_file(  # type: ignore
                create_file_data=create_file_data, openai_client=openai_client
            )
        response = cast(OpenAI, openai_client).files.create(**create_file_data)  # type: ignore[arg-type]
        return OpenAIFileObject(**response.model_dump())

    async def afile_content(
        self,
        file_content_request: FileContentRequest,
        openai_client: AsyncOpenAI,
    ) -> HttpxBinaryResponseContent:
        response = await openai_client.files.content(**file_content_request)
        return HttpxBinaryResponseContent(response=response.response)

    def file_content(
        self,
        _is_async: bool,
        file_content_request: FileContentRequest,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ) -> Union[
        HttpxBinaryResponseContent, Coroutine[Any, Any, HttpxBinaryResponseContent]
    ]:
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.afile_content(  # type: ignore
                file_content_request=file_content_request,
                openai_client=openai_client,
            )
        response = cast(OpenAI, openai_client).files.content(**file_content_request)

        return HttpxBinaryResponseContent(response=response.response)

    async def afile_content_streaming(
        self,
        file_content_request: FileContentRequest,
        openai_client: AsyncOpenAI,
        chunk_size: int = 1024 * 1024,
    ) -> FileContentStreamingResult:
        response_cm = openai_client.files.with_streaming_response.content(
            **file_content_request
        )
        response = await response_cm.__aenter__()
        headers = dict(response.headers)

        async def _stream() -> AsyncIterator[bytes]:
            exc: Optional[BaseException] = None
            try:
                async for chunk in response.iter_bytes(chunk_size=chunk_size):
                    yield chunk
            except BaseException as e:
                exc = e
                raise
            finally:
                if exc is None:
                    await response_cm.__aexit__(None, None, None)
                else:
                    await response_cm.__aexit__(type(exc), exc, exc.__traceback__)

        return FileContentStreamingResult(stream_iterator=_stream(), headers=headers)

    def file_content_streaming(
        self,
        _is_async: bool,
        file_content_request: FileContentRequest,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        chunk_size: int = 1024 * 1024,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ) -> FileContentStreamingResult:
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.afile_content_streaming(  # type: ignore
                file_content_request=file_content_request,
                openai_client=openai_client,
                chunk_size=chunk_size,
            )

        response_cm = cast(OpenAI, openai_client).files.with_streaming_response.content(
            **file_content_request
        )
        response = response_cm.__enter__()
        headers = dict(response.headers)

        def _stream() -> Iterator[bytes]:
            exc: Optional[BaseException] = None
            try:
                yield from response.iter_bytes(chunk_size=chunk_size)
            except BaseException as e:
                exc = e
                raise
            finally:
                if exc is None:
                    response_cm.__exit__(None, None, None)
                else:
                    response_cm.__exit__(type(exc), exc, exc.__traceback__)

        return FileContentStreamingResult(stream_iterator=_stream(), headers=headers)

    async def aretrieve_file(
        self,
        file_id: str,
        openai_client: AsyncOpenAI,
    ) -> FileObject:
        response = await openai_client.files.retrieve(file_id=file_id)
        return response

    def retrieve_file(
        self,
        _is_async: bool,
        file_id: str,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.aretrieve_file(  # type: ignore
                file_id=file_id,
                openai_client=openai_client,
            )
        response = openai_client.files.retrieve(file_id=file_id)

        return response

    async def adelete_file(
        self,
        file_id: str,
        openai_client: AsyncOpenAI,
    ) -> FileDeleted:
        response = await openai_client.files.delete(file_id=file_id)
        return response

    def delete_file(
        self,
        _is_async: bool,
        file_id: str,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.adelete_file(  # type: ignore
                file_id=file_id,
                openai_client=openai_client,
            )
        response = openai_client.files.delete(file_id=file_id)

        return response

    async def alist_files(
        self,
        openai_client: AsyncOpenAI,
        purpose: Optional[str] = None,
    ):
        if isinstance(purpose, str):
            response = await openai_client.files.list(purpose=purpose)
        else:
            response = await openai_client.files.list()
        return response

    def list_files(
        self,
        _is_async: bool,
        api_base: str,
        api_key: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        purpose: Optional[str] = None,
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.alist_files(  # type: ignore
                purpose=purpose,
                openai_client=openai_client,
            )

        if isinstance(purpose, str):
            response = openai_client.files.list(purpose=purpose)
        else:
            response = openai_client.files.list()

        return response


class OpenAIBatchesAPI(BaseLLM):
    """
    OpenAI methods to support for batches
    - create_batch()
    - retrieve_batch()
    - cancel_batch()
    - list_batch()
    """

    def __init__(self) -> None:
        super().__init__()

    def get_openai_client(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
        _is_async: bool = False,
    ) -> Optional[Union[OpenAI, AsyncOpenAI]]:
        received_args = locals()
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = None
        if client is None:
            data = {}
            for k, v in received_args.items():
                if k == "self" or k == "client" or k == "_is_async":
                    pass
                elif k == "api_base" and v is not None:
                    data["base_url"] = v
                elif v is not None:
                    data[k] = v
            if _is_async is True:
                openai_client = AsyncOpenAI(**data)
            else:
                openai_client = OpenAI(**data)  # type: ignore
        else:
            openai_client = client

        return openai_client

    async def acreate_batch(
        self,
        create_batch_data: CreateBatchRequest,
        openai_client: AsyncOpenAI,
    ) -> LiteLLMBatch:
        response = await openai_client.batches.create(**create_batch_data)  # type: ignore[arg-type]
        return LiteLLMBatch(**response.model_dump())

    def create_batch(
        self,
        _is_async: bool,
        create_batch_data: CreateBatchRequest,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[Union[OpenAI, AsyncOpenAI]] = None,
    ) -> Union[LiteLLMBatch, Coroutine[Any, Any, LiteLLMBatch]]:
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.acreate_batch(  # type: ignore
                create_batch_data=create_batch_data, openai_client=openai_client
            )
        response = cast(OpenAI, openai_client).batches.create(**create_batch_data)  # type: ignore[arg-type]

        return LiteLLMBatch(**response.model_dump())

    async def aretrieve_batch(
        self,
        retrieve_batch_data: RetrieveBatchRequest,
        openai_client: AsyncOpenAI,
    ) -> LiteLLMBatch:
        log.debug("retrieving batch, args= %s", retrieve_batch_data)
        response = await openai_client.batches.retrieve(**retrieve_batch_data)  # type: ignore[arg-type]
        return LiteLLMBatch(**response.model_dump())

    def retrieve_batch(
        self,
        _is_async: bool,
        retrieve_batch_data: RetrieveBatchRequest,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.aretrieve_batch(  # type: ignore
                retrieve_batch_data=retrieve_batch_data, openai_client=openai_client
            )
        response = cast(OpenAI, openai_client).batches.retrieve(**retrieve_batch_data)  # type: ignore[arg-type]
        return LiteLLMBatch(**response.model_dump())

    async def acancel_batch(
        self,
        cancel_batch_data: CancelBatchRequest,
        openai_client: AsyncOpenAI,
    ) -> LiteLLMBatch:
        log.debug("async cancelling batch, args= %s", cancel_batch_data)
        response = await openai_client.batches.cancel(**cancel_batch_data)
        return LiteLLMBatch(**response.model_dump())

    def cancel_batch(
        self,
        _is_async: bool,
        cancel_batch_data: CancelBatchRequest,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.acancel_batch(  # type: ignore
                cancel_batch_data=cancel_batch_data, openai_client=openai_client
            )

        # At this point, openai_client is guaranteed to be a sync OpenAI client
        if not isinstance(openai_client, OpenAI):
            raise ValueError(
                "OpenAI client is not an instance of OpenAI. Make sure you passed a sync OpenAI client."
            )
        response = openai_client.batches.cancel(**cancel_batch_data)
        return LiteLLMBatch(**response.model_dump())

    async def alist_batches(
        self,
        openai_client: AsyncOpenAI,
        after: Optional[str] = None,
        limit: Optional[int] = None,
    ):
        log.debug("listing batches, after= %s, limit= %s", after, limit)
        response = await openai_client.batches.list(after=after, limit=limit)  # type: ignore
        return response

    def list_batches(
        self,
        _is_async: bool,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        after: Optional[str] = None,
        limit: Optional[int] = None,
        client: Optional[OpenAI] = None,
    ):
        openai_client: Optional[Union[OpenAI, AsyncOpenAI]] = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
            _is_async=_is_async,
        )
        if openai_client is None:
            raise ValueError(
                "OpenAI client is not initialized. Make sure api_key is passed or OPENAI_API_KEY is set in the environment."
            )

        if _is_async is True:
            if not isinstance(openai_client, AsyncOpenAI):
                raise ValueError(
                    "OpenAI client is not an instance of AsyncOpenAI. Make sure you passed an AsyncOpenAI client."
                )
            return self.alist_batches(  # type: ignore
                openai_client=openai_client, after=after, limit=limit
            )
        response = openai_client.batches.list(after=after, limit=limit)  # type: ignore
        return response


class OpenAIAssistantsAPI(BaseLLM):
    def __init__(self) -> None:
        super().__init__()

    def get_openai_client(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI] = None,
    ) -> OpenAI:
        received_args = locals()
        if client is None:
            data = {}
            for k, v in received_args.items():
                if k == "self" or k == "client":
                    pass
                elif k == "api_base" and v is not None:
                    data["base_url"] = v
                elif v is not None:
                    data[k] = v
            openai_client = OpenAI(**data)  # type: ignore
        else:
            openai_client = client

        return openai_client

    def async_get_openai_client(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI] = None,
    ) -> AsyncOpenAI:
        received_args = locals()
        if client is None:
            data = {}
            for k, v in received_args.items():
                if k == "self" or k == "client":
                    pass
                elif k == "api_base" and v is not None:
                    data["base_url"] = v
                elif v is not None:
                    data[k] = v
            openai_client = AsyncOpenAI(**data)  # type: ignore
        else:
            openai_client = client

        return openai_client

    ### ASSISTANTS ###

    async def async_get_assistants(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        order: Optional[str] = "desc",
        limit: Optional[int] = 20,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ) -> AsyncCursorPage[Assistant]:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )
        request_params = {
            "order": order,
            "limit": limit,
        }
        if before:
            request_params["before"] = before
        if after:
            request_params["after"] = after

        response = await openai_client.beta.assistants.list(**request_params)  # type: ignore

        return response

    # fmt: off

    @overload
    def get_assistants(
        self, 
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        aget_assistants: Literal[True], 
    ) -> Coroutine[None, None, AsyncCursorPage[Assistant]]:
        ...

    @overload
    def get_assistants(
        self, 
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI],
        aget_assistants: Optional[Literal[False]], 
    ) -> SyncCursorPage[Assistant]: 
        ...

    # fmt: on

    def get_assistants(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client=None,
        aget_assistants=None,
        order: Optional[str] = "desc",
        limit: Optional[int] = 20,
        before: Optional[str] = None,
        after: Optional[str] = None,
    ):
        if aget_assistants is not None and aget_assistants is True:
            return self.async_get_assistants(
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        request_params = {
            "order": order,
            "limit": limit,
        }

        if before:
            request_params["before"] = before
        if after:
            request_params["after"] = after

        response = openai_client.beta.assistants.list(**request_params)  # type: ignore

        return response

    # Create Assistant
    async def async_create_assistants(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        create_assistant_data: dict,
    ) -> Assistant:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = await openai_client.beta.assistants.create(**create_assistant_data)

        return response

    def create_assistants(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        create_assistant_data: dict,
        client=None,
        async_create_assistants=None,
    ):
        if async_create_assistants is not None and async_create_assistants is True:
            return self.async_create_assistants(
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
                create_assistant_data=create_assistant_data,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = openai_client.beta.assistants.create(**create_assistant_data)
        return response

    # Delete Assistant
    async def async_delete_assistant(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        assistant_id: str,
    ) -> AssistantDeleted:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = await openai_client.beta.assistants.delete(assistant_id=assistant_id)

        return response

    def delete_assistant(
        self,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        assistant_id: str,
        client=None,
        async_delete_assistants=None,
    ):
        if async_delete_assistants is not None and async_delete_assistants is True:
            return self.async_delete_assistant(
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
                assistant_id=assistant_id,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = openai_client.beta.assistants.delete(assistant_id=assistant_id)
        return response

    ### MESSAGES ###

    async def a_add_message(
        self,
        thread_id: str,
        message_data: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI] = None,
    ) -> OpenAIMessage:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        thread_message: OpenAIMessage = await openai_client.beta.threads.messages.create(  # type: ignore
            thread_id, **message_data  # type: ignore
        )

        response_obj: Optional[OpenAIMessage] = None
        if getattr(thread_message, "status", None) is None:
            thread_message.status = "completed"
            response_obj = OpenAIMessage(**thread_message.dict())
        else:
            response_obj = OpenAIMessage(**thread_message.dict())
        return response_obj

    # fmt: off

    @overload
    def add_message(
        self, 
        thread_id: str,
        message_data: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        a_add_message: Literal[True], 
    ) -> Coroutine[None, None, OpenAIMessage]:
        ...

    @overload
    def add_message(
        self, 
        thread_id: str,
        message_data: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI],
        a_add_message: Optional[Literal[False]], 
    ) -> OpenAIMessage: 
        ...

    # fmt: on

    def add_message(
        self,
        thread_id: str,
        message_data: dict,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client=None,
        a_add_message: Optional[bool] = None,
    ):
        if a_add_message is not None and a_add_message is True:
            return self.a_add_message(
                thread_id=thread_id,
                message_data=message_data,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        thread_message: OpenAIMessage = openai_client.beta.threads.messages.create(  # type: ignore
            thread_id, **message_data  # type: ignore
        )

        response_obj: Optional[OpenAIMessage] = None
        if getattr(thread_message, "status", None) is None:
            thread_message.status = "completed"
            response_obj = OpenAIMessage(**thread_message.dict())
        else:
            response_obj = OpenAIMessage(**thread_message.dict())
        return response_obj

    async def async_get_messages(
        self,
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI] = None,
    ) -> AsyncCursorPage[OpenAIMessage]:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = await openai_client.beta.threads.messages.list(thread_id=thread_id)

        return response

    # fmt: off

    @overload
    def get_messages(
        self, 
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        aget_messages: Literal[True], 
    ) -> Coroutine[None, None, AsyncCursorPage[OpenAIMessage]]:
        ...

    @overload
    def get_messages(
        self, 
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI],
        aget_messages: Optional[Literal[False]], 
    ) -> SyncCursorPage[OpenAIMessage]: 
        ...

    # fmt: on

    def get_messages(
        self,
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client=None,
        aget_messages=None,
    ):
        if aget_messages is not None and aget_messages is True:
            return self.async_get_messages(
                thread_id=thread_id,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = openai_client.beta.threads.messages.list(thread_id=thread_id)

        return response

    ### THREADS ###

    async def async_create_thread(
        self,
        metadata: Optional[dict],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        messages: Optional[Iterable[OpenAICreateThreadParamsMessage]],
    ) -> Thread:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        data = {}
        if messages is not None:
            data["messages"] = messages  # type: ignore
        if metadata is not None:
            data["metadata"] = metadata  # type: ignore

        message_thread = await openai_client.beta.threads.create(**data)  # type: ignore

        return Thread(**message_thread.dict())

    # fmt: off

    @overload
    def create_thread(
        self, 
        metadata: Optional[dict],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        messages: Optional[Iterable[OpenAICreateThreadParamsMessage]],
        client: Optional[AsyncOpenAI],
        acreate_thread: Literal[True], 
    ) -> Coroutine[None, None, Thread]:
        ...

    @overload
    def create_thread(
        self, 
        metadata: Optional[dict],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        messages: Optional[Iterable[OpenAICreateThreadParamsMessage]],
        client: Optional[OpenAI],
        acreate_thread: Optional[Literal[False]], 
    ) -> Thread: 
        ...

    # fmt: on

    def create_thread(
        self,
        metadata: Optional[dict],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        messages: Optional[Iterable[OpenAICreateThreadParamsMessage]],
        client=None,
        acreate_thread=None,
    ):
        if acreate_thread is not None and acreate_thread is True:
            return self.async_create_thread(
                metadata=metadata,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
                messages=messages,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        data = {}
        if messages is not None:
            data["messages"] = messages  # type: ignore
        if metadata is not None:
            data["metadata"] = metadata  # type: ignore

        message_thread = openai_client.beta.threads.create(**data)  # type: ignore

        return Thread(**message_thread.dict())

    async def async_get_thread(
        self,
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
    ) -> Thread:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = await openai_client.beta.threads.retrieve(thread_id=thread_id)

        return Thread(**response.dict())

    # fmt: off

    @overload
    def get_thread(
        self, 
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
        aget_thread: Literal[True], 
    ) -> Coroutine[None, None, Thread]:
        ...

    @overload
    def get_thread(
        self, 
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[OpenAI],
        aget_thread: Optional[Literal[False]], 
    ) -> Thread: 
        ...

    # fmt: on

    def get_thread(
        self,
        thread_id: str,
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client=None,
        aget_thread=None,
    ):
        if aget_thread is not None and aget_thread is True:
            return self.async_get_thread(
                thread_id=thread_id,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = openai_client.beta.threads.retrieve(thread_id=thread_id)

        return Thread(**response.dict())

    def delete_thread(self):
        pass

    ### RUNS ###

    async def arun_thread(
        self,
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        stream: Optional[bool],
        tools: Optional[Iterable[AssistantToolParam]],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client: Optional[AsyncOpenAI],
    ) -> Run:
        openai_client = self.async_get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        response = await openai_client.beta.threads.runs.create_and_poll(  # type: ignore
            thread_id=thread_id,
            assistant_id=assistant_id,
            additional_instructions=additional_instructions,
            instructions=instructions,
            metadata=metadata,
            model=model,
            tools=tools,
        )

        return response

    def async_run_thread_stream(
        self,
        client: AsyncOpenAI,
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        tools: Optional[Iterable[AssistantToolParam]],
        event_handler: Optional[AssistantEventHandler],
    ) -> AsyncAssistantStreamManager[AsyncAssistantEventHandler]:
        data: Dict[str, Any] = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "additional_instructions": additional_instructions,
            "instructions": instructions,
            "metadata": metadata,
            "model": model,
            "tools": tools,
        }
        if event_handler is not None:
            data["event_handler"] = event_handler
        return client.beta.threads.runs.stream(**data)  # type: ignore

    def run_thread_stream(
        self,
        client: OpenAI,
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        tools: Optional[Iterable[AssistantToolParam]],
        event_handler: Optional[AssistantEventHandler],
    ) -> AssistantStreamManager[AssistantEventHandler]:
        data: Dict[str, Any] = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "additional_instructions": additional_instructions,
            "instructions": instructions,
            "metadata": metadata,
            "model": model,
            "tools": tools,
        }
        if event_handler is not None:
            data["event_handler"] = event_handler
        return client.beta.threads.runs.stream(**data)  # type: ignore

    # fmt: off

    @overload
    def run_thread(
        self, 
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        stream: Optional[bool],
        tools: Optional[Iterable[AssistantToolParam]],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client,
        arun_thread: Literal[True], 
        event_handler: Optional[AssistantEventHandler],
    ) -> Coroutine[None, None, Run]:
        ...

    @overload
    def run_thread(
        self, 
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        stream: Optional[bool],
        tools: Optional[Iterable[AssistantToolParam]],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client,
        arun_thread: Optional[Literal[False]], 
        event_handler: Optional[AssistantEventHandler],
    ) -> Run: 
        ...

    # fmt: on

    def run_thread(
        self,
        thread_id: str,
        assistant_id: str,
        additional_instructions: Optional[str],
        instructions: Optional[str],
        metadata: Optional[Dict],
        model: Optional[str],
        stream: Optional[bool],
        tools: Optional[Iterable[AssistantToolParam]],
        api_key: Optional[str],
        api_base: Optional[str],
        timeout: Union[float, httpx.Timeout],
        max_retries: Optional[int],
        organization: Optional[str],
        client=None,
        arun_thread=None,
        event_handler: Optional[AssistantEventHandler] = None,
    ):
        if arun_thread is not None and arun_thread is True:
            if stream is not None and stream is True:
                _client = self.async_get_openai_client(
                    api_key=api_key,
                    api_base=api_base,
                    timeout=timeout,
                    max_retries=max_retries,
                    organization=organization,
                    client=client,
                )
                return self.async_run_thread_stream(
                    client=_client,
                    thread_id=thread_id,
                    assistant_id=assistant_id,
                    additional_instructions=additional_instructions,
                    instructions=instructions,
                    metadata=metadata,
                    model=model,
                    tools=tools,
                    event_handler=event_handler,
                )
            return self.arun_thread(
                thread_id=thread_id,
                assistant_id=assistant_id,
                additional_instructions=additional_instructions,
                instructions=instructions,
                metadata=metadata,
                model=model,
                stream=stream,
                tools=tools,
                api_key=api_key,
                api_base=api_base,
                timeout=timeout,
                max_retries=max_retries,
                organization=organization,
                client=client,
            )
        openai_client = self.get_openai_client(
            api_key=api_key,
            api_base=api_base,
            timeout=timeout,
            max_retries=max_retries,
            organization=organization,
            client=client,
        )

        if stream is not None and stream is True:
            return self.run_thread_stream(
                client=openai_client,
                thread_id=thread_id,
                assistant_id=assistant_id,
                additional_instructions=additional_instructions,
                instructions=instructions,
                metadata=metadata,
                model=model,
                tools=tools,
                event_handler=event_handler,
            )

        response = openai_client.beta.threads.runs.create_and_poll(  # type: ignore
            thread_id=thread_id,
            assistant_id=assistant_id,
            additional_instructions=additional_instructions,
            instructions=instructions,
            metadata=metadata,
            model=model,
            tools=tools,
        )

        return response
