# bound.transport.response.api.handler
## @lineage: bound.transport.channel.response.api.handler
## @lineage: bound.channel.action.api.handler
import json
import ssl
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Coroutine,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)
import httpx
from aiohttp import ClientSession

from anchor.provider.param.response import DeleteResponseResult
from anchor.provider.param.legacy import GenericLiteLLMParams

from bound.transport.http import AsyncHTTPHandler, HTTPHandler, _get_httpx_client, get_async_httpx_client
from bound.surface.legacy.config.response import BaseResponsesAPIConfig
from bound.surface.legacy.provider import ProviderTypes
from bound.surface.legacy.config.resolver import config
from bound.surface.legacy.types import CallTypes
from bound.surface.legacy.openai.types import ResponseInputParam, ResponsesAPIResponse

from bound.transport.stream.iterator import ResponseStreamIterator, MockResponsesAPIStreamingIterator, ResponsesAPIStreamingIterator, SyncResponsesAPIStreamingIterator
from bound.watcher.delegator import LogDelegator

from watcher.plane.emitter import get_emitter 

log = get_emitter("handler.api")

class ResponseApiHandler:
    def response_api_handler(
        self,
        model: str,
        input: Union[str, ResponseInputParam],
        responses_api_provider_config: BaseResponsesAPIConfig,
        response_api_optional_request_params: Dict,
        custom_llm_provider: str,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        fake_stream: bool = False,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[
        ResponsesAPIResponse,
        ResponseStreamIterator,
        Coroutine[
            Any, Any, Union[ResponsesAPIResponse, ResponseStreamIterator]
        ],
    ]:
        if _is_async:
            # Return the async coroutine if called with _is_async=True
            return self.async_response_api_handler(
                model=model,
                input=input,
                responses_api_provider_config=responses_api_provider_config,
                response_api_optional_request_params=response_api_optional_request_params,
                custom_llm_provider=custom_llm_provider,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                extra_headers=extra_headers,
                extra_body=extra_body,
                timeout=timeout,
                client=client if isinstance(client, AsyncHTTPHandler) else None,
                fake_stream=fake_stream,
                litellm_metadata=litellm_metadata,
                shared_session=shared_session,
            )

        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=response_api_optional_request_params.get("extra_headers", {}) or {},
            model=model,
            litellm_params=litellm_params,
        )

        if extra_headers:
            headers.update(extra_headers)

        # Check if streaming is requested
        stream = response_api_optional_request_params.get("stream", False)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        data = responses_api_provider_config.transform_responses_api_request(
            model=model,
            input=input,
            response_api_optional_request_params=response_api_optional_request_params,
            litellm_params=litellm_params,
            headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        if extra_body:
            data.update(extra_body)

        # Preserve the OpenAI-style request context (not sent to the provider) for streaming
        # hooks/metadata; the streaming iterator now consumes this to run deployment hooks
        # with the same info as chat, including litellm_params.
        request_context: Dict[str, Any] = {"input": input}
        try:
            request_context.update(response_api_optional_request_params)
        except Exception:
            pass
        # Needed by streaming callbacks/metadata helpers to reconstruct api_base/model_id
        # but never included in the outbound provider payload.
        request_context["litellm_params"] = dict(litellm_params)

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            if stream:
                # For streaming, use stream=True in the request
                if fake_stream is True:
                    stream, data = self._prepare_fake_stream_request(
                        stream=stream,
                        data=data,
                        fake_stream=fake_stream,
                    )

                response = sync_httpx_client.post(
                    url=api_base,
                    headers=headers,
                    json=data,
                    timeout=timeout
                    or float(response_api_optional_request_params.get("timeout", 0)),
                    stream=stream,
                )
                if fake_stream is True:
                    return MockResponsesAPIStreamingIterator(
                        response=response,
                        model=model,
                        logging_obj=logging_obj,
                        responses_api_provider_config=responses_api_provider_config,
                        litellm_metadata=litellm_metadata,
                        custom_llm_provider=custom_llm_provider,
                        request_data=request_context,
                        call_type=CallTypes.responses.value,
                    )

                return SyncResponsesAPIStreamingIterator(
                    response=response,
                    model=model,
                    logging_obj=logging_obj,
                    responses_api_provider_config=responses_api_provider_config,
                    litellm_metadata=litellm_metadata,
                    custom_llm_provider=custom_llm_provider,
                    request_data=request_context,
                    call_type=CallTypes.responses.value,
                )
            else:
                # For non-streaming requests
                response = sync_httpx_client.post(
                    url=api_base,
                    headers=headers,
                    json=data,
                    timeout=timeout
                    or float(response_api_optional_request_params.get("timeout", 0)),
                )
        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_response_api_response(
            model=model,
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_response_api_handler(
        self,
        model: str,
        input: Union[str, ResponseInputParam],
        responses_api_provider_config: BaseResponsesAPIConfig,
        response_api_optional_request_params: Dict,
        custom_llm_provider: str,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        fake_stream: bool = False,
        litellm_metadata: Optional[Dict[str, Any]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[ResponsesAPIResponse, ResponseStreamIterator]:
        """
        Async version of the responses API handler.
        Uses async HTTP client to make requests.
        """
        if client is None or not isinstance(client, AsyncHTTPHandler):
            log.debug(
                f"Creating HTTP client for responses API with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=response_api_optional_request_params.get("extra_headers", {}) or {},
            model=model,
            litellm_params=litellm_params,
        )

        if extra_headers:
            headers.update(extra_headers)

        # Check if streaming is requested
        stream = response_api_optional_request_params.get("stream", False)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        data = responses_api_provider_config.transform_responses_api_request(
            model=model,
            input=input,
            response_api_optional_request_params=response_api_optional_request_params,
            litellm_params=litellm_params,
            headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        if extra_body:
            data.update(extra_body)

        # Preserve the OpenAI-style request context (not sent to the provider) for streaming
        # hooks/metadata; the streaming iterator now consumes this to run deployment hooks
        # with the same info as chat, including litellm_params.
        request_context: Dict[str, Any] = {"input": input}
        try:
            request_context.update(response_api_optional_request_params)
        except Exception:
            pass
        # Needed by streaming callbacks/metadata helpers to reconstruct api_base/model_id
        # but never included in the outbound provider payload.
        request_context["litellm_params"] = dict(litellm_params)

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            if stream:
                # For streaming, we need to use stream=True in the request
                if fake_stream is True:
                    stream, data = self._prepare_fake_stream_request(
                        stream=stream,
                        data=data,
                        fake_stream=fake_stream,
                    )

                response = await async_httpx_client.post(
                    url=api_base,
                    headers=headers,
                    json=data,
                    timeout=timeout
                    or float(response_api_optional_request_params.get("timeout", 0)),
                    stream=stream,
                )

                if fake_stream is True:
                    return MockResponsesAPIStreamingIterator(
                        response=response,
                        model=model,
                        logging_obj=logging_obj,
                        responses_api_provider_config=responses_api_provider_config,
                        litellm_metadata=litellm_metadata,
                        custom_llm_provider=custom_llm_provider,
                        request_data=request_context,
                        call_type=CallTypes.responses.value,
                    )

                # Return the streaming iterator
                return ResponsesAPIStreamingIterator(
                    response=response,
                    model=model,
                    logging_obj=logging_obj,
                    responses_api_provider_config=responses_api_provider_config,
                    litellm_metadata=litellm_metadata,
                    custom_llm_provider=custom_llm_provider,
                    request_data=request_context,
                    call_type=CallTypes.responses.value,
                )
            else:
                # For non-streaming, proceed as before
                response = await async_httpx_client.post(
                    url=api_base,
                    headers=headers,
                    json=data,
                    timeout=timeout
                    or float(response_api_optional_request_params.get("timeout", 0)),
                )

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_response_api_response(
            model=model,
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_delete_response_api_handler(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> DeleteResponseResult:
        """
        Async version of the delete response API handler.
        Uses async HTTP client to make requests.
        """
        if client is None or not isinstance(client, AsyncHTTPHandler):
            log.debug(
                f"Creating HTTP client for delete_response with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_delete_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        headers.setdefault("Content-Type", "application/json")

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        delete_kwargs: Dict[str, Any] = {
            "url": url,
            "headers": headers,
            "timeout": timeout,
        }
        if data:
            delete_kwargs["json"] = data

        try:
            response = await async_httpx_client.delete(**delete_kwargs)

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_delete_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    def delete_response_api_handler(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[DeleteResponseResult, Coroutine[Any, Any, DeleteResponseResult]]:
        """
        Async version of the responses API handler.
        Uses async HTTP client to make requests.
        """
        if _is_async:
            return self.async_delete_response_api_handler(
                response_id=response_id,
                responses_api_provider_config=responses_api_provider_config,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                custom_llm_provider=custom_llm_provider,
                extra_headers=extra_headers,
                extra_body=extra_body,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )
        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_delete_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        headers.setdefault("Content-Type", "application/json")

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        delete_kwargs: Dict[str, Any] = {
            "url": url,
            "headers": headers,
            "timeout": timeout,
        }
        if data:
            delete_kwargs["json"] = data

        try:
            response = sync_httpx_client.delete(**delete_kwargs)

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_delete_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    def cancel_response_api_handler(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        """
        Async version of the responses API handler.
        Uses async HTTP client to make requests.
        """
        if _is_async:
            return self.async_cancel_response_api_handler(
                response_id=response_id,
                responses_api_provider_config=responses_api_provider_config,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                custom_llm_provider=custom_llm_provider,
                extra_headers=extra_headers,
                extra_body=extra_body,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )
        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_cancel_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        ## LOGGING
        logging_obj.pre_call(
            input=response_id,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": url,
                "headers": headers,
            },
        )

        try:
            response = sync_httpx_client.post(
                url=url, headers=headers, json=data, timeout=timeout
            )

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_cancel_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_cancel_response_api_handler(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> ResponsesAPIResponse:
        """
        Async version of the cancel response API handler.
        Uses async HTTP client to make requests.
        """
        if client is None or not isinstance(client, AsyncHTTPHandler):
            log.debug(
                f"Creating HTTP client for cancel_response with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_cancel_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        ## LOGGING
        logging_obj.pre_call(
            input=response_id,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": url,
                "headers": headers,
            },
        )

        try:
            response = await async_httpx_client.post(
                url=url, headers=headers, json=data, timeout=timeout
            )

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_cancel_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    def compact_response_api_handler(
        self,
        model: str,
        input: Union[str, "ResponseInputParam"],
        responses_api_provider_config: BaseResponsesAPIConfig,
        response_api_optional_request_params: Dict,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        """
        Handler for the compact responses API.
        """
        if _is_async:
            return self.async_compact_response_api_handler(
                model=model,
                input=input,
                responses_api_provider_config=responses_api_provider_config,
                response_api_optional_request_params=response_api_optional_request_params,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                custom_llm_provider=custom_llm_provider,
                extra_headers=extra_headers,
                extra_body=extra_body,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )
        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model=model, litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        (
            url,
            data,
        ) = responses_api_provider_config.transform_compact_response_api_request(
            model=model,
            input=input,
            response_api_optional_request_params=response_api_optional_request_params,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": url,
                "headers": headers,
            },
        )

        try:
            response = sync_httpx_client.post(
                url=url, headers=headers, json=data, timeout=timeout
            )

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_compact_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_compact_response_api_handler(
        self,
        model: str,
        input: Union[str, "ResponseInputParam"],
        responses_api_provider_config: BaseResponsesAPIConfig,
        response_api_optional_request_params: Dict,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str],
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> ResponsesAPIResponse:
        """
        Async version of the compact response API handler.
        """
        if client is None or not isinstance(client, AsyncHTTPHandler):
            log.debug(
                f"Creating HTTP client for compact_response with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model=model, litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        (
            url,
            data,
        ) = responses_api_provider_config.transform_compact_response_api_request(
            model=model,
            input=input,
            response_api_optional_request_params=response_api_optional_request_params,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )
        data = BaseResponsesAPIConfig.normalize_responses_api_request_dict(data)

        ## LOGGING
        logging_obj.pre_call(
            input=input,
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": url,
                "headers": headers,
            },
        )

        try:
            response = await async_httpx_client.post(
                url=url, headers=headers, json=data, timeout=timeout
            )

        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_compact_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    def get_responses(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str] = None,
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[ResponsesAPIResponse, Coroutine[Any, Any, ResponsesAPIResponse]]:
        """
        Get a response by ID
        Uses GET /v1/responses/{response_id} endpoint in the responses API
        """
        if _is_async:
            return self.async_get_responses(
                response_id=response_id,
                responses_api_provider_config=responses_api_provider_config,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                custom_llm_provider=custom_llm_provider,
                extra_headers=extra_headers,
                extra_body=extra_body,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )

        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_get_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        ## LOGGING
        logging_obj.pre_call(
            input="",
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            response = sync_httpx_client.get(url=url, headers=headers, params=data)
        except Exception as e:
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_get_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_get_responses(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str] = None,
        extra_headers: Optional[Dict[str, Any]] = None,
        extra_body: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> ResponsesAPIResponse:
        """
        Async version of get_responses
        """
        if client is None or not isinstance(client, AsyncHTTPHandler):
            verbose_logger.debug(
                f"Creating HTTP client for get_responses with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, data = responses_api_provider_config.transform_get_response_api_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
        )

        ## LOGGING
        logging_obj.pre_call(
            input="",
            api_key="",
            additional_args={
                "complete_input_dict": data,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            response = await async_httpx_client.get(
                url=url, headers=headers, params=data
            )

        except Exception as e:
            verbose_logger.exception(f"Error retrieving response: {e}")
            raise self._handle_error(
                e=e,
                provider_config=responses_api_provider_config,
            )

        return responses_api_provider_config.transform_get_response_api_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    #####################################################################
    ################ LIST RESPONSES INPUT ITEMS HANDLER ###########################
    #####################################################################
    def list_responses_input_items(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include: Optional[List[str]] = None,
        limit: int = 20,
        order: Literal["asc", "desc"] = "desc",
        extra_headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        _is_async: bool = False,
        shared_session: Optional["ClientSession"] = None,
    ) -> Union[Dict, Coroutine[Any, Any, Dict]]:
        if _is_async:
            return self.async_list_responses_input_items(
                response_id=response_id,
                responses_api_provider_config=responses_api_provider_config,
                litellm_params=litellm_params,
                logging_obj=logging_obj,
                custom_llm_provider=custom_llm_provider,
                after=after,
                before=before,
                include=include,
                limit=limit,
                order=order,
                extra_headers=extra_headers,
                timeout=timeout,
                client=client,
                shared_session=shared_session,
            )

        if client is None or not isinstance(client, HTTPHandler):
            sync_httpx_client = _get_httpx_client(
                params={"ssl_verify": litellm_params.get("ssl_verify", None)}
            )
        else:
            sync_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, params = responses_api_provider_config.transform_list_input_items_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
            after=after,
            before=before,
            include=include,
            limit=limit,
            order=order,
        )

        logging_obj.pre_call(
            input="",
            api_key="",
            additional_args={
                "complete_input_dict": params,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            response = sync_httpx_client.get(url=url, headers=headers, params=params)
        except Exception as e:
            raise self._handle_error(e=e, provider_config=responses_api_provider_config)

        return responses_api_provider_config.transform_list_input_items_response(
            raw_response=response,
            logging_obj=logging_obj,
        )

    async def async_list_responses_input_items(
        self,
        response_id: str,
        responses_api_provider_config: BaseResponsesAPIConfig,
        litellm_params: GenericLiteLLMParams,
        logging_obj: LogDelegator,
        custom_llm_provider: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        include: Optional[List[str]] = None,
        limit: int = 20,
        order: Literal["asc", "desc"] = "desc",
        extra_headers: Optional[Dict[str, Any]] = None,
        timeout: Optional[Union[float, httpx.Timeout]] = None,
        client: Optional[Union[HTTPHandler, AsyncHTTPHandler]] = None,
        shared_session: Optional["ClientSession"] = None,
    ) -> Dict:
        if client is None or not isinstance(client, AsyncHTTPHandler):
            verbose_logger.debug(
                f"Creating HTTP client for list_input_items with shared_session: {id(shared_session) if shared_session else None}"
            )
            async_httpx_client = get_async_httpx_client(
                llm_provider=ProviderTypes(custom_llm_provider),
                params={"ssl_verify": litellm_params.get("ssl_verify", None)},
                shared_session=shared_session,
            )
        else:
            async_httpx_client = client

        headers = responses_api_provider_config.validate_environment(
            headers=extra_headers or {}, model="None", litellm_params=litellm_params
        )

        if extra_headers:
            headers.update(extra_headers)

        api_base = responses_api_provider_config.get_complete_url(
            api_base=litellm_params.api_base,
            litellm_params=dict(litellm_params),
        )

        url, params = responses_api_provider_config.transform_list_input_items_request(
            response_id=response_id,
            api_base=api_base,
            litellm_params=litellm_params,
            headers=headers,
            after=after,
            before=before,
            include=include,
            limit=limit,
            order=order,
        )

        logging_obj.pre_call(
            input="",
            api_key="",
            additional_args={
                "complete_input_dict": params,
                "api_base": api_base,
                "headers": headers,
            },
        )

        try:
            response = await async_httpx_client.get(
                url=url, headers=headers, params=params
            )
        except Exception as e:
            raise self._handle_error(e=e, provider_config=responses_api_provider_config)

        return responses_api_provider_config.transform_list_input_items_response(
            raw_response=response,
            logging_obj=logging_obj,
        )