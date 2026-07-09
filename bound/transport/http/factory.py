# bound.transport.http.factory
import os
import ssl
import time
import asyncio
import warnings
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

import httpx
from aiohttp import ClientSession

from bound.surface.exception import Timeout
from bound.surface.legacy.provider import ProviderTypes
from bound.transport.http.base import _DEFAULT_TIMEOUT, _HTTPX_CLIENT_CACHE, httpxSpecialProvider
from bound.transport.http.client import AsyncHTTPClient
from bound.transport.http.sync import HTTPClient
from watcher.plane.emitter import get_emitter

log = get_emitter("http.factory")

def get_client(
    is_async: bool,
    llm_provider: Optional[Union[ProviderTypes, httpxSpecialProvider]] = None,
    params: Optional[dict] = None,
    shared_session: Optional["ClientSession"] = None,
) -> Union[AsyncHTTPClient, HTTPClient]:
    params_key = ""
    handler_params = {}

    ## Parameter refinement and cache key generation
    if params is not None:
        # Dynamically determine which parameter keys to ignore based on the execution context
        ignore_keys = {"shared_session"}
        if not is_async:
            ignore_keys.add("disable_aiohttp_transport")

        handler_params = {k: v for k, v in params.items() if k not in ignore_keys}

        # Ensure deterministic cache key generation by sorting the parameters alphanumerically
        for key, value in sorted(handler_params.items()):
            params_key += f"{key}_{value}"
    else:
        # Apply default configuration when no explicit parameters are provided
        handler_params = {"timeout": _DEFAULT_TIMEOUT}

    ## Cache key assembly
    # Append appropriate prefixes and provider-specific suffixes based on the client type
    prefix = "async_httpx_client" if is_async else "httpx_client"
    suffix = f"_{llm_provider}" if (is_async and llm_provider) else ""
    cache_key = f"{prefix}_{params_key}{suffix}"

    ## Cache hit resolution
    if cache_key in _HTTPX_CLIENT_CACHE:
        return _HTTPX_CLIENT_CACHE[cache_key]

    ## Cache miss resolution (Instance creation)
    if is_async:
        # Inject the shared aiohttp session exclusively for asynchronous clients
        if shared_session is not None:
            handler_params["shared_session"] = shared_session
        client = AsyncHTTPClient(**handler_params)
    else:
        client = HTTPClient(**handler_params)

    ## State persistence
    _HTTPX_CLIENT_CACHE[cache_key] = client
    return client

## Legacy Compatibility Wrappers
def get_async_client(
    llm_provider: Union[ProviderTypes, httpxSpecialProvider],
    params: Optional[dict] = None,
    shared_session: Optional["ClientSession"] = None,
) -> AsyncHTTPClient:
    """Instantiates or retrieves an asynchronous HTTP client."""
    return get_client(is_async=True, llm_provider=llm_provider, params=params, shared_session=shared_session)

def get_http_client(params: Optional[dict] = None) -> HTTPClient:
    """Instantiates or retrieves a synchronous HTTP client."""
    return get_client(is_async=False, params=params)