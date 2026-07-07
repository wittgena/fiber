# bound.channel.bridge.convert.header
## @lineage: bound.channel.action.support.header
import httpx
from typing import Optional, Union
from bound.surface.legacy.types import OPENAI_RESPONSE_HEADERS

def process_response_headers(response_headers: Union[httpx.Headers, dict]) -> dict:
    openai_headers = {}
    processed_headers = {}
    additional_headers = {}

    for k, v in response_headers.items():
        if k in OPENAI_RESPONSE_HEADERS:
            openai_headers[k] = v
        if k.startswith("llm_provider-"):
            processed_headers[k] = v
        else:
            additional_headers["{}-{}".format("llm_provider", k)] = v

    additional_headers = {
        **openai_headers,
        **processed_headers,
        **additional_headers,
    }
    return additional_headers

def get_response_headers(_response_headers: Optional[dict] = None) -> dict:
    if _response_headers is None:
        return {}

    openai_headers = {}
    if "x-ratelimit-limit-requests" in _response_headers:
        openai_headers["x-ratelimit-limit-requests"] = _response_headers[
            "x-ratelimit-limit-requests"
        ]
    if "x-ratelimit-remaining-requests" in _response_headers:
        openai_headers["x-ratelimit-remaining-requests"] = _response_headers[
            "x-ratelimit-remaining-requests"
        ]
    if "x-ratelimit-limit-tokens" in _response_headers:
        openai_headers["x-ratelimit-limit-tokens"] = _response_headers[
            "x-ratelimit-limit-tokens"
        ]
    if "x-ratelimit-remaining-tokens" in _response_headers:
        openai_headers["x-ratelimit-remaining-tokens"] = _response_headers[
            "x-ratelimit-remaining-tokens"
        ]
    llm_provider_headers = _get_llm_provider_headers(_response_headers)
    return {**llm_provider_headers, **openai_headers}


def _get_llm_provider_headers(response_headers: dict) -> dict:
    llm_provider_headers = {}
    for k, v in response_headers.items():
        if "llm_provider" not in k:
            _key = "{}-{}".format("llm_provider", k)
            llm_provider_headers[_key] = v
        else:
            llm_provider_headers[k] = v
    return llm_provider_headers
