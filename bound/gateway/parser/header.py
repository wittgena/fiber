# bound.gateway.parser.header
## @lineage: bound.gateway.header
## @lineage: bound.gateway.adapter.header
import httpx
from typing import Optional, Union
from eco.legacy.types import OPENAI_RESPONSE_HEADERS

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
    additional_headers = {**openai_headers, **processed_headers, **additional_headers,}
    return additional_headers

def get_response_headers(response_headers: Optional[dict] = None) -> dict:
    if response_headers is None:
        return {}

    headers = {}
    if "x-ratelimit-limit-requests" in response_headers:
        headers["x-ratelimit-limit-requests"] = response_headers["x-ratelimit-limit-requests"]
    if "x-ratelimit-remaining-requests" in response_headers:
        headers["x-ratelimit-remaining-requests"] = response_headers["x-ratelimit-remaining-requests"]
    if "x-ratelimit-limit-tokens" in response_headers:
        headers["x-ratelimit-limit-tokens"] = response_headers["x-ratelimit-limit-tokens"]
    if "x-ratelimit-remaining-tokens" in response_headers:
        headers["x-ratelimit-remaining-tokens"] = response_headers["x-ratelimit-remaining-tokens"]

    llm_provider_headers = {}
    for k, v in response_headers.items():
        if "llm_provider" not in k:
            _key = "{}-{}".format("llm_provider", k)
            llm_provider_headers[_key] = v
        else:
            llm_provider_headers[k] = v
    return {**llm_provider_headers, **headers}