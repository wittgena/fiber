# bound.channel.action.support.header
## @lineage: bound.channel.client.action.support.header
## @lineage: anchor.channel.client.action.support.header
## @lineage: anchor.channel.action.support.header
from typing import Optional

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
