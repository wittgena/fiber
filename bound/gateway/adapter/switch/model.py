# bound.gateway.adapter.switch.model
## @lineage: gateway.adapter.switch.model
## @lineage: eco.switch.model
## @lineage: adapter.switch.model
## @lineage: bound.adapter.switch.model
"""
@desc: Exposes the primary execution endpoints, dynamically switching between LiteLLM and the internal Brane router.
@flow: Caller -> anchor.switch.entry -> (litellm | bound.channel)
@tag: facade, execution-boundary, dynamic-routing
"""
import os
from bound.gateway.adapter.switch.params import LITELLM_CONVERT_SWITCH

if LITELLM_CONVERT_SWITCH:
    try:
        from litellm import get_supported_openai_params
        from litellm.utils import create_pretrained_tokenizer, supports_vision, token_counter
    except ImportError:
        LITELLM_CONVERT_SWITCH = False

if not LITELLM_CONVERT_SWITCH:
    try:
        from bound.resolver.model.support import get_supported_openai_params, supports_vision
        from bound.gateway.token.splitter import create_pretrained_tokenizer
        from bound.gateway.token.counter import token_counter
    except ImportError as e:
        raise ImportError(
            f"Failed to load execution boundaries from internal bound modules. "
            f"Check your Brane topology mapping. Error: {e}"
        )