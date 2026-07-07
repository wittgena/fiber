# bound.surface.switch.entry
## @lineage: anchor.surface.switch.entry
## @lineage: bound.channel.switch.entry
## @lineage: bound.channel.compat.switch.entry
## @lineage: anchor.channel.compat.switch.entry
## @lineage: anchor.channel.switch.entry
## @lineage: anchor.switch.entry
"""
@phase: Membrane Execution Entry
@desc: Exposes the primary execution endpoints, dynamically switching between LiteLLM and the internal Brane router.
@flow: Caller -> anchor.switch.entry -> (litellm | bound.channel)
@tag: facade, execution-boundary, dynamic-routing
"""
import os
from bound.surface.switch.params import LITELLM_CONVERT_SWITCH

if LITELLM_CONVERT_SWITCH:
    try:
        from litellm import completion, acompletion
        from litellm import embedding, aembedding
        from litellm.responses.main import responses
        from litellm.responses.main import aresponses 
    except ImportError:
        LITELLM_CONVERT_SWITCH = False

if not LITELLM_CONVERT_SWITCH:
    try:
        from bound.channel.action.completion import completion, acompletion
        from bound.channel.action.embedding import embedding, aembedding
        from bound.channel.action.api.response import responses
        from bound.channel.action.api.aresponse import aresponses
    except ImportError as e:
        raise ImportError(
            f"Failed to load execution boundaries from internal bound modules. "
            f"Check your Brane topology mapping. Error: {e}"
        )