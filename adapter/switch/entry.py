# adapter.switch.entry
## @lineage: bound.adapter.switch.entry
## @lineage: anchor.bind.switch.entry
"""
@desc: Exposes the primary execution endpoints, dynamically switching between LiteLLM and the internal Brane router.
@flow: Caller -> anchor.switch.entry -> (litellm | bound.channel)
@tag: facade, execution-boundary, dynamic-routing
"""
import os
from adapter.switch.params import LITELLM_CONVERT_SWITCH

if LITELLM_CONVERT_SWITCH:
    try:
        from litellm import completion, acompletion
        from litellm import embedding, aembedding
        from litellm.responses.main import responses
        from litellm.responses.main import aresponses
        from litellm.responses.streaming_iterator import SyncResponsesAPIStreamingIterator
        from litellm.cost_calculator import completion_cost

    except ImportError:
        LITELLM_CONVERT_SWITCH = False

if not LITELLM_CONVERT_SWITCH:
    try:
        from adapter.legacy.action.completion import completion, acompletion
        from adapter.legacy.action.embedding import embedding, aembedding
        from adapter.legacy.action.response import responses
        from adapter.legacy.action.response import aresponses
        from bound.transport.stream.iterator.response import SyncResponsesAPIStreamingIterator
        from adapter.legacy.cost.calculator import completion_cost
    except ImportError as e:
        raise ImportError(
            f"Failed to load execution boundaries from internal bound modules. "
            f"Check your Brane topology mapping. Error: {e}"
        )