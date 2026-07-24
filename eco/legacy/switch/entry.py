# eco.legacy.switch.entry
import os
from eco.legacy.switch.params import LITELLM_CONVERT_SWITCH

if LITELLM_CONVERT_SWITCH:
    try:
        from litellm import completion, acompletion
        from litellm import embedding, aembedding
        from litellm.responses.main import responses
        from litellm.responses.main import aresponses
        from litellm.cost_calculator import completion_cost

    except ImportError:
        LITELLM_CONVERT_SWITCH = False

if not LITELLM_CONVERT_SWITCH:
    try:
        from eco.legacy.action.completion import completion, acompletion
        from eco.legacy.action.embedding import embedding, aembedding
        from eco.legacy.cost.calculator import completion_cost
    except ImportError as e:
        raise ImportError(
            f"Failed to load execution boundaries from internal bound modules. "
            f"Check your Brane topology mapping. Error: {e}"
        )