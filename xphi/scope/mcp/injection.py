# xphi.scope.mcp.injection
## @lineage: server.mcp.utilities.context_injection
"""Context injection utilities for MCPServer."""
from __future__ import annotations
import inspect
import typing
from collections.abc import Callable
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ator.mcp.server.context import Context
    ContextType = Context
else:
    ContextType = Any

def find_context_parameter(fn: Callable[..., Any]) -> str | None:
    try:
        hints = typing.get_type_hints(fn)
    except Exception:  # pragma: lax no cover
        # If we can't resolve type hints, we can't find the context parameter
        return None

    # Check each parameter's type hint
    for param_name, annotation in hints.items():
        # Handle direct Context type
        if inspect.isclass(annotation) and issubclass(annotation, ContextType):
            return param_name

        # Handle generic types like Optional[Context]
        origin = typing.get_origin(annotation)
        if origin is not None:
            args = typing.get_args(annotation)
            for arg in args:
                if inspect.isclass(arg) and issubclass(arg, Context):
                    return param_name

    return None


def inject_context(
    fn: Callable[..., Any],
    kwargs: dict[str, Any],
    context: Any | None,
    context_kwarg: str | None,
) -> dict[str, Any]:
    """Inject context into function kwargs if needed.

    Args:
        fn: The function that will be called
        kwargs: The current keyword arguments
        context: The context object to inject (if any)
        context_kwarg: The name of the parameter to inject into

    Returns:
        Updated kwargs with context injected if applicable
    """
    if context_kwarg is not None and context is not None:
        return {**kwargs, context_kwarg: context}
    return kwargs
