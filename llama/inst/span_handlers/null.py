# llama.inst.span_handlers.null
## @lineage: xor.loop.inst.span_handlers.null
## @lineage: xphi.loop.inst.span_handlers.null
## @lineage: bound.adapter.llama.instrumentation.span_handlers.null
## @lineage: bound.adapter.instrumentation.span_handlers.null
## @lineage: anchor.adapter.instrumentation.span_handlers.null
## @lineage: bridge.llama.core.instrumentation.span_handlers.null
import inspect
from typing import Any, Dict, Optional
from llama.inst.span.base import BaseSpan
from llama.inst.span_handlers.base import BaseSpanHandler


class NullSpanHandler(BaseSpanHandler[BaseSpan]):
    @classmethod
    def class_name(cls) -> str:
        """Class name."""
        return "NullSpanHandler"

    def span_enter(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        parent_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Logic for entering a span."""
        return

    def span_exit(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        result: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Logic for exiting a span."""
        return

    def new_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        parent_span_id: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Create a span."""
        return

    def prepare_to_exit_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        result: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """Logic for exiting a span."""
        return

    def prepare_to_drop_span(
        self,
        id_: str,
        bound_args: inspect.BoundArguments,
        instance: Optional[Any] = None,
        err: Optional[BaseException] = None,
        **kwargs: Any,
    ) -> None:
        """Logic for droppping a span."""
        return
