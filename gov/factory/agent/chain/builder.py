# gov.factory.agent.chain.builder
## @lineage: atoa.factory.agent.chain.builder
## @lineage: atoa.agent.factory.chain.builder
## @lineage: agent.topos.factory.chain.builder
## @lineage: atoa.topos.factory.chain.builder
## @lineage: gov.sandbox.engine.factory.chain.builder
## @lineage: sandbox.factory.chain.builder
## @lineage: gov.engine.chain.builder
from typing import Callable
from gov.action.tool.terminal import TerminalObservation
from mesh.engine.executor.context import (
    ExecutionContext,
    ExecutionEngine,
    ExecutionMiddleware,
)

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)


class ChainBuilder(ExecutionEngine):
    """
    Orchestrator that chains multiple middlewares before reaching the low-level base engine.
    """

    def __init__(self, base_engine: ExecutionEngine, middlewares: list[ExecutionMiddleware]):
        self.base_engine = base_engine
        self.middlewares = middlewares

    def execute(self, context: ExecutionContext) -> TerminalObservation:
        """Builds and executes the chain of responsibility."""

        def _build_chain(index: int) -> Callable[[ExecutionContext], TerminalObservation]:
            if index < len(self.middlewares):
                middleware = self.middlewares[index]
                # Pass the current context and the next step in the chain
                return lambda ctx: middleware.process(ctx, _build_chain(index + 1))
            else:
                # Terminal node: The actual low-level execution engine (e.g., PollingExecutionEngine)
                return lambda ctx: self.base_engine.execute(ctx)

        # Initiate the recursive chain from the first middleware
        first_step = _build_chain(0)
        
        try:
            return first_step(context)
        except Exception as e:
            log.error(f"Pipeline execution failed: {str(e)}")
            # Fallback observation if the pipeline crashes unexpectedly
            return TerminalObservation.from_text(
                text=f"Internal pipeline error during execution: {str(e)}",
                command=context.action.command,
                is_error=True
            )