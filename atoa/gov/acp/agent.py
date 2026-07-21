# atoa.gov.acp.agent
## @lineage: gov.acp.agent
## @lineage: gov.policy.acp.agent
## @lineage: gov.protocol.acp.agent
from __future__ import annotations
import os
from collections.abc import Generator
from typing import TYPE_CHECKING, Any
from pydantic import Field

from watcher.plane.observer.span import unified_flow_span

from atoa.disc.ator import Ator
from atoa.disc.event.llm.system import SystemPromptEvent
from eco.call.disc.tool import Tool

from atoa.driver.tensor import Driver
from eco.call.action.message import TextContent
from atoa.gov.acp.client import ACPClient
from atoa.gov.acp.step import ACPTrajectory
from atoa.gov.acp.connection import ACPConnectionManifold

from atoa.context.gov.command import UpdateAgentState

if TYPE_CHECKING:
    from atoa.call.types import ConversationCallbackType
    from atoa.context.gov.protocol import ConvStateProtocol

from atoa.gov.acp.support import (
    _extract_token_usage,
    _estimate_cost_from_tokens,
)
from phase.bind.transport.spawn import default_environment
from watcher.plane.emitter import get_emitter

log = get_emitter(name="acp.agent", phase="agent_infrastructure")

def make_dummy_llm() -> Driver:
    return Driver(model="acp-managed")

class ACPAgent(ACPTrajectory, Ator):
    """
    @desc: Core Substrate for ACP-compatible Subprocess Architecture.
    @role: Integrates the execution step topology (Mixin) and the protocol connection manifold (Composition).
    """
    llm: Driver = Field(default_factory=make_dummy_llm)
    tools: list[Tool] = Field(default_factory=list)
    include_default_tools: list[str] = Field(default_factory=list)

    acp_command: list[str] = Field(..., description="Root execution binary array for ACP mapping.")
    acp_args: list[str] = Field(default_factory=list, description="Dimensional configuration flags for the binary.")
    acp_env: dict[str, str] = Field(default_factory=dict, description="Entropy state variables isolated for the subprocess.")
    acp_session_mode: str | None = Field(default=None)
    acp_prompt_timeout: float = Field(default=1800.0)
    acp_model: str | None = Field(default=None)

    manifold: ACPConnectionManifold | None = Field(default=None, exclude=True)
    executor: Any = Field(default=None, exclude=True)
    client: ACPClient | None = Field(default=None, exclude=True)
    is_closed: bool = Field(default=False, exclude=True)
    on_activity: Any = Field(default=None, exclude=True)

    def model_post_init(self, __context: object) -> None:
        super().model_post_init(__context)
        if self.acp_model:
            self.llm.metrics.model_name = self.acp_model
            if self.llm.metrics.accumulated_token_usage is not None:
                self.llm.metrics.accumulated_token_usage.model = self.acp_model

    # [개선점] 메서드명 언더스코어 제거
    def record_usage(self, response: Any, session_id: str, elapsed: float | None = None, usage_update: Any | None = None) -> None:
        """@desc: Synchronize and commit external telemetry data into the local structural metrics"""
        if not self.client: return
        
        cost_recorded = False
        if usage_update is not None and usage_update.cost is not None:
            last_cost = self.client._last_cost_by_session.get(session_id, 0.0)
            delta = usage_update.cost.amount - last_cost
            if delta > 0:
                self.llm.metrics.add_cost(delta)
                cost_recorded = True
            self.client._last_cost_by_session[session_id] = usage_update.cost.amount
            self.client._last_cost = usage_update.cost.amount

        input_tokens, output_tokens, cache_read, cache_write, reasoning = _extract_token_usage(response)
        if input_tokens or output_tokens:
            self.llm.metrics.add_token_usage(
                prompt_tokens=input_tokens, completion_tokens=output_tokens,
                cache_read_tokens=cache_read, cache_write_tokens=cache_write,
                reasoning_tokens=reasoning, response_id=session_id,
                context_window=self.client._context_window_by_session.get(session_id, self.client._context_window),
            )

        if not cost_recorded and (input_tokens or output_tokens) and self.acp_model:
            cost = _estimate_cost_from_tokens(self.acp_model, input_tokens, output_tokens)
            if cost > 0:
                self.llm.metrics.add_cost(cost)

        if elapsed is not None:
            self.llm.metrics.add_response_latency(elapsed, session_id)

        if self.llm.telemetry._stats_update_callback is not None:
            try: self.llm.telemetry._stats_update_callback()
            except Exception: log.debug("Telemetry synchronization failed.", exc_info=True)

    @property
    def agent_name(self) -> str: return self.manifold.agent_name if self.manifold else ""

    @property
    def agent_version(self) -> str: return self.manifold.agent_version if self.manifold else ""

    def get_all_llms(self) -> Generator[Driver]:
        yield self.llm

    def init_state(self, state: ConvStateProtocol, on_event: ConversationCallbackType) -> None:
        """@lifecycle.init"""
        with unified_flow_span(action="init_acp", workspace=str(state.workspace.working_dir)):
            log.info("## @phase.bootstrap: Synthesizing ACP Subprocess Substrate")
            on_event(
                SystemPromptEvent(
                    source="agent",
                    system_prompt=TextContent(text="[OVERRIDE] Topology managed entirely by isolated ACP Subprocess."),
                    tools=[],
                )
            )

            if self.tools or self.mcp_config or self.agent_context:
                raise NotImplementedError("Structural boundaries prevent native tools or contexts inside ACP Agents. Configure externally.")

            from arch.xor.proto.asyncer import AsyncExecutor
            self.executor = AsyncExecutor()
            self.client = ACPClient()

            env = default_environment()
            env.update(os.environ)
            env.update(self.acp_env)
            env.pop("CLAUDECODE", None)

            ## @compose: Manifold Object (Dependency Injection pattern)
            self.manifold = ACPConnectionManifold(
                executor=self.executor,
                client=self.client,
                working_dir=str(state.workspace.working_dir),
                command=self.acp_command[0],
                args=list(self.acp_command[1:]) + list(self.acp_args),
                env=env,
                acp_model=self.acp_model,
                acp_session_mode=self.acp_session_mode
            )

            try:
                self.manifold.boot()
            except Exception as e:
                log.error(f"[FATAL] Absolute divergence during ACP initialization: {e}")
                self.cleanup()
                raise

            # 상위 Ator 클래스에서 Public으로 변경된 is_initialized 속성 활용
            self.is_initialized = True
            
            # =================================================================
            # [개선됨] 직접 딕셔너리 할당(Update)을 제거하고 Command 패턴을 통해 반영
            # =================================================================
            state.apply(UpdateAgentState(key="acp_agent_name", value=self.agent_name))
            state.apply(UpdateAgentState(key="acp_agent_version", value=self.agent_version))

    def ask(self, question: str) -> str | None:
        """@desc: Delegates fork operations to the composed connection manifold"""
        if not self.manifold:
            raise RuntimeError("Topological failure: Connection Manifold not initialized.")
        return self.manifold.fork_and_prompt(question, self.record_usage)

    def close(self) -> None:
        if not self.is_closed:
            self.is_closed = True
            self.cleanup()

    def cleanup(self) -> None:
        if self.manifold:
            self.manifold.teardown()
            self.manifold = None

        if self.executor:
            try: self.executor.close()
            except Exception: pass
            self.executor = None

    def __del__(self) -> None:
        try: self.close()
        except Exception: pass