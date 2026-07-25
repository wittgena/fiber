# gov.acp.agent
## @lineage: gov.atoa.acp.agent
## @lineage: eco.gov.atoa.acp.agent
## @lineage: atoa.gov.acp.agent
## @lineage: agent.gov.acp.agent
from __future__ import annotations
import os
import asyncio
from collections.abc import Generator
from typing import TYPE_CHECKING, Any
from pydantic import Field

from watcher.plane.observer.span import unified_flow_span

from gov.disc.ator import Ator
from atoa.event.llm.system import SystemPromptEvent
from atoa.event.llm.message import MessageEvent
from gov.disc.tool import Tool

from agent.driver.tensor import Driver
from atoa.mesh.action.message import Message, TextContent
from gov.acp.client import ACPClient
from gov.acp.step import ACPTrajectory
from gov.acp.connection import ACPConnectionManifold

from arch.topos.bound.payload import StreamPayloadAdapter
from arch.topos.bound.tunnel import UniversalFacade

from gov.acp.support import (
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
    @role: 외부 프로세스(CLI 등)로 툴 실행을 전적으로 위임하는 블랙박스(Blackbox) 에이전트.
           새로운 비동기 Ator 워커 파이프라인(process_task)을 준수합니다.
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

    # =========================================================================
    # [핵심 1] 기반 클래스(Ator) 규격에 맞춘 비동기 초기화 
    # =========================================================================
    async def initialize(self) -> None:
        """@desc: Async initialization for ACP subprocess. (기존 init_state 대체)"""
        if self.is_initialized:
            return

        with unified_flow_span(action="init_acp", workspace=os.getcwd()):
            log.info("## @phase.bootstrap: Synthesizing ACP Subprocess Substrate")

            if self.tools or self.mcp_config or self.agent_context:
                log.warning("Structural boundaries prevent native tools or contexts inside ACP Agents.")

            from agent.executor.base import AsyncExecutor
            self.executor = AsyncExecutor()
            self.client = ACPClient()

            env = default_environment()
            env.update(os.environ)
            env.update(self.acp_env)
            env.pop("CLAUDECODE", None)

            # 분리된 아키텍처에서는 Gov의 Workspace를 알 수 없으므로 우선 프로세스 CWD를 바인딩합니다.
            self.manifold = ACPConnectionManifold(
                executor=self.executor,
                client=self.client,
                working_dir=os.getcwd(),
                command=self.acp_command[0],
                args=list(self.acp_command[1:]) + list(self.acp_args),
                env=env,
                acp_model=self.acp_model,
                acp_session_mode=self.acp_session_mode
            )

            try:
                # 부트스트랩 동기/비동기 호환 래퍼
                if asyncio.iscoroutinefunction(self.manifold.boot):
                    await self.manifold.boot()
                else:
                    self.manifold.boot()
            except Exception as e:
                log.error(f"[FATAL] Absolute divergence during ACP initialization: {e}")
                self.cleanup()
                raise

            self.is_initialized = True

    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        """
        @desc: Gov로부터 페이로드를 전달받아, 블랙박스 외부 프로세스에 질문을 위임하고 
               그 응답을 Tunnel로 다시 방출합니다.
        """
        if not self.manifold:
            raise RuntimeError("Topological failure: Connection Manifold not initialized.")

        iteration = task_payload.get("iteration", 0)
        if iteration == 0:
            sys_event = SystemPromptEvent(
                source="agent",
                system_prompt=TextContent(text="[OVERRIDE] Topology managed entirely by isolated ACP Subprocess."),
                tools=[],
            )
            await tunnel.stream_produce(response_topic, {
                "type": "system_prompt",
                "event_payload": sys_event.model_dump()
            })

        events = task_payload.get("events", [])
        instruction = "Proceed with task."
        
        for event_dict in reversed(events):
            if event_dict.get("source") == "user" and "llm_message" in event_dict:
                content_list = event_dict["llm_message"].get("content", [])
                texts = [c.get("text", "") for c in content_list if isinstance(c, dict) and "text" in c]
                if texts:
                    instruction = " ".join(texts)
                    break

        log.info(f"[{self.agent_name}] Delegating instruction to external ACP Process: {instruction[:50]}...")
        loop = asyncio.get_running_loop()
        response_text = await loop.run_in_executor(
            None, 
            self.manifold.fork_and_prompt, 
            instruction, 
            self.record_usage
        )

        if response_text:
            msg_event = MessageEvent(
                source="agent",
                llm_message=Message(role="assistant", content=[TextContent(text=response_text)])
            )
            payload_raw = {"type": "message", "event_payload": msg_event.model_dump()}
            await tunnel.stream_produce(response_topic, StreamPayloadAdapter.encode(payload_raw))

        finish_payload = {"type": "finish"}
        await tunnel.stream_produce(response_topic, StreamPayloadAdapter.encode(finish_payload))

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
    def agent_name(self) -> str: return self.manifold.agent_name if self.manifold else "ACPAgent"

    @property
    def agent_version(self) -> str: return self.manifold.agent_version if self.manifold else "1.0.0"

    def get_all_llms(self) -> Generator[Driver]:
        yield self.llm

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