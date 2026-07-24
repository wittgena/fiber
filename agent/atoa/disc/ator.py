# agent.atoa.disc.ator
## @lineage: atoa.agent.disc.ator
from __future__ import annotations
import re
import json
import asyncio
from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from pydantic import BaseModel, ConfigDict, Field

from agent.atoa.context import AtorContext
from agent.atoa.disc.tool import Tool
from agent.atoa.disc.schema.reflect import ReflectorBase
from agent.atoa.driver.tensor import Driver

from agent.eco.mcp.client import MCPClient
from agent.eco.mcp.factory import create_mcp_tools
from agent.atoa.action.tool.mcp import MCPExecutor
from agent.atoa.action.definition import ActionDefinition

from agent.atoa.action.factory import CoreAction
from agent.atoa.action.resolver import ActionResolver

from arch.topos.bound.payload import StreamPayloadAdapter
from arch.topos.bound.tunnel import UniversalFacade
from arch.topos.bound.surge.disc import DiscMixin
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class Ator(DiscMixin, ABC):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    llm: Driver = Field(..., description="LLM configuration for the agent.")
    actions: list[Tool] = Field(
        default_factory=lambda: [Tool(name=action.value, params={}) for action in CoreAction],
        description="List of core cognitive and system control actions (e.g., finish, bridge, think).",
    )
    tools: list[Tool] = Field(
        default_factory=list,
        description="List of external capabilities schemas to load for the LLM.",
    )
    mcp_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional MCP configuration dictionary to create MCP tools (Agent-side managed).",
    )
    filter_tools_regex: str | None = Field(
        default=None,
        description="Optional regex to filter the external tools available to the agent by name. Core actions are immune.",
    )
    agent_context: AtorContext = Field(
        default_factory=AtorContext,
        description="AgentContext to manage prompts, secrets, and environment.",
    )
    reflector: ReflectorBase | None = Field(
        default=None,
        description="Optional reflector to evaluate agent actions and messages in real-time.",
    )
    tool_concurrency_limit: int = Field(
        default=1,
        ge=1,
        description="Maximum number of tool calls to execute concurrently within a single agent step.",
    )
    
    # [수정] 런타임 툴은 실행기(executor)가 아닌 스키마(Definition) 집합으로서의 역할을 명확히 합니다.
    runtime_tools: dict[str, ActionDefinition] = Field(default_factory=dict, exclude=True)
    is_initialized: bool = Field(default=False, exclude=True)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    # [수정] state(ConvStateProtocol) 의존성 제거 및 비동기 초기화 도입
    async def initialize(self) -> None:
        """
        @desc: 에이전트 구동에 필요한 LLM 도구 스키마(Schema) 및 MCP 클라이언트를 초기화합니다.
               실제 툴의 실행 환경(Gov)과는 무관하게 LLM에 주입할 인터페이스만 준비합니다.
        """
        if self.is_initialized:
            log.warning(f"[{self.name}] Agent already initialized; skipping re-initialization.")
            return

        resolved_defs: list[ActionDefinition] = []
        unique_specs: dict[str, Tool] = {}
        for spec in (self.actions + self.tools):
            unique_specs[spec.name] = spec

        combined_specs = self.actions + self.tools
        
        loop = asyncio.get_running_loop()
        
        def _resolve_sync():
            local_defs = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                for spec in combined_specs:
                    # state 없이 스키마만 Resolve하도록 수정 가정
                    futures.append(executor.submit(ActionResolver.resolve, spec, None))

                if self.mcp_config:
                    futures.append(executor.submit(create_mcp_tools, self.mcp_config, 30))

                for future in futures:
                    local_defs.extend(future.result())
            return local_defs

        resolved_defs = await loop.run_in_executor(None, _resolve_sync)

        if self.filter_tools_regex:
            pattern = re.compile(self.filter_tools_regex)
            resolved_defs = [
                tool for tool in resolved_defs 
                if CoreAction.is_safe_cognitive(tool.name) or pattern.match(tool.name)
            ]
            log.info(f"[{self.name}] Filtered tools: {[t.name for t in resolved_defs]}")

        for tool in resolved_defs:
            if not isinstance(tool, ActionDefinition):
                raise ValueError(f"Tool {tool} is not an instance of 'ActionDefinition'.")

        tool_names = [tool.name for tool in resolved_defs]
        if len(tool_names) != len(set(tool_names)):
            duplicates = set(name for name in tool_names if tool_names.count(name) > 1)
            raise ValueError(f"Duplicate capability names found: {duplicates}")

        self.runtime_tools = {tool.name: tool for tool in resolved_defs}
        self.is_initialized = True
        log.info(f"[{self.name}] Successfully initialized with capabilities: {list(self.runtime_tools.keys())}")

    async def run_worker(self, tunnel: UniversalFacade, conversation_id: str) -> None:
        """
        @desc: 독립 컨테이너나 스레드에서 실행되는 메인 워커 루프입니다.
               Tunnel(Redis Streams)을 구독하여 Gov(Conver)의 명령을 대기합니다.
        """
        if not self.is_initialized:
            await self.initialize()

        task_topic = f"agent:tasks:{conversation_id}"
        response_topic = f"agent:responses:{conversation_id}"
        group_name = f"agent_worker_group_{conversation_id}"
        consumer_name = f"worker_{id(self)}"

        log.info(f"[{self.name}] Worker started. Listening on {task_topic}")

        while True:
            try:
                # 1. Gov 노드로부터의 추론 요청(Task Payload) 수신 대기
                results = await tunnel.stream_consume(
                    topic=task_topic, 
                    group=group_name, 
                    consumer=consumer_name, 
                    count=1, 
                    block=5000
                )
                
                if not results:
                    continue

                for stream_name, messages in results:
                    for message_id, message_data in messages:
                        log.debug(f"[{self.name}] Received task message: {message_id}")
                        try:
                            # 어댑터로 파싱 후 비즈니스 로직(process_task)에 순수 딕셔너리 전달
                            parsed_task = StreamPayloadAdapter.decode(message_data)
                            await self.process_task(parsed_task, tunnel, response_topic)
                        finally:
                            await tunnel.stream_ack(task_topic, group_name, message_id)
            except Exception as e:
                log.error(f"[{self.name}] Critical error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(1) # 에러 발생 시 백오프

    @abstractmethod
    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        """
        @desc: 전달받은 대화 상태(events)와 환결 변수들을 기반으로 LLM을 호출하고, 
               그 결과(Action 또는 Message)를 response_topic 채널을 통해 Gov에 발행(Produce)합니다.
        
        Args:
            task_payload: Conver.run() 에서 전달한 {"events": [...], "iteration": ...} 형태의 데이터
            tunnel: 이벤트를 반환할 통신 객체
            response_topic: 결과를 쏴주어야 할 스트림 토픽
        """
        pass

    def get_active_mcp_clients(self) -> set[MCPClient]:
        clients: set[MCPClient] = set()
        for tool in self.runtime_tools.values():
            if hasattr(tool, 'executor') and isinstance(tool.executor, MCPExecutor):
                client = tool.executor.client
                clients.add(client)
        return clients

    def close(self) -> None:
        mcp_clients = self.get_active_mcp_clients()
        for client in mcp_clients:
            try:
                if not getattr(client, "is_closed", False):
                    log.debug(f"[{self.name}] Closing MCP Client explicitly...")
                    client.sync_close()
            except Exception as e:
                log.warning(f"Error while closing MCP client: {e}", exc_info=True)

    def verify(self, persisted: Ator, events: Sequence[Any] | None = None) -> Ator:
        if persisted.__class__ is not self.__class__:
            raise ValueError(
                "Cannot load from persisted: persisted agent is of type "
                f"{persisted.__class__.__name__}, but self is of type "
                f"{self.__class__.__name__}."
            )

        runtime_names = {t.name for t in self.actions + self.tools}
        persisted_names = {t.name for t in persisted.actions + persisted.tools}

        missing_in_runtime = persisted_names - runtime_names
        if missing_in_runtime:
            raise ValueError(
                f"Cannot resume conversation: capabilities were removed mid-conversation "
                f"(removed: {sorted(missing_in_runtime)}). "
            )
        return self

    def model_dump_succint(self, **kwargs):
        if "exclude_none" not in kwargs:
            kwargs["exclude_none"] = True
        dumped = super().model_dump(**kwargs)
        if "tools" in dumped and isinstance(dumped["tools"], dict):
            dumped["tools"] = list(dumped["tools"].keys())
        if "actions" in dumped and isinstance(dumped["actions"], dict):
            dumped["actions"] = list(dumped["actions"].keys())
        return dumped

    def get_all_llms(self) -> Generator[Driver]:
        yielded_ids: set[int] = set()
        visited: set[int] = set()

        def _walk(obj: object) -> Iterable[Driver]:
            oid = id(obj)
            if oid in visited:
                return ()
            visited.add(oid)

            if isinstance(obj, Driver):
                llm_out: list[Driver] = []
                if type(obj) is Driver and oid not in yielded_ids:
                    yielded_ids.add(oid)
                    llm_out.append(obj)

                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    llm_out.extend(_walk(val))
                return llm_out

            if isinstance(obj, BaseModel):
                model_out: list[Driver] = []
                for name in type(obj).model_fields:
                    try:
                        val = getattr(obj, name)
                    except Exception:
                        continue
                    model_out.extend(_walk(val))
                return model_out

            if isinstance(obj, dict):
                dict_out: list[Driver] = []
                for k, v in obj.items():
                    dict_out.extend(_walk(k))
                    dict_out.extend(_walk(v))
                return dict_out

            if isinstance(obj, (list, tuple, set, frozenset)):
                container_out: list[Driver] = []
                for item in obj:
                    container_out.extend(_walk(item))
                return container_out

            return ()
        yield from _walk(self)

    @property
    def tools_map(self) -> dict[str, ActionDefinition]:
        if not self.is_initialized:
            raise RuntimeError("Agent not initialized; call initialize() before use")
        return self.runtime_tools

    def ask(self, question: str) -> str | None:
        """비동기 아키텍처에서는 더 이상 직접적인 ask() 동기 호출을 지원하지 않습니다."""
        log.warning("Ator.ask() is deprecated in decoupled architecture.")
        return None