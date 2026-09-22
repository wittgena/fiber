# fiber.dev.ex.agent.mcp.orchestrator
import asyncio
import json
from typing import Any, Optional

from pydantic import BaseModel, Field
import mcp_types

from fiber.dev.ex.agent.mcp.client import (
    MCPClient,
    create_mcp_client,
    fetch_mcp_tools_sync,
    call_mcp_tool_raw,
    MCPConfig
)
from fiber.dev.ex.agent.protocol.executor import AsyncExecutor
from fiber.dev.ex.facade.driver import LLMFacade, OpenAIToolConvertible
from fiber.dev.ex.facade.response import LLMResponse
from fiber.llm.model.message import Message, TextContent, MessageToolCall
from fiber.llm.model.profile import BaseLLMProfile
from fiber.llm.param import ChatCompletionToolParam

from xphi.kernel.space.tunnel.factory import UniversalFacade
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("agent.mcp.orchestrator")

class MCPAgentConfig(BaseModel):
    mcp_config: MCPConfig
    llm_profile: BaseLLMProfile = Field(..., description="LLMFacade 통신에 사용할 LLM 프로필")
    system_prompt: str = Field(default="You are a helpful assistant.", description="기본 시스템 프롬프트")
    max_iterations: int = Field(default=5, description="최대 도구 호출 반복 횟수")


class MCPToolWrapper(OpenAIToolConvertible):
    """@desc: mcp_types.Tool을 fiber.dev.ex.facade의 OpenAIToolConvertible 규격으로 래핑"""
    def __init__(self, raw_tool: mcp_types.Tool):
        self.raw_tool = raw_tool

    @property
    def name(self) -> str:
        return self.raw_tool.name

    def to_openai_tool(self, add_security_risk_prediction: bool = True) -> ChatCompletionToolParam:
        return {
            "type": "function",
            "function": {
                "name": self.raw_tool.name,
                "description": self.raw_tool.description or "",
                "parameters": self.raw_tool.inputSchema
            }
        }

class MCPOrchestrator:
    """LLMFacade와 통신하고 MCP 클라이언트를 제어하는 순수 에이전트"""
    def __init__(self, config: MCPAgentConfig):
        self.config = config
        self._is_initialized = False
        
        self._async_executor = AsyncExecutor()
        self._mcp_client: Optional[MCPClient] = None
        self._runtime_tools: list[MCPToolWrapper] = []

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def initialize(self) -> None:
        """MCP 클라이언트를 초기화하고 사용할 도구들을 래핑"""
        if self._is_initialized:
            return

        log.info(f"[{self.name}] Initializing MCP Client...")
        
        self._mcp_client = create_mcp_client(
            executor=self._async_executor, 
            config=self.config.mcp_config
        )
        
        loop = asyncio.get_running_loop()
        raw_mcp_tools = await loop.run_in_executor(
            None, 
            fetch_mcp_tools_sync, 
            self._mcp_client, 
            30.0
        )
        
        self._runtime_tools = [MCPToolWrapper(t) for t in raw_mcp_tools]
        self._is_initialized = True
        log.info(f"[{self.name}] Initialization complete. Loaded {len(self._runtime_tools)} tools.")

    async def run_worker(self, tunnel: UniversalFacade, conversation_id: str) -> None:
        """터널에서 이벤트를 수신하는 메인 워커 루프"""
        if not self._is_initialized:
            await self.initialize()

        task_topic = f"agent:tasks:{conversation_id}"
        response_topic = f"agent:responses:{conversation_id}"
        group_name = f"mcp_worker_group_{conversation_id}"
        consumer_name = f"worker_{id(self)}"

        log.info(f"[{self.name}] Worker started. Listening on {task_topic}")

        while True:
            try:
                results = await tunnel.stream_consume(
                    topic=task_topic, 
                    group=group_name, 
                    consumer=consumer_name, 
                    count=1, 
                    block=5000
                )
                
                if not results:
                    continue

                for stream_name, payload in results:
                    for message_id, message_data in payload:
                        try:
                            parsed_task = json.loads(message_data.get(b"payload", b"{}").decode("utf-8"))
                            await self.process_task(parsed_task, tunnel, response_topic)
                        finally:
                            await tunnel.stream_ack(task_topic, group_name, message_id)
            
            except Exception as e:
                log.error(f"[{self.name}] Critical error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(1)

    async def process_task(self, task_payload: dict, tunnel: UniversalFacade, response_topic: str) -> None:
        """LLMFacade와 MCP 도구 간의 교차 호출 시퀀스"""
        log.info(f"[{self.name}] Processing new task...")
        user_message_text = task_payload.get("prompt", "")
        messages = [
            Message(role="system", content=[TextContent(text=self.config.system_prompt)]),
            Message(role="user", content=[TextContent(text=user_message_text)])
        ]

        for iteration in range(self.config.max_iterations):
            log.info(f"[{self.name}] Iteration {iteration + 1}")
            
            # 1. LLMFacade를 통해 실제 LLM 호출 수행
            try:
                llm_response: LLMResponse = await LLMFacade.make_completion(
                    llm=self.config.llm_profile,
                    messages=messages,
                    tools=self._runtime_tools
                )
            except Exception as e:
                log.error(f"[{self.name}] LLMFacade completion failed: {e}")
                await self._emit_response(tunnel, response_topic, {"type": "error", "message": str(e)})
                break

            # 메트릭 로깅
            usage = llm_response.metrics.accumulated_token_usage
            log.info(f"LLM Reply Tokens: P:{usage.prompt_tokens} / C:{usage.completion_tokens}")
            
            response_msg: Message = llm_response.message
            messages.append(response_msg)

            # 도구 호출(Tool Call) 여부 확인
            tool_calls = self._extract_tool_calls(response_msg)
            
            if not tool_calls:
                # 일반 텍스트 응답이면 종료
                content_text = "".join(c.text for c in response_msg.content if isinstance(c, TextContent))
                await self._emit_response(tunnel, response_topic, {"type": "final_answer", "content": content_text})
                break

            # 2. MCP 도구 실행 파이프라인
            for tool_call in tool_calls:
                tool_name = tool_call.function.name
                arguments_str = tool_call.function.arguments
                
                try:
                    arguments = json.loads(arguments_str) if arguments_str else {}
                except json.JSONDecodeError:
                    arguments = {}

                await self._emit_response(tunnel, response_topic, {"type": "tool_start", "tool_name": tool_name})
                
                try:
                    # fiber.dev.ex.agent.mcp.call_mcp_tool_raw 실행
                    result = await call_mcp_tool_raw(
                        client=self._mcp_client,
                        tool_name=tool_name,
                        arguments=arguments
                    )
                    
                    # MCP의 mcp_types.CallToolResult(보통 텍스트 묶음 반환) 처리
                    result_dump = result.model_dump()
                    result_str = json.dumps(result_dump)
                    
                    # 도구 호출 결과를 LLM History에 추가
                    tool_result_msg = Message(
                        role="tool", 
                        tool_call_id=tool_call.id,
                        content=[TextContent(text=result_str)]
                    )
                    messages.append(tool_result_msg)
                    await self._emit_response(tunnel, response_topic, {
                        "type": "tool_result", 
                        "tool_name": tool_name, 
                        "status": "success", 
                        "result": result_dump
                    })
                except Exception as e:
                    log.error(f"Tool {tool_name} failed: {e}")
                    error_str = f"Error executing tool: {str(e)}"
                    messages.append(Message(
                        role="tool", 
                        tool_call_id=tool_call.id,
                        content=[TextContent(text=error_str)]
                    ))
                    await self._emit_response(tunnel, response_topic, {
                        "type": "tool_result", 
                        "tool_name": tool_name, 
                        "status": "error", 
                        "message": str(e)
                    })

    def _extract_tool_calls(self, message: Message) -> list[MessageToolCall]:
        """LLM 응답에서 MessageToolCall 인스턴스를 필터링하여 추출"""
        tool_calls = []
        for c in message.content:
            if isinstance(c, MessageToolCall):
                tool_calls.append(c)
        return tool_calls

    async def _emit_response(self, tunnel: UniversalFacade, topic: str, payload: dict) -> None:
        try:
            raw_payload = {b"payload": json.dumps(payload).encode("utf-8")}
            await tunnel.stream_produce(topic, payload=raw_payload)
        except Exception as e:
            log.error(f"[{self.name}] Failed to emit response: {e}")

    def close(self) -> None:
        if self._mcp_client and not getattr(self._mcp_client, "is_closed", False):
            log.info(f"[{self.name}] Closing MCP Client explicitly...")
            try:
                self._mcp_client.sync_close()
            except Exception as e:
                log.warning(f"Error while closing MCP client: {e}", exc_info=True)
                
        self._async_executor.close()