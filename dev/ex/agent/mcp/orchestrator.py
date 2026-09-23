# fiber.dev.ex.agent.mcp.orchestrator
import asyncio
import json
import uuid
import contextvars
from typing import Any, Optional

from pydantic import BaseModel, Field
import mcp_types

from fiber.dev.ex.agent.mcp.client import (
    MCPClient,
    create_mcp_client,
    fetch_mcp_tools_sync,
    call_mcp_tool_raw,
    MCPConfig,
    MCPError
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

# [2026-07-28 핵심] 네트워크 계층(mcp.client)으로 멱등성 키를 전달하기 위한 컨텍스트 변수
# SecurityProvider나 headers_factory가 이 값을 읽어 HTTP 헤더에 주입합니다.
current_idempotency_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_idempotency_key", default=None
)


class MCPAgentConfig(BaseModel):
    mcp_config: MCPConfig
    llm_profile: BaseLLMProfile = Field(..., description="LLM profile for LLMFacade communication")
    system_prompt: str = Field(default="You are a helpful enterprise assistant.", description="Base system prompt")
    max_iterations: int = Field(default=5, description="Maximum iterations for LLM reasoning and tool calls")


class MCPToolWrapper(OpenAIToolConvertible):
    """Wraps raw MCP tools to make them compatible with OpenAI tool schemas."""
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
    """
    [2026-07-28 Aligned] Agent that orchestrates LLM reasoning and strictly stateless MCP Client interactions.
    """
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
        """Initializes the MCP client and fetches available tools via the stateless bridge."""
        if self._is_initialized:
            return

        log.info(f"[{self.name}] Initializing MCP Client...")
        
        self._mcp_client = create_mcp_client(
            executor=self._async_executor, 
            config=self.config.mcp_config
        )
        
        loop = asyncio.get_running_loop()
        try:
            # 2026-07-28 Gateway의 공시(Discovery) 엔드포인트를 타격하여 도구 목록 확보
            raw_mcp_tools = await loop.run_in_executor(
                None, 
                fetch_mcp_tools_sync, 
                self._mcp_client, 
                30.0
            )
            
            self._runtime_tools = [MCPToolWrapper(t) for t in raw_mcp_tools]
            self._is_initialized = True
            log.info(f"[{self.name}] Initialization complete. Loaded {len(self._runtime_tools)} tools.")
        except Exception as e:
            log.error(f"[{self.name}] Failed to discover tools from Gateway: {e}")
            raise

    async def run_worker(self, tunnel: UniversalFacade, conversation_id: str) -> None:
        """Main worker loop to consume events from the tunnel and process them."""
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
        """Executes the cross-invocation sequence adapting to Stateless Gateway responses."""
        log.info(f"[{self.name}] Processing new task...")
        user_message_text = task_payload.get("prompt", "")
        
        messages = [
            Message(role="system", content=[TextContent(text=self.config.system_prompt)]),
            Message(role="user", content=[TextContent(text=user_message_text)])
        ]

        for iteration in range(self.config.max_iterations):
            log.info(f"[{self.name}] Iteration {iteration + 1}")
            
            # [핵심] 턴(Turn) 단위의 고유 ID 발급 (LLM 환각에 의한 멱등성 키 충돌 방지용)
            turn_id = uuid.uuid4().hex
            
            # 1. Request completion from LLM via LLMFacade
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

            usage = llm_response.metrics.accumulated_token_usage
            log.info(f"LLM Reply Tokens: P:{usage.prompt_tokens} / C:{usage.completion_tokens}")
            
            response_msg: Message = llm_response.message
            messages.append(response_msg)

            # Check if LLM requested any tool calls
            tool_calls = self._extract_tool_calls(response_msg)
            
            if not tool_calls:
                content_text = "".join(c.text for c in response_msg.content if isinstance(c, TextContent))
                await self._emit_response(tunnel, response_topic, {"type": "final_answer", "content": content_text})
                break

            # 2. Execute MCP Tools Pipeline (Stateless execution with Cognitive Resilience)
            for tool_call in tool_calls:
                tool_name = tool_call.function.name
                arguments_str = tool_call.function.arguments
                
                try:
                    arguments = json.loads(arguments_str) if arguments_str else {}
                except json.JSONDecodeError as e:
                    error_msg = f"JSON decode error for arguments: {e}"
                    messages.append(Message(role="tool", tool_call_id=tool_call.id, content=[TextContent(text=error_msg)]))
                    await self._emit_response(tunnel, response_topic, {
                        "type": "tool_result", "tool_name": tool_name, "status": "error", "message": error_msg
                    })
                    continue

                await self._emit_response(tunnel, response_topic, {"type": "tool_start", "tool_name": tool_name})
                
                # [핵심 정렬] Fiber의 턴 ID와 LLM의 Tool Call ID를 결합한 완벽한 합성 멱등성 키 생성
                composite_idempotency_key = f"{turn_id}::{tool_call.id}"
                token = current_idempotency_key.set(composite_idempotency_key)
                
                try:
                    # Gateway 타격 (mcp.client 내부에서 SDK가 HTTP POST 발송)
                    result = await call_mcp_tool_raw(
                        client=self._mcp_client,
                        tool_name=tool_name,
                        arguments=arguments
                    )
                    
                    # 툴 응답 결과가 Gateway의 거절(에러)을 포함하는지 확인
                    result_dump = result.model_dump()
                    is_error = result.isError if hasattr(result, 'isError') else False
                    
                    if is_error:
                        # Gateway가 반환한 로직/권한 에러를 LLM이 인지할 수 있도록 피드백
                        error_str = f"Gateway Execution Error: {result_dump.get('content', 'Unknown infrastructure error')}"
                        log.warning(f"[{self.name}] Gateway denied tool '{tool_name}': {error_str}")
                        
                        messages.append(Message(role="tool", tool_call_id=tool_call.id, content=[TextContent(text=error_str)]))
                        await self._emit_response(tunnel, response_topic, {
                            "type": "tool_result", "tool_name": tool_name, "status": "error", "message": error_str
                        })
                    else:
                        # 정상 응답
                        result_str = json.dumps(result_dump)
                        messages.append(Message(role="tool", tool_call_id=tool_call.id, content=[TextContent(text=result_str)]))
                        await self._emit_response(tunnel, response_topic, {
                            "type": "tool_result", "tool_name": tool_name, "status": "success", "result": result_dump
                        })
                        
                except Exception as e:
                    # 네트워크 단절 등 치명적 에러 발생 시 처리
                    log.error(f"[{self.name}] Tool '{tool_name}' failed to execute: {e}")
                    error_str = f"System Error executing tool via Gateway: {str(e)}"
                    messages.append(Message(role="tool", tool_call_id=tool_call.id, content=[TextContent(text=error_str)]))
                    await self._emit_response(tunnel, response_topic, {
                        "type": "tool_result", "tool_name": tool_name, "status": "error", "message": error_str
                    })
                finally:
                    # 다음 도구 실행을 위해 멱등성 키 컨텍스트 초기화
                    current_idempotency_key.reset(token)

    def _extract_tool_calls(self, message: Message) -> list[MessageToolCall]:
        return [c for c in message.content if isinstance(c, MessageToolCall)]

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