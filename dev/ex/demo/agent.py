# fiber.dev.ex.demo.agent
import asyncio
import uuid
import json
import contextvars
from typing import Dict, List, Any, Optional

from fiber.dev.ex.agent.mcp.client import (
    MCPClient, create_mcp_client, fetch_mcp_tools_sync, call_mcp_tool_raw, MCPConfig
)
from fiber.dev.ex.agent.protocol.executor import AsyncExecutor
from fiber.dev.ex.facade.driver import LLMFacade
from fiber.dev.ex.facade.response import LLMResponse
from fiber.llm.model.message import TextContent
from fiber.llm.model.profile import BaseLLMProfile
from fiber.dev.ex.agent.mcp.orchestrator import MCPToolWrapper

from fiber.dev.ex.demo.state import (
    AgentState, 
    SystemPromptEvent, 
    UserMessageEvent, 
    AssistantTurnEvent, 
    ToolObservationEvent, 
    ToolErrorEvent
)

MODEL_NAME = "gemini/gemini-3.1-flash-lite"

# =====================================================================
# 1. 멱등성 키 동적 주입을 위한 ContextVar 
# =====================================================================
# 실행(invoke) 시점에 Action의 ID를 이 변수에 주입하면, mcp.client 내부의 
# headers_factory가 이 값을 읽어 HTTP 헤더에 안전하게 맵핑합니다.
current_idempotency_key: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "current_idempotency_key", default=None
)

class SecurityProvider:
    def __init__(self, spiffe_id: str, billing_token: str = "valid_x402"):
        self.spiffe_id = spiffe_id
        self.billing_token = billing_token

    def get_headers(self) -> Dict[str, str]:
        # ContextVar를 지연 평가(Lazy Evaluation)하여 현재 실행 중인 Action의 멱등성 키 획득
        idem_key = current_idempotency_key.get() or uuid.uuid4().hex
        return {
            "x-idempotency-key": idem_key,
            "x-nonce": uuid.uuid4().hex,
            "x-spiffe-id": self.spiffe_id,
            "X-X402-Receipt": self.billing_token
        }

# =====================================================================
# 2. Main Agent Orchestrator
# =====================================================================
class MultiMCPRouter:
    """
    1:N Multi-Worker Orchestrator.
    Event-Sourced 상태 관리와 MCP SDK(Stateless 확장판) 통신을 오케스트레이션합니다.
    """
    def __init__(self, target_workers: List[str], llm_profile: BaseLLMProfile, system_prompt: str):
        self.target_workers = target_workers
        self.llm_profile = llm_profile
        self.system_prompt = system_prompt
        self.max_iterations = 5
        
        self.security_provider = SecurityProvider("spiffe://public/agent/super_agent")
        self.base_url = "http://127.0.0.1:8000/v1/mcp-gateway"
        
        self._async_executor = AsyncExecutor()
        self._clients: Dict[str, MCPClient] = {}

        self._tool_routing_map: Dict[str, str] = {}
        self._unified_tools: List[Dict[str, Any]] = []

    async def initialize(self):
        print(f"🔌 Initializing MCP Clients (transport='stateless') for: {self.target_workers}...")
        loop = asyncio.get_running_loop()
        
        for worker_id in self.target_workers:
            # [2026-07-28 핵심] mcp.client를 "stateless" 모드로 구성
            config = MCPConfig(
                transport="stateless", 
                server_url=f"{self.base_url}/{worker_id}/invoke",
                headers_factory=self.security_provider.get_headers
            )
            
            client = create_mcp_client(executor=self._async_executor, config=config)
            self._clients[worker_id] = client
            
            try:
                # SDK 내부의 무상태 통신을 통해 GET /tools 공시 데이터 확보
                raw_tools = await loop.run_in_executor(None, fetch_mcp_tools_sync, client, 10.0)
                
                for t in raw_tools:
                    wrapped_tool = MCPToolWrapper(t)
                    self._unified_tools.append(wrapped_tool.to_openai_tool())
                    self._tool_routing_map[wrapped_tool.name] = worker_id
                    
                print(f"  └─ [{worker_id}] 🟢 Connected. Loaded {len(raw_tools)} tools.")
            except Exception as e:
                print(f"  └─ [{worker_id}] 🔴 Failed to connect: {e}")

        print(f"✅ Initialization complete. Aggregated {len(self._unified_tools)} cross-domain tools.")

    async def process_task(self, prompt: str):
        print(f"\n🚀 Dispatching Complex Task: \n{prompt}")
        
        # 1. 상태 머신 초기화 (demo.state 활용)
        state = AgentState()
        state.add_event(SystemPromptEvent(content=self.system_prompt))
        state.add_event(UserMessageEvent(content=prompt))

        for iteration in range(self.max_iterations):
            print(f"\n🧠 [LLM Reasoning - Iteration {iteration + 1}]")
            
            # [핵심 정렬] 턴(Turn) 단위의 고유 ID 발급 (LLM 환각에 의한 병렬 멱등성 충돌 방지)
            turn_id = uuid.uuid4().hex
            
            # 2. State를 LLM 프롬프트로 변환
            messages = state.to_llm_messages()
            try:
                llm_response: LLMResponse = await LLMFacade.make_completion(
                    llm=self.llm_profile,
                    messages=messages,
                    tools=self._unified_tools
                )
            except Exception as e:
                print(f"🚨 Fatal LLM Error: {e}")
                break

            # 3. LLM의 응답을 이벤트로 기록
            state.add_event(AssistantTurnEvent(
                message_dump=llm_response.message.model_dump()
            ))

            # 4. [정렬] 최적화된 demo.state에서 순수 MessageToolCall 추출
            pending_calls = state.extract_pending_tool_calls()
            
            if not pending_calls:
                final_text = "".join(c.text for c in llm_response.message.content if hasattr(c, 'text'))
                print(f"\n🤖 [Final Answer]\n{final_text}")
                break

            # 5. 네트워크 계층 호출 및 인지적 예외(Cognitive Resilience) 처리
            for tool_call in pending_calls:
                tool_name = tool_call.function.name
                target_worker = self._tool_routing_map.get(tool_name)
                
                if not target_worker:
                    print(f"🚨 Routing Error: Unknown tool '{tool_name}'")
                    state.add_event(ToolErrorEvent(
                        tool_call_id=tool_call.id,
                        tool_name=tool_name,
                        error_message=f"Routing Error: Unknown tool '{tool_name}'"
                    ))
                    continue

                # JSON 인자 파싱 및 에러 처리 (상태 원장이 아닌 에이전트 루프에서 담당)
                try:
                    arguments = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                except json.JSONDecodeError as e:
                    error_msg = f"JSON decode error: {str(e)}"
                    print(f"  └─ ⚠️ [Parse Error] {error_msg}")
                    state.add_event(ToolErrorEvent(
                        tool_call_id=tool_call.id,
                        tool_name=tool_name,
                        error_message=error_msg
                    ))
                    continue

                # [핵심 정렬] Fiber 턴 ID와 LLM Tool Call ID를 결합한 합성 멱등성 키 주입
                composite_idempotency_key = f"{turn_id}::{tool_call.id}"
                token = current_idempotency_key.set(composite_idempotency_key)
                
                print(f"⚙️ [Tool Call] Routing '{tool_name}' ➔ [{target_worker}] (Idempotency: {composite_idempotency_key})")
                target_client = self._clients[target_worker]
                
                try:
                    # SDK를 통해 실행 (내부적으로 Stateless HTTP POST 발송)
                    result = await call_mcp_tool_raw(
                        client=target_client,
                        tool_name=tool_name,
                        arguments=arguments
                    )
                    
                    result_dump = result.model_dump()
                    is_error = getattr(result, 'isError', False)
                    
                    if is_error:
                        # Gateway의 검증 실패(401, 502 등)를 LLM 문맥으로 전환
                        error_msg = f"Gateway Execution Error: {result_dump.get('content', 'Unknown error')}"
                        print(f"  └─ ⚠️ [Gateway Rejected] {error_msg}")
                        state.add_event(ToolErrorEvent(
                            tool_call_id=tool_call.id,
                            tool_name=tool_name,
                            error_message=error_msg
                        ))
                    else:
                        print(f"  └─ ✅ [Success] Retrieved response from {target_worker}")
                        state.add_event(ToolObservationEvent(
                            tool_call_id=tool_call.id,
                            tool_name=tool_name,
                            result=result_dump
                        ))
                        
                except Exception as e:
                    print(f"  └─ ❌ [Network Fault] {e}")
                    # 파이썬 크래시를 방지하고 State Ledger에 에러 기록
                    state.add_event(ToolErrorEvent(
                        tool_call_id=tool_call.id,
                        tool_name=tool_name,
                        error_message=f"Internal MCP Client Fault: {str(e)}"
                    ))
                finally:
                    # 다음 Action을 위해 ContextVar 초기화
                    current_idempotency_key.reset(token)

    def close(self):
        """안전하게 MCP Client와 Executor를 닫습니다."""
        for client in self._clients.values():
            if not getattr(client, "is_closed", False):
                try:
                    client.sync_close()
                except Exception:
                    pass
        self._async_executor.close()


async def run_agent():
    target_workers = ["oracle-01", "finlib-01", "search-archive-01"]
    agent = MultiMCPRouter(
        target_workers=target_workers,
        llm_profile=BaseLLMProfile(model_name=MODEL_NAME),
        system_prompt=(
            "You are an enterprise AI designed to comprehensively handle financial analysis, "
            "market research, exchange rates, and mathematical calculations. "
            "Solve the user's request by intelligently combining the provided tools. "
            "If a tool fails due to a network or authentication error, analyze the error and try a different approach."
        )
    )

    await agent.initialize()
    
    prompt = (
        "1. Fetch the latest market price for BTCUSDT.\n"
        "2. Search the Github Archive for market evidence data related to 'Domain_1_Control_Failure'.\n"
        "3. Synthesize both pieces of information into a comprehensive summary report."
    )
    
    await agent.process_task(prompt=prompt)
    agent.close()

if __name__ == "__main__":
    asyncio.run(run_agent())