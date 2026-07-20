# xor.scope.mock.completion
## @lineage: xphi.xor.mock.completion
## @lineage: xphi.scope.mock.completion
"""
@phase: Mock Generation Boundary (Completion)
@desc: Provides pure factory functions to generate deterministic completion responses without LiteLLM dependencies.
"""
import uuid
import time
from typing import Any, Dict, List, Optional
from bound.adapter.switch.params import ModelResponse, Choices, Message

def create_mock_completion(
    content: str, 
    model: str = "mock-gpt-4", 
    role: str = "assistant",
    **kwargs
) -> ModelResponse:
    """
    일반적인 텍스트 응답(Chat Completion) 객체를 생성합니다.
    다운스트림의 test_agent_completion() 등에서 사용됩니다.
    """
    mock_msg = Message(role=role, content=content, **kwargs)
    mock_choice = Choices(index=0, message=mock_msg, finish_reason="stop")
    
    return ModelResponse(
        id=f"chatcmpl-mock-{uuid.uuid4().hex[:8]}",
        model=model,
        choices=[mock_choice],
        created=int(time.time()),
        object="chat.completion"
    )

def create_mock_tool_call(
    tool_name: str, 
    arguments: Dict[str, Any], 
    model: str = "mock-gpt-4"
) -> ModelResponse:
    """
    Function/Tool Calling 응답을 시뮬레이션합니다.
    OpenHands의 Agent Tool Use 루프 검증에 필수적입니다.
    """
    import json
    from bound.adapter.switch.params import ChatCompletionMessageToolCall, Function
    
    tool_call = ChatCompletionMessageToolCall(
        id=f"call_mock_{uuid.uuid4().hex[:8]}",
        type="function",
        function=Function(name=tool_name, arguments=json.dumps(arguments))
    )
    
    mock_msg = Message(role="assistant", content=None, tool_calls=[tool_call])
    mock_choice = Choices(index=0, message=mock_msg, finish_reason="tool_calls")
    
    return ModelResponse(
        id=f"chatcmpl-mock-{uuid.uuid4().hex[:8]}",
        model=model,
        choices=[mock_choice],
        created=int(time.time()),
        object="chat.completion"
    )