# xphi.xor.mock.response
## @lineage: xphi.scope.mock.response
"""
@phase: Mock Generation Boundary (API Responses)
@desc: Simulates MCP tool orchestrations, Websocket flows, and ResponsesAPI payloads.
"""
import uuid
import time
from typing import List
from bound.surface.legacy.openai.types import ResponsesAPIResponse

def create_mock_mcp_response(
    input_text: str,
    tool_results: list = [],
    model: str = "mock-mcp-model"
) -> ResponsesAPIResponse:
    """
    `aresponses_api_with_mcp` 함수(MCP 오케스트레이션)를 통과한 
    멀티 턴(Multi-turn) 결과물을 흉내 냅니다.
    """
    return ResponsesAPIResponse(
        id=f"resp-mock-{uuid.uuid4().hex[:8]}",
        object="responses",
        created=int(time.time()),
        model=model,
        status="completed",
        input=input_text,
        output=[{
            "type": "text",
            "text": "This is a mocked MCP response."
        }] + tool_results
    )

def create_mock_stream_iterator(chunks: List[str]):
    """
    `BaseResponsesAPIStreamingIterator` 또는 `CustomStreamWrapper`를 
    대체하여 Async For Loop 테스트를 지원합니다.
    """
    class MockAsyncIterator:
        def __init__(self, data: List[str]):
            self.data = data
            self.index = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.index >= len(self.data):
                raise StopAsyncIteration
            
            chunk = self.data[self.index]
            self.index += 1
            # 실제 스트리밍 환경 모사를 위한 약간의 지연
            import asyncio
            await asyncio.sleep(0.01) 
            return chunk

    return MockAsyncIterator(chunks)