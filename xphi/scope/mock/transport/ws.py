# xphi.scope.mock.transport.ws
"""
@phase: Mock Generation Boundary (WebSocket)
@desc: Simulates bidirectional WebSocket connections and OpenAI Realtime API events without actual network I/O.
"""
import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

class MockWebSocketConnection:
    """@desc: A unified mock object that can replace both client-side WebSockets"""
    def __init__(self, pre_programmed_responses: Optional[List[Any]] = None):
        self.sent_messages: List[Any] = []
        self._receive_queue: asyncio.Queue = asyncio.Queue()
        self.closed: bool = False
        
        # @desc: Pre-loads the queue with fake messages that the server will respond with when the test starts.
        if pre_programmed_responses:
            for resp in pre_programmed_responses:
                self._receive_queue.put_nowait(resp)

    async def send(self, data: Any) -> None:
        """@desc: Captures the action of sending binary or text data to the backend"""
        self.sent_messages.append(data)

    async def send_text(self, data: str) -> None:
        """@desc: Captures the action of sending text data to the client"""
        self.sent_messages.append(data)

    async def recv(self) -> Any:
        """@desc: Returns a pending message from the queue. If the queue is empty, it asynchronously awaits incoming data"""
        if self.closed:
            raise Exception("Mock WebSocket connection is already closed")
            
        data = await self._receive_queue.get()
        if isinstance(data, Exception):
            raise data
        return data

    async def receive_text(self) -> str:
        """@desc: Operates identically to recv() but strictly guarantees a text return value"""
        data = await self.recv()
        return str(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """@desc: Terminates the connection and unblocks any pending queue receivers"""
        self.closed = True
        # @phase: Teardown
        # @desc: Inject an exception into the queue to awaken any blocked recv() calls.
        await self._receive_queue.put(Exception(f"Connection closed: {code} - {reason}"))

    def put_incoming_message(self, message: Any) -> None:
        """@desc: A utility method used by external sources (like test scripts) to inject messages into the WebSocket in real-time"""
        self._receive_queue.put_nowait(message)

class MockWebSocketContextManager:
    """
    @desc: An asynchronous context manager designed to mock the `async with websockets.connect(...)` syntax.
    """
    def __init__(self, mock_ws: MockWebSocketConnection):
        self.mock_ws = mock_ws

    async def __aenter__(self) -> MockWebSocketConnection:
        return self.mock_ws

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self.mock_ws.closed:
            await self.mock_ws.close()

def create_mock_realtime_event(event_type: str, **kwargs) -> str:
    """@desc: Generates a mock event conforming to the OpenAI Realtime API specifications and returns it as a JSON string"""
    event = {
        "type": event_type,
        "event_id": f"evt_mock_{uuid.uuid4().hex[:8]}"
    }
    event.update(kwargs)
    return json.dumps(event)