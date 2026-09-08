# fiber.infra.protocol.agent
import sys
import json
import logging
import asyncio
from typing import Dict, Any, Optional

"""Module-Level I/O Hijacking (STDOUT 원천 봉쇄)"""
_REAL_STDOUT = sys.stdout
sys.stdout = sys.stderr


"""LEGACY PROTOCOL: Synchronous & Blocking (For 'ephemeral' & 'linear' modes)"""
class AgentProtocol:
    """@desc: 레거시(동기식) 워커를 위한 베이스 클래스 - 순차적 처리 및 YIELD 시 Blocking 발생"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        
        logging.basicConfig(
            stream=sys.stderr, 
            level=logging.INFO, 
            format=f"%(asctime)s [%(levelname)s] [{self.agent_name}] %(message)s"
        )
        self.log = logging.getLogger(self.agent_name)

    """Single Point of Egress"""
    def _emit_rpc_message(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = json.dumps(message) + "\n"
            _REAL_STDOUT.write(raw_out)
            _REAL_STDOUT.flush()
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message: 
                self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    """표준 JSON-RPC Message Builders"""
    def send_response(self, req_id: Any, result: Any):
        self._emit_rpc_message({"id": req_id, "result": result})

    def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        self._emit_rpc_message({"id": req_id, "error": err_obj})

    def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self._emit_rpc_message(msg)

    def serve_forever(self):
        """무한 입력 대기 루프 (동기식 - 한 줄을 처리할 때까지 다음 입력을 받지 못함)"""
        self.log.info(f"Sync Agent '{self.agent_name}' Ignited. Listening on stdin...")
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
                self._route_request(payload)
            except json.JSONDecodeError:
                self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Internal Fracture: {e}", exc_info=True)
                self.send_error(payload.get("id") if isinstance(payload, dict) else None, -32603, "Internal Server Error")

    def _route_request(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})

        if method == "initialize":
            self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
        elif method == "tools/list":
            self.handle_tools_list(req_id)
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            meta = params.get("_meta", {})
            try:
                self.handle_tools_call(req_id, tool_name, arguments, meta)
            except Exception as e:
                self.log.error(f"Execution Fault in '{tool_name}': {e}", exc_info=True)
                self.send_error(req_id, -32000, str(e))
        else:
            self.send_error(req_id, -32601, f"Unknown method: {method}")

    """Abstract Handlers (Override these in subclasses)"""
    def handle_tools_list(self, req_id: Any):
        self.send_response(req_id, {"tools": []})

    def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")


"""ADVANCED PROTOCOL: Asynchronous & Multiplexing (For 'multiplex' mode)"""
class AsyncAgentProtocol:
    """@desc: 모던(비동기) 워커를 위한 베이스 클래스 - 코루틴 라우팅 및 Non-blocking I/O 지원"""
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        
        logging.basicConfig(
            stream=sys.stderr, 
            level=logging.INFO, 
            format=f"%(asctime)s [%(levelname)s] [ASYNC|{self.agent_name}] %(message)s"
        )
        self.log = logging.getLogger(self.agent_name)
        
        # 출력 섞임 방지용 비동기 락
        self._stdout_lock = asyncio.Lock()
        
        # I/O 스트림 객체 (루프 시작 시 초기화)
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def _initialize_streams(self):
        """파이썬 표준 STDIN/STDOUT을 비동기 스트림으로 래핑합니다."""
        loop = asyncio.get_running_loop()
        self._reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(self._reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        
        # 내부적으로 hijacking 된 _REAL_STDOUT 객체를 그대로 사용합니다.
        w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, _REAL_STDOUT)
        self._writer = asyncio.StreamWriter(w_transport, w_protocol, self._reader, loop)

    """Single Point of Egress (Async)"""
    async def _emit_rpc_message_async(self, message: Dict[str, Any]):
        message["jsonrpc"] = "2.0"
        try:
            raw_out = (json.dumps(message) + "\n").encode('utf-8')
            
            # 동시 출력 시 스트림 파편화 방지 (Egress Mutex)
            async with self._stdout_lock:
                self._writer.write(raw_out)
                await self._writer.drain()
                
        except TypeError as e:
            self.log.error(f"Payload Serialization Failed: {e}")
            if "error" not in message: 
                await self.send_error(message.get("id"), -32603, "Internal Serialization Error")

    """표준 JSON-RPC Message Builders (Async)"""
    async def send_response(self, req_id: Any, result: Any):
        await self._emit_rpc_message_async({"id": req_id, "result": result})

    async def send_error(self, req_id: Any, code: int, message: str, data: Optional[Any] = None):
        err_obj = {"code": code, "message": message}
        if data is not None:
            err_obj["data"] = data
        await self._emit_rpc_message_async({"id": req_id, "error": err_obj})

    async def send_request(self, req_id: Any, method: str, params: Optional[Dict[str, Any]] = None):
        msg = {"id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        await self._emit_rpc_message_async(msg)

    async def serve_forever_async(self):
        """코루틴 기반 무한 입력 대기 루프 (입력을 읽는 즉시 태스크로 분리)"""
        await self._initialize_streams()
        self.log.info(f"Async Agent '{self.agent_name}' Ignited. Listening on async stdin...")
        
        while True:
            try:
                line_bytes = await self._reader.readline()
                if not line_bytes: # EOF
                    break
                    
                line = line_bytes.decode('utf-8').strip()
                if not line:
                    continue
                    
                payload = json.loads(line)
                
                # [핵심] 처리를 기다리지 않고(Non-blocking), 백그라운드 코루틴으로 발사합니다.
                asyncio.create_task(self._route_request_async(payload))
                
            except json.JSONDecodeError:
                await self.send_error(None, -32700, "Parse error: Invalid JSON")
            except Exception as e:
                self.log.error(f"Stream Reader Fracture: {e}", exc_info=True)

    async def _route_request_async(self, req: Dict[str, Any]):
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params", {})
        
        # 워커 재개(RESUME)를 위한 특수 액션 힌트 처리
        action = req.get("action") 

        try:
            if action == "RESUME":
                # 워커 내부에 특정 req_id를 기다리고 있는 로직이 있다면 여기서 처리 (예: Event Set)
                await self.handle_resume(req_id, req)
                return

            if method == "initialize":
                await self.send_response(req_id, {"protocolVersion": "2026-09-04", "capabilities": {}})
            elif method == "tools/list":
                await self.handle_tools_list(req_id)
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                meta = params.get("_meta", {})
                await self.handle_tools_call(req_id, tool_name, arguments, meta)
            else:
                await self.send_error(req_id, -32601, f"Unknown method: {method}")
                
        except Exception as e:
            self.log.error(f"Async Routing/Execution Fault: {e}", exc_info=True)
            await self.send_error(req_id, -32000, f"Execution failed: {str(e)}")

    """Abstract Async Handlers (Override these in subclasses)"""
    async def handle_tools_list(self, req_id: Any):
        await self.send_response(req_id, {"tools": []})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        await self.send_error(req_id, -32601, f"Tool '{tool_name}' not implemented")
        
    async def handle_resume(self, req_id: Any, payload: Dict[str, Any]):
        """Multiplex 워커가 YIELD 후 다시 데이터를 받았을 때 호출되는 콜백"""
        self.log.warning(f"RESUME payload received for {req_id}, but handle_resume is not implemented.")