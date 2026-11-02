# fiber.dev.base.mcp.payload
## @lineage: fiber.infra.mcp.payload
from typing import Any, Dict, List, Optional, Union
from pydantic import Field
import orjson

from xphi.arch.model.surge.model import SurgeBaseModel, DynamicSurgeModel

class McpTextContent(SurgeBaseModel):
    type: str = "text"
    text: str

    @classmethod
    def from_data(cls, data: Any) -> "McpTextContent":
        if isinstance(data, str):
            return cls(text=data)
        return cls(text=orjson.dumps(data).decode("utf-8"))

class McpToolResult(DynamicSurgeModel):
    content: List[McpTextContent] = Field(default_factory=list)
    isError: bool = False

class McpErrorDetail(SurgeBaseModel):
    code: int
    message: str
    data: Optional[Any] = None

class McpToolCallParams(DynamicSurgeModel):
    name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)

class McpInitializeParams(DynamicSurgeModel):
    protocolVersion: str = "2026-07-28"
    capabilities: Dict[str, Any] = Field(default_factory=dict)
    clientInfo: Dict[str, str] = Field(
        default_factory=lambda: {"name": "fiber-gateway", "version": "1.0.0"}
    )

class McpRequest(DynamicSurgeModel):
    """MCP JSON-RPC 2.0 Ingress 요청 모델"""
    jsonrpc: str = "2.0"
    id: Union[int, str]
    method: str
    params: Dict[str, Any] = Field(default_factory=dict)

    def to_wire(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)

class McpResponse(DynamicSurgeModel):
    """MCP JSON-RPC 2.0 Egress 응답 모델"""
    jsonrpc: str = "2.0"
    id: Optional[Union[int, str]] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[McpErrorDetail] = None

    @property
    def is_error(self) -> bool:
        return self.error is not None

    def to_wire(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)

    def to_json(self) -> str:
        return self.model_dump_orjson(exclude_none=True)

    def extract_text(self, default: str = "{}") -> str:
        if self.is_error:
            raise RuntimeError(f"JSON-RPC Error [{self.error.code}]: {self.error.message}")
        if not self.result:
            return default
        content = self.result.get("content")
        if isinstance(content, list) and content and isinstance(content[0], dict):
            return content[0].get("text", default)
        return default

    def extract_data(self, parse_json: bool = True) -> Any:
        raw_text = self.extract_text()
        return orjson.loads(raw_text) if parse_json else raw_text

class McpPayloadBuilder:
    """MCP JSON-RPC 2.0 요청/응답 통합 빌더 및 파서"""
    def __init__(self, start_seq: int = 0):
        self._seq = start_seq

    def next_id(self) -> int:
        self._seq += 1
        return self._seq

    @classmethod
    def request(
        cls,
        req_id: Union[int, str],
        method: str,
        params: Optional[Union[Dict[str, Any], SurgeBaseModel]] = None,
        as_dict: bool = True,
    ) -> Union[Dict[str, Any], McpRequest]:
        req = McpRequest(id=req_id, method=method, params=params or {})
        return req.to_wire() if as_dict else req

    @classmethod
    def initialize(
        cls,
        req_id: Union[int, str] = 1,
        client_name: str = "fiber-gateway-e2e",
        protocol_version: str = "2026-07-28",
        as_dict: bool = True,
    ) -> Union[Dict[str, Any], McpRequest]:
        params = McpInitializeParams(
            protocolVersion=protocol_version,
            clientInfo={"name": client_name, "version": "1.0.0"},
        )
        return cls.request(req_id, "initialize", params=params, as_dict=as_dict)

    @classmethod
    def tool_call(
        cls,
        req_id: Union[int, str],
        tool_name: str,
        as_dict: bool = True,
        **arguments: Any,
    ) -> Union[Dict[str, Any], McpRequest]:
        params = McpToolCallParams(name=tool_name, arguments=arguments)
        return cls.request(req_id, "tools/call", params=params, as_dict=as_dict)

    def next_request(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request(self.next_id(), method, params=params, as_dict=True)

    def next_initialize(self, client_name: str = "fiber-gateway-e2e") -> Dict[str, Any]:
        return self.initialize(self.next_id(), client_name=client_name, as_dict=True)

    def next_tool_call(self, tool_name: str, **arguments: Any) -> Dict[str, Any]:
        return self.tool_call(self.next_id(), tool_name, as_dict=True, **arguments)

    @classmethod
    def result(
        cls,
        req_id: Union[int, str],
        result_data: Dict[str, Any],
        as_dict: bool = True,
    ) -> Union[Dict[str, Any], McpResponse]:
        resp = McpResponse(id=req_id, result=result_data)
        return resp.to_wire() if as_dict else resp

    @classmethod
    def tool_ok(
        cls,
        req_id: Union[int, str],
        data: Any,
        as_dict: bool = True,
    ) -> Union[Dict[str, Any], McpResponse]:
        tool_res = McpToolResult(content=[McpTextContent.from_data(data)], isError=False)
        resp = McpResponse(id=req_id, result=tool_res.model_dump())
        return resp.to_wire() if as_dict else resp

    @classmethod
    def error(
        cls,
        req_id: Optional[Union[int, str]],
        code: int,
        message: str,
        data: Optional[Any] = None,
        as_dict: bool = True,
    ) -> Union[Dict[str, Any], McpResponse]:
        err = McpErrorDetail(code=code, message=message, data=data)
        resp = McpResponse(id=req_id, error=err)
        return resp.to_wire() if as_dict else resp

    @classmethod
    def parse_request(cls, raw: Union[str, bytes, Dict[str, Any]]) -> McpRequest:
        data = orjson.loads(raw) if isinstance(raw, (str, bytes)) else raw
        return McpRequest.model_validate(data)

    @classmethod
    def parse_response(cls, raw: Union[str, bytes, Dict[str, Any]]) -> McpResponse:
        data = orjson.loads(raw) if isinstance(raw, (str, bytes)) else raw
        return McpResponse.model_validate(data)

    @classmethod
    def extract_data(cls, raw: Union[str, bytes, Dict[str, Any]], parse_json: bool = True) -> Any:
        return cls.parse_response(raw).extract_data(parse_json=parse_json)