# topos.gateway.logic.http
## @lineage: void.topos.gateway.logic.http
## @lineage: topos.edge.gateway.logic.http
## @lineage: edge.gateway.logic.http
## @lineage: fiber.gateway.logic.http
## @lineage: meta.logic.gate.http
## @lineage: logict.flow.gate.http
import httpx
import re
from typing import Any, Dict
from arch.gov.flow import PhaseFlow, Transduction
from arch.contract.registry.unified import contract
from watcher.plane.emitter import get_logger

log = get_logger('gate.http')

class GateHttp(Transduction):
    """HTTP 기반 Transductor의 공통 로직을 처리하는 베이스 클래스"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.base_url = "http://0.0.0.0:8000"
        self.client = httpx.Client(base_url=self.base_url, timeout=15.0)

    def _parse_instruction(self, instruction: str, payload: dict) -> tuple:
        """'POST /api/conversations/{id} ...' 형태의 문자열에서 Method와 Path를 추출하고 변수 주입"""
        match = re.match(r"(GET|POST|PUT|DELETE)\s+([/\w{}]+)", instruction)
        if not match:
            return "GET", "/"
        
        method, raw_path = match.groups()
        # payload의 값을 사용하여 path의 {id} 등을 치환
        path = raw_path.format(**payload)
        return method, raw_path, path


@contract.ator("post.transfer")
class PostTransfer(GateHttp):
    """@flow: 대상 시스템에 자극(Ψ)을 주입하여 상태 전이를 유도"""
    def _project(self, flow: PhaseFlow, ator_node: Any) -> dict:
        context = ator_node.spec.get("context", {})
        instruction = context.get("instruction", "POST /")
        payload = flow.payload
        
        method, raw_path, path = self._parse_instruction(instruction, payload)
        
        # Genesis, Injection 등에 사용할 Body Payload 구성 (실제로는 context에서 매핑 필요)
        req_body = context.get("request_body", {})
        
        log.info(f"  [HTTP Transductor] Injecting Ψ into {path}")
        
        try:
            response = self.client.post(path, json=req_body)
            response.raise_for_status()
            data = response.json()
            
            log.info(f"  [HTTP Transductor] State transition successful.")
            
            # 응답에서 ID 등을 추출하여 다음 노드를 위해 Payload에 병합
            extracted_id = data.get("id") or data.get("conversation_id")
            
            return {
                **payload,
                "conversation_id": extracted_id or payload.get("conversation_id", "probe-dummy-id"),
                "last_response": data,
                "http_status": response.status_code
            }
            
        except httpx.HTTPStatusError as e:
            log.error(f"  [HTTP Transductor] Fracture at {path}: {e.response.status_code}")
            return {
                **payload,
                "http_status": e.response.status_code,
                "last_failed_path": raw_path,
                "last_failed_method": "post"
            }
        except Exception as e:
            log.error(f"  [HTTP Transductor] Unhandled Fracture: {e}")
            return {
                **payload,
                "http_status": 500,
                "last_failed_path": raw_path,
                "last_failed_method": "post"
            }

@contract.ator("get.transfer")
class GetTransfer(GateHttp):
    """@flow: 대상 시스템의 변화된 상태(Φ′)를 관측"""
    def _project(self, flow: PhaseFlow, ator_node: Any) -> dict:
        context = ator_node.spec.get("context", {})
        instruction = context.get("instruction", "GET /")
        payload = flow.payload
        
        method, raw_path, path = self._parse_instruction(instruction, payload)
        
        log.info(f"  [HTTP Transductor] Tracing world line at {path}")
        
        try:
            response = self.client.get(path)
            response.raise_for_status()
            
            return {
                **payload,
                "last_response": response.json(),
                "http_status": response.status_code
            }
        except httpx.HTTPStatusError as e:
            log.error(f"  [HTTP Transductor] Observation fractured: {e.response.status_code}")
            return {
                **payload,
                "http_status": e.response.status_code,
                "last_failed_path": raw_path,
                "last_failed_method": "get"
            }