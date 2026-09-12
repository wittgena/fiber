# fiber.dphi.edge.parser
import json
import logging
from typing import Dict, Any, Optional

log = logging.getLogger("edge.parser")

MCP_EXTRACTION_RULES = {
    # MCP 규격 내 직렬화된 텍스트가 위치한 선언적 경로
    "text_content_path": "result.content.0.text"
}

class McpStateTraverser:
    """혼합된 데이터 토폴로지(Dict, List, Object)를 지정된 경로(Path)로 안전하게 탐색합니다."""
    @staticmethod
    def resolve(obj: Any, path: str, default: Any = None) -> Any:
        if not path or obj is None:
            return default

        keys = path.split('.')
        current = obj
        for k in keys:
            if current is None:
                return default
            if isinstance(current, dict):
                current = current.get(k)
            elif isinstance(current, (list, tuple)):
                try:
                    current = current[int(k)]
                except (IndexError, ValueError):
                    return default
            else:
                current = getattr(current, k, None)
        return current if current is not None else default

class McpPayloadParser:
    """룰셋(Rule-based) 기반으로 MCP 페이로드에서 데이터를 안전하게 추출합니다."""
    
    @staticmethod
    def extract_telemetry(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not payload or not isinstance(payload, dict):
            return None

        try:
            # 1. 룰셋 경로를 기반으로 Traverser를 통해 깊은 뎁스의 텍스트 데이터를 한 번에 추출
            path = MCP_EXTRACTION_RULES["text_content_path"]
            text_data = McpStateTraverser.resolve(payload, path, "")
            
            if not text_data or not isinstance(text_data, str):
                return None

            # 2. 직렬화된 JSON 디코딩 및 telemetry 블록 반환
            parsed_data = json.loads(text_data)
            if isinstance(parsed_data, dict):
                return parsed_data.get("telemetry")

        except json.JSONDecodeError:
            # 일반 텍스트 응답일 경우 예외 없이 통과
            pass
        except Exception as e:
            log.warning(f"[McpParser] Payload extraction failed: {e}")
            
        return None