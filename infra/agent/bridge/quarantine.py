# fiber.infra.agent.bridge.quarantine
## @lineage: fiber.a2a.bridge.quarantine
import json
import logging
from typing import Any, Dict

log = logging.getLogger("bridge.quarantine")

"""Declarative Ruleset"""
QUARANTINE_RULES = {
    "legacy-01": {
        # [Rule 2: Resume Extraction] YIELD 재개 시 특정 경로만 추출하여 단축 반환
        "resume_path": "params._meta.inputResponses",
        # [Rule 1: Data Sanitization] 값 교정 (클라이언트 오타 방어)
        "sanitizers": [
            {"path": "params.arguments.target_env", "match": "prod", "replace": "production"}
        ],
        # [Rule 3: Compatibility] 데이터 매핑/이동 (MCP 표준 -> 레거시 규격)
        "mappings": [
            {"src": "params._meta.user_id", "dst": "params.arguments.user_id"}
        ]
    },
    "margin-01": {
        # [Scrubbing] 레거시 스키마 크래시 방지를 위한 메타데이터 삭제
        "deletions": [
            "params._meta"
        ]
    }
}

class PayloadTraverser:
    """점 표기법(Dot-notation) 기반의 안전한 Dict 탐색/조작 엔진"""
    @staticmethod
    def resolve(obj: Dict, path: str, default: Any = None) -> Any:
        if not path or not isinstance(obj, dict): return default
        current = obj
        for k in path.split('.'):
            if not isinstance(current, dict) or k not in current:
                return default
            current = current[k]
        return current

    @staticmethod
    def set(obj: Dict, path: str, value: Any):
        if not path or not isinstance(obj, dict): return
        keys = path.split('.')
        current = obj
        for k in keys[:-1]:
            if k not in current or not isinstance(current[k], dict):
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value

    @staticmethod
    def delete(obj: Dict, path: str):
        if not path or not isinstance(obj, dict): return
        keys = path.split('.')
        current = obj
        for k in keys[:-1]:
            if k not in current or not isinstance(current[k], dict):
                return
            current = current[k]
        current.pop(keys[-1], None)

class IsolationAdapter:
    """기본 격리 어댑터: 코어망 표준을 따르는 에이전트를 위한 Pass-through"""
    def translate_ingress(self, payload: dict) -> dict:
        return payload
        
    def translate_egress(self, raw_output: str) -> dict:
        """[엄격한 규약] 레거시의 출력물이 유효한 JSON이 아니면 즉시 빠른 실패(Fail-Fast)"""
        line = raw_output.strip()
        if not line:
            raise RuntimeError("IPC Contract Violation: Agent closed stream unexpectedly.")
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            log.error(f"[Quarantine] STDOUT Pollution Detected: {line}")
            raise RuntimeError(f"IPC Contract Violation: Agent stdout is not valid JSON. (Output: {line[:50]}...)")


class DeclarativeIsolationAdapter(IsolationAdapter):
    """규칙 기반 범용 격리 어댑터 (LLM StateMapper 패턴 적용)"""
    def __init__(self, target_id: str):
        self.target_id = target_id
        self.rule = QUARANTINE_RULES.get(target_id, {})

    def translate_ingress(self, payload: dict) -> dict:
        if not self.rule:
            return payload

        # 1. Short-circuit Extraction (Resume 처리)
        resume_path = self.rule.get("resume_path")
        if resume_path:
            resume_data = PayloadTraverser.resolve(payload, resume_path)
            if resume_data:
                return resume_data  # 역직렬화된 Elicitation 응답만 즉시 반환

        # 2. Sanitizations (값 교정)
        for san in self.rule.get("sanitizers", []):
            val = PayloadTraverser.resolve(payload, san["path"])
            if val == san["match"]:
                PayloadTraverser.set(payload, san["path"], san["replace"])

        # 3. Mappings (값 복사/이동)
        for map_rule in self.rule.get("mappings", []):
            src_val = PayloadTraverser.resolve(payload, map_rule["src"])
            if src_val is not None:
                PayloadTraverser.set(payload, map_rule["dst"], src_val)

        # 4. Deletions (구형 에이전트 크래시 방지용 필드 삭제)
        for del_path in self.rule.get("deletions", []):
            PayloadTraverser.delete(payload, del_path)

        return payload

class QuarantineRegistry:
    """타겟 ID에 따라 적절한 격리 어댑터를 반환하는 팩토리"""
    @staticmethod
    def get_adapter(target_id: str) -> IsolationAdapter:
        if target_id in QUARANTINE_RULES:
            return DeclarativeIsolationAdapter(target_id)
        return IsolationAdapter()