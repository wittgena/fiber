# fiber.phase.scope.resolver.log
## @lineage: xphi.arch.contract.resolver.log
import json
from typing import List, Tuple, Dict, Any, Optional, Generic, TypeVar
from xphi.xor.parser.ruleset.stream import ElasticDSLRulesetParser
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("resolver.log")

T = TypeVar('T')

class LogResolver(Generic[T]):
    """@desc: Translates high-level domain rulesets into deeply nested queries or compiled engines."""
    
    DEFAULT_RULESET = {
        "global_config": {
            "base_query": {"environment": "production"},
            "noise_exclusions": {"tags": ["debug", "load-test"], "service": "test-runner"}
        },
        "targets": [
            {"tag": "ingestor-memory-critical", "condition": {"service": "ingest.issue", "level": "ERROR"}, "keywords": [{"OR": ["OOM", "memory leak", "heap dump"]}], "apply_exclusions": True},
            {"tag": "gateway-auth-anomaly", "condition": {"service": "api_gateway"}, "keywords": [{"AND": ["unauthorized", "token"]}, {"OR": ["expired", "invalid signature"]}], "apply_exclusions": True}
        ]
    }

    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None):
        self.ruleset = ruleset if ruleset is not None else self.DEFAULT_RULESET
        # 기본값은 ES 기반이나 주입되는 파서에 따라 유연히 대응
        self.parser = parser if parser is not None else ElasticDSLRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> T:
        """
        @desc: 리졸버 공통 인터페이스. 주입된 파서에 따라 
               1. List[Tuple[Dict, str]] (ES) 
               2. CompiledRuleEngine (Stream) 등을 반환합니다.
        """
        try:
            resolved_result = self.parser.parse_ruleset(self.ruleset, target_tags)
            log.info(f"✅ Successfully resolved ruleset topology via {self.parser.__class__.__name__}.")
            return resolved_result
            
        except Exception as e:
            log.error(f"🚨 Failed to resolve ruleset: {str(e)}")
            return None