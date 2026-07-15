# bound.resolver.log
"""
@desc: 
- Resolves structured log rulesets into Elasticsearch JSON queries
- Aligned with the unified 'resolve()' interface pattern
"""
import json
from typing import List, Tuple, Dict, Any, Optional
from xphi.analyzer.parser.ruleset import ElasticDSLRulesetParser
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.log")

class LogResolver:
    """@desc: Translates high-level domain rulesets into deeply nested Elasticsearch queries."""
    
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
        self.parser = parser if parser is not None else ElasticDSLRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> List[Tuple[Dict[str, Any], str]]:
        """
        @desc: 리졸버 공통 인터페이스. 룰셋을 컴파일하여 (ES JSON Dict, Tag) 형태로 반환합니다.
        """
        try:
            parsed_queries = self.parser.parse_ruleset(self.ruleset, target_tags)
            
            resolved_results = []
            for q_obj, tag in parsed_queries:
                resolved_results.append((q_obj.to_dict(), tag))
                
            log.info(f"✅ Successfully resolved {len(resolved_results)} log queries.")
            return resolved_results
            
        except Exception as e:
            log.error(f"🚨 Failed to resolve log ruleset: {str(e)}")
            return []

if __name__ == "__main__":
    resolver = LogResolver()
    resolved_queries = resolver.resolve()
    log.info("=== [Resolved Elasticsearch JSON Queries] ===")
    for es_json, tag in resolved_queries:
        log.info(f"\n[Tag]: {tag}")
        log.info(json.dumps(es_json, indent=2))
        log.info("-" * 60)