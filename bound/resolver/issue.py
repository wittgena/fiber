# bound.resolver.issue
"""
@desc: 
- Resolves structured issue rulesets into GitHub Search string queries.
- Aligned with the unified 'resolve()' interface pattern.
"""
from typing import List, Tuple, Dict, Any, Optional
from xphi.analyzer.parser.ruleset import LuqumRulesetParser
from watcher.plane.emitter import get_emitter

log = get_emitter("resolver.issue")

class IssueResolver:
    """@desc: Translates high-level domain rulesets into GitHub Search string queries."""
    
    DEFAULT_RULESET = {
        "global_config": {
            "base_query": {"is": "open", "language": "python"},
            "noise_exclusions": {"label": ["good first issue", "frontend", "ui", "documentation", "design"]}
        },
        "targets": [
            {"tag": "ai-memory-fragmentation", "condition": {"label": "bug"}, "keywords": [{"AND": ["agent"]}, {"OR": ["memory leak", "context loss", "state fragmentation", "memory corruption"]}], "apply_exclusions": False},
            {"tag": "topology-workflow", "condition": {"label": "bug"}, "keywords": [{"OR": ["workflow", "DAG", "execution graph"]}, {"OR": ["infinite loop", "circular dependency", "dead end"]}], "apply_exclusions": False},
            {"tag": "bounty-core-backend", "condition": {"label": "bug-bounty"}, "keywords": [], "apply_exclusions": True}
        ]
    }

    def __init__(self, ruleset: Optional[Dict[str, Any]] = None, parser: Optional[Any] = None):
        """의존성 주입(DI)을 통해 외부 룰셋이나 파서를 오버라이드할 수 있도록 구성"""
        self.ruleset = ruleset if ruleset is not None else self.DEFAULT_RULESET
        self.parser = parser if parser is not None else LuqumRulesetParser()

    def resolve(self, target_tags: Optional[List[str]] = None) -> List[Tuple[str, str]]:
        """
        @desc: 리졸버 공통 인터페이스. 룰셋을 컴파일하여 (Query String, Tag) 형태로 반환합니다.
        """
        try:
            # Luqum 파서를 통해 룰셋을 안전한 쿼리 문자열로 변환
            resolved_queries = self.parser.parse_ruleset(self.ruleset, target_tags)
            
            log.info(f"✅ Successfully resolved {len(resolved_queries)} issue queries.")
            return resolved_queries
            
        except Exception as e:
            log.error(f"🚨 Failed to resolve issue ruleset: {str(e)}")
            return []

if __name__ == "__main__":
    resolver = IssueResolver()
    SEED_QUERIES = resolver.resolve()

    log.info("=== [Generated GitHub SEED_QUERIES] ===")
    for query, tag in SEED_QUERIES:
        log.info(f"\n[Tag]: {tag}")
        log.info(f"[Query]: {query}")
        log.info("-" * 60)