# xphi.analyzer.parser.ruleset
import json
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any, Optional
from luqum.parser import parse as luqum_parse
from luqum.tree import AndOperation, Group
from elasticsearch.dsl import Q
from watcher.plane.emitter import get_emitter

log = get_emitter("parser.ruleset")

class AbstractRulesetParser(ABC):
    @abstractmethod
    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> List[Tuple[Any, str]]:
        pass

class LuqumRulesetParser(AbstractRulesetParser):
    def _format_value(self, value: str) -> str:
        return f'"{value}"' if ' ' in value else value

    def _dict_to_ast(self, kv_dict: Dict[str, Any], prefix: str = "") -> Optional[Any]:
        if not kv_dict: return None
            
        terms = []
        for key, value in kv_dict.items():
            if isinstance(value, list):
                for v in value:
                    terms.append(f"{prefix}{key}:{self._format_value(v)}")
            else:
                terms.append(f"{prefix}{key}:{self._format_value(value)}")
                
        if not terms: return None
        return luqum_parse(" ".join(terms))

    def _keywords_to_ast(self, keywords: List[Dict[str, List[str]]]) -> Optional[Any]:
        if not keywords: return None
            
        components = []
        for group in keywords:
            if "AND" in group and group["AND"]:
                terms = [self._format_value(v) for v in group["AND"]]
                components.append("(" + " AND ".join(terms) + ")" if len(terms) > 1 else terms[0])
            elif "OR" in group and group["OR"]:
                terms = [self._format_value(v) for v in group["OR"]]
                components.append("(" + " OR ".join(terms) + ")" if len(terms) > 1 else terms[0])

        if not components: return None
        combined_str = " AND ".join(components)
        return Group(luqum_parse(combined_str))

    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> List[Tuple[str, str]]:
        global_config = ruleset.get("global_config", {})
        
        base_ast = self._dict_to_ast(global_config.get("base_query", {}))
        excl_ast = self._dict_to_ast(global_config.get("noise_exclusions", {}), prefix="-")
        
        compiled_queries = []
        
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            if target_tags and tag not in target_tags: continue
                
            cond_ast = self._dict_to_ast(target.get("condition", {}))
            kw_ast = self._keywords_to_ast(target.get("keywords", []))
            apply_exclusions = target.get("apply_exclusions", False)
            
            asts = []
            if base_ast: asts.append(base_ast)
            if cond_ast: asts.append(cond_ast)
            if apply_exclusions and excl_ast: asts.append(excl_ast)
            if kw_ast: asts.append(kw_ast)
            
            if asts:
                final_ast = AndOperation(*asts) if len(asts) > 1 else asts[0]
                compiled_queries.append((str(final_ast), tag))
                
        return compiled_queries


class ElasticDSLRulesetParser(AbstractRulesetParser):
    def _dict_to_q(self, kv_dict: Dict[str, Any], is_exclusion: bool = False) -> Optional[Q]:
        """
        @desc: Dict를 받아 Q 객체로 변환합니다. 
               is_exclusion이 True이면 ~Q (NOT) 연산자를 적용합니다.
        """
        if not kv_dict: return None
        
        queries = []
        for key, value in kv_dict.items():
            if isinstance(value, list):
                # 리스트는 terms 쿼리로 변환 (예: tags IN ["debug", "load-test"])
                q = Q("terms", **{key: value})
            else:
                # 단일 값은 term 쿼리로 변환 (예: environment == "production")
                q = Q("term", **{key: value})
                
            # 노이즈 필터인 경우 ~ (NOT 연산자)를 붙여줍니다.
            queries.append(~q if is_exclusion else q)
            
        if not queries: return None
        
        # 생성된 여러 Q 객체를 & (AND) 연산자로 결합하여 단일 Q 반환
        combined_q = queries[0]
        for q in queries[1:]:
            combined_q &= q
            
        return combined_q

    def _keywords_to_q(self, keywords: List[Dict[str, List[str]]], search_field: str = "message") -> Optional[Q]:
        """
        @desc: 키워드 리스트를 받아 로그의 message 필드를 검색하는 match_phrase 쿼리로 변환합니다.
        """
        if not keywords: return None
        
        components = []
        for group in keywords:
            if "AND" in group and group["AND"]:
                # AND 그룹: Q() & Q() 결합
                q = Q("match_phrase", **{search_field: group["AND"][0]})
                for val in group["AND"][1:]:
                    q &= Q("match_phrase", **{search_field: val})
                components.append(q)
                
            elif "OR" in group and group["OR"]:
                # OR 그룹: Q() | Q() 결합
                q = Q("match_phrase", **{search_field: group["OR"][0]})
                for val in group["OR"][1:]:
                    q |= Q("match_phrase", **{search_field: val})
                components.append(q)

        if not components: return None
        
        # 각 키워드 그룹(리스트의 요소)은 최종적으로 & (AND)로 묶임
        combined_q = components[0]
        for q in components[1:]:
            combined_q &= q
            
        return combined_q

    def parse_ruleset(self, ruleset: Dict[str, Any], target_tags: Optional[List[str]] = None) -> List[Tuple[Q, str]]:
        global_config = ruleset.get("global_config", {})
        
        base_q = self._dict_to_q(global_config.get("base_query", {}))
        excl_q = self._dict_to_q(global_config.get("noise_exclusions", {}), is_exclusion=True)
        
        compiled_queries = []
        
        for target in ruleset.get("targets", []):
            tag = target.get("tag")
            if target_tags and tag not in target_tags: continue
                
            cond_q = self._dict_to_q(target.get("condition", {}))
            kw_q = self._keywords_to_q(target.get("keywords", []))
            apply_exclusions = target.get("apply_exclusions", False)
            
            # 파이썬 객체인 Q를 결합하기만 하면, Elasticsearch DSL이 내부적으로 트리를 구성함
            final_q = None
            for q_obj in [base_q, cond_q, excl_q if apply_exclusions else None, kw_q]:
                if q_obj is not None:
                    final_q = q_obj if final_q is None else (final_q & q_obj)
            
            if final_q:
                compiled_queries.append((final_q, tag))
                
        return compiled_queries