# xphi.kernel.contract.validator
## @lineage: sphere.validator.contract
from __future__ import annotations
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Any

class ContractValidator:
    """flow.json을 기반으로 설정 페이로드의 정적 정합성을 검사"""
    def __init__(self, flow_map_path: Path):
        with open(flow_map_path, "r", encoding="utf-8") as f:
            self.flow_map = json.load(f)
            self.nodes = {}
            
            for group, nodes_list in self.flow_map.items():
                for node in nodes_list:
                    explicit_args = node.get("positional_args", [])
                    explicit_kwargs = node.get("contract", {}).get("explicit", {})
                    
                    # 우선순위 1: 명시적 이름 (kwargs or args)
                    node_name = explicit_kwargs.get("name", "")
                    if not node_name and explicit_args:
                        node_name = str(explicit_args[0])
                    # 우선순위 2: FQN 전체 보존 (Split 절대 금지)
                    if not node_name:
                        node_name = node["fqn"]
                        
                    self.nodes[node_name.lower()] = node
                    self.nodes[node["fqn"].lower()] = node

    def _resolve_type(self, c_type: str) -> Dict[str, Any]:
        c_type = c_type.lower()
        
        ## 완벽한 일치 (Exact Match)
        if c_type in self.nodes:
            return self.nodes[c_type]
            
        ## 접미사 일치 (Suffix Match)
        for registered_name, node_meta in self.nodes.items():
            if registered_name.endswith(f".{c_type}") or registered_name == c_type:
                return node_meta
                
        return None

    def validate(self, config: Dict[str, Any]):
        categories = ["kernel", "field", "watcher", "regime", "ators"]
        
        for cat in categories:
            target = config.get(cat)
            if not target: 
                continue
                
            items = target if isinstance(target, list) else [target]
            
            for item in items:
                if "type" not in item: 
                    continue
                    
                c_type = item["type"].lower()
                node_meta = self._resolve_type(c_type)
                
                if not node_meta:
                    raise ValueError(f"[StaticError] '{cat}' 카테고리의 타입 '{c_type}'이 flow.json에 존재하지 않습니다.")

                params = item.get("params", {})
                required_args = set(node_meta.get("shape_hints", {}).get("signature", {}).get("args", []))
                provided_args = set(params.keys())

                missing = required_args - provided_args - {'self', 'cls', 'config', 'kwargs'}
                if missing:
                    raise TypeError(f"[StaticError] '{c_type}' 초기화에 필요한 파라미터가 누락되었습니다: {missing}")