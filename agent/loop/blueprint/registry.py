# fiber.agent.loop.blueprint.registry
## @lineage: fiber.agent.conver.loop.blueprint.registry
## @lineage: surgent.engine.blueprint.registry
## @lineage: surgent.agent.blueprint.registry
## @lineage: surgent.agent.protocol.blueprint.registry
from __future__ import annotations

import os
from typing import Optional, Dict, Tuple, Union
from enum import Enum

from fiber.agent.loop.blueprint.manifest import BlueprintType, BLUEPRINT_MANIFESTS

from xphi.arch.model.surge.blueprint import SurgeBlueprint, SurgeNode
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("context.blueprint")

# =====================================================================
# 1. Blueprint Assembly Factory
# =====================================================================
def build_blueprint(
    topology_name: str, 
    focus: str, 
    steps: list[dict], 
    instruction: str,          # 💡 Manifest에서 텍스트로 생성된 시스템 규칙을 직접 주입받음
    relations: str = "sequential",
    min_cognitive_score: int = 1
) -> Tuple[SurgeBlueprint, int]:
    nodes = []
    for i, step in enumerate(steps):
        action_name = step.get("action", "terminal")
        content = step.get("content", "")
        
        node = SurgeNode(
            id=f"step_{i+1}_{action_name}",
            intent=step.get("intent", "execute"),
            action=action_name,
            description=f"Use the '{action_name.lower()}' tool: {content}",
            expected_outcome=step.get("expected_outcome", f"Successfully completed {action_name} phase.")
        )
        
        # 💡 LLM이 Manifest에서 작성한 파라미터를 그대로 바인딩
        if "params_template" in step:
            node.params_template = step["params_template"]
            
        nodes.append(node)
    
    blueprint = SurgeBlueprint(
        topology_name=topology_name,
        focus=focus,
        depth_limit=len(steps),
        relations_constraint=relations,
        system_instructions=instruction.strip(),
        nodes=nodes
    )
    return blueprint, min_cognitive_score


# =====================================================================
# 2. Dynamic Registry Initialization (Memory Load)
# =====================================================================
BLUEPRINT_REGISTRY: Dict[BlueprintType, Dict[str, Tuple[SurgeBlueprint, int]]] = {}

for b_type, scenarios in BLUEPRINT_MANIFESTS.items():
    BLUEPRINT_REGISTRY[b_type] = {}
    for scenario_key, cfg in scenarios.items():
        # Manifest의 Dict 구조를 읽어들여 런타임 파이썬 객체로 조립
        blueprint, score = build_blueprint(
            topology_name=cfg["topology_name"],
            focus=cfg["focus"],
            steps=cfg["steps"],
            instruction=cfg["instruction"],
            relations=cfg.get("relations", "sequential"),
            min_cognitive_score=cfg.get("min_cognitive_score", 1)
        )
        BLUEPRINT_REGISTRY[b_type][scenario_key] = (blueprint, score)


# =====================================================================
# 3. Task Resolver Class
# =====================================================================
class TaskResolver:
    """
    Agent Launcher가 Blueprint를 호출할 때 사용하는 인터페이스.
    동작 중 에러 발생을 원천 차단하기 위해 순수 데이터로만 구성된 Registry를 조회합니다.
    """
    def __init__(self):
        log.debug("[TaskResolver] Unified DPHI Blueprint Registry dynamically loaded into memory from Declarative Manifests.")

    def resolve(self, category: Union[Enum, str], b_type: BlueprintType) -> Tuple[Optional[SurgeBlueprint], int]:
        cat_key = category.value if isinstance(category, Enum) else str(category)
        
        item = BLUEPRINT_REGISTRY.get(b_type, {}).get(cat_key)
        if item:
            return item[0], item[1]
            
        # JSON 포맷으로 생성된 외부 Blueprint 매니페스트에 대한 Fallback 로드
        fallback_path = f"./blueprints/{b_type.value}/{cat_key}.json"
        if os.path.exists(fallback_path):
            try:
                log.info(f"[TaskResolver] Dynamically loading declarative blueprint from {fallback_path}")
                # (TODO: JSON Parsing logic matching Manifest structure)
            except Exception as e:
                log.error(f"[TaskResolver] Failed to parse dynamic blueprint {cat_key}: {e}")
                
        return None, 1