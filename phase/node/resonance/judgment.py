# phase.node.resonance.judgment
## @lineage: watcher.ator.resonance.judgment
## @lineage: arch.topos.ator.resonance.judgment
## @lineage: phase.ator.resonance.judgment
## @lineage: phase.bind.resonance.judgment
## @lineage: phase.resonance.judgment
## @lineage: swarm.resonance.judgment
## @lineage: hub.residue.resonance.judgment
## @lineage: xphi.resonance.judgment
## @lineage: cognitive.resonance.judgment
## @lineage: cognitive.xphi.resonance.judgment
## @lineage: meta.transcript.resonance.judgment
## @lineage: cognitive.transcript.resonance.judgment
## @lineage: xphi.transcript.resonance.judgment
import asyncio
from arch.contract.registry.unified import contract
from arch.model.phase.flow import Judgment

@contract.ator("resonance.judgment")
class ResonanceJudgment(Judgment):
    """
    [Pure Router] 
    RuleWorkflow의 데코레이터가 하던 역할을 대체합니다.
    어떤 인프라(LlamaIndex 등)에도 의존하지 않고 오직 상태(Tension/Status)에 따라 위상 궤적을 틉니다.
    """
    def judge(self, flow, rules):
        # 1. 이전 노드에서 발생한 Tension이나 Status 파악
        status = flow.payload.get("status", "success")
        tension_level = flow.payload.get("tension", 0.0)
        
        # 2. 파열(Rupture) 임계치 초과 시 강제 재진입(Retry/Rupture) 궤적 발동
        if tension_level > 0.8:
            status = "rupture"
            
        # 3. 선언된 XPHI 룰에 따라 다음 노드 반환 (기본값: 시스템 종료/UGA)
        next_node = rules.get(status, "UGA")
        
        return next_node