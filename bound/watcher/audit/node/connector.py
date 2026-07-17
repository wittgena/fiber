# bound.watcher.audit.node.connector
## @lineage: arch.topos.node.connector
## @lineage: phase.gov.node.connector
import asyncio
from typing import Optional
from arch.topos.node.gan import Message, GanNode
from watcher.plane.emitter import get_emitter
from watcher.kernel.state.spec import TransRule
from arch.topos.node.state import NodeType

log = get_emitter('node.connector')

class TensionAlert(Message):
    """@desc: Aura가 임계 텐션을 감지했을 때 펌프(DOM)에 방출하는 경고 이벤트"""
    def __init__(self, tension_level: float, persona: str, cause: str):
        super().__init__("aura_tension_alert", bubble=True)
        self.tension_level = tension_level
        self.persona = persona
        self.cause = cause

class NodeConnector(GanNode):
    """
    @desc: 제어 평면(GanNode)과 실행 평면(Topos Graph)을 잇는 단방향 시냅스.
    Aura의 상태 이벤트를 수신하여, Topos의 위상 변이(TransRule) 규칙으로 번역(Translate)한 뒤
    Redis/Queue 경계를 통해 주입(Inject)합니다.
    """
    def __init__(self, name: str, topos_queue: asyncio.Queue):
        super().__init__(name)
        # Topos의 Execution Plane으로 향하는 '경계(Boundary)' 포인터
        self.topos_queue = topos_queue 

    async def on_aura_tension_alert(self, message: TensionAlert):
        """Aura 트리에서 버블링된 텐션 경고를 수신"""
        log.warning(f"[{self.name}] ⚡ 시냅스 활성화: 텐션 {message.tension_level} 감지 ({message.persona})")
        
        # 1. 텐션 수치에 따른 위상 변이(Mutation) 전략 추론
        trans_rules = self._synthesize_mutation_rules(message.tension_level)
        
        if trans_rules:
            log.info(f"[{self.name}] 🧬 위상 변이 규칙(TransRule) 생성 완료. Topos 경계로 주입합니다.")
            await self._inject_to_topos_boundary(trans_rules, message.cause)

    def _synthesize_mutation_rules(self, tension: float) -> list[TransRule]:
        """텐션 임계치에 따라 시스템의 위상을 물리적으로 어떻게 바꿀지 결정"""
        rules = []
        if tension > 20.0:
            # [긴급] 무거운 'stable_core' 노드를 바이패스하고 가벼운 'legacy_symlink'로 위상 교체
            log.warning(f"[{self.name}] 🚨 임계 텐션 돌파. 바이패스 라우팅(Symlink) 위상을 강제 적용합니다.")
            rules.append(TransRule(target_node="stable_core", new_node="legacy_symlink", kind=NodeType.SYMLINK))
            
        elif tension > 10.0:
            # [경고] 로드가 몰리므로 워커 노드를 증설하는 방향의 위상 변이 (가정)
            log.info(f"[{self.name}] ⚠️ 중간 텐션. 보조 워커 노드 확장을 제안합니다.")
            rules.append(TransRule(target_node="worker_pool", new_node="expanded_worker_pool", kind=NodeType.CORE))
            
        return rules

    async def _inject_to_topos_boundary(self, rules: list[TransRule], cause: str):
        """
        GanNode의 공간에서 Topos의 공간(Redis/Queue)으로 데이터를 넘깁니다.
        여기서 '경계(Boundary)'를 넘는 행위가 일어납니다.
        """
        # ToposOrganizer가 이해할 수 있는 형태(Event, Payload)로 캡슐화
        mutation_payload = {
            "type": "EvolutionSignal",
            "rules": rules,
            "cause": cause
        }
        
        # 실제 구현에서는 RedisBoundary.emit() 또는 psi_queue.put()이 됩니다.
        await self.topos_queue.put(mutation_payload)
        log.info(f"[{self.name}] 💉 Topos Execution Plane에 진화(Evolution) 신호 주입 완료.")