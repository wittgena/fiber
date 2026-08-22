# phase.agent.verse.event
## @lineage: agent.nexus.verse.event
## @lineage: nexus.agent.verse.event
## @lineage: meta.agent.verse.event
## @lineage: ops.chat.adapter.verse.event
from textual.message import Message

class VerseTriggerEvent(Message):
    """@desc: VerseSensor를 가동하라는 신호 (타이머 또는 명시적 액션에서 발행)"""
    def __init__(self, cause: str):
        super().__init__()
        self.cause = cause # 예: "periodic_tick", "action_feed", "system_alert"

class VerseVoicedEvent(Message):
    """@desc: LLM 추론이 완료되어 UI에 표시할 텍스트가 준비되었음을 알리는 신호"""
    def __init__(self, formatted_text: str):
        super().__init__()
        self.formatted_text = formatted_text