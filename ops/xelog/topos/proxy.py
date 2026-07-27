# ops.xelog.topos.proxy
import asyncio
import json
from dataclasses import dataclass
from typing import Callable, Dict, Any, Optional

from atoa.event.llm.action import ActionEvent
from agent.disc.status import ConverStatus

from arch.contract.schema.resonance import BridgeEvent
from arch.contract.event.bus import AsyncEventBus
from arch.contract.interface import IPhaseAtor, IPhaseField
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType, PhaseField
from arch.contract.event.network import EventTransductor
from arch.contract.event.next import next_id, next_phase_id

from phase.bind.client.engine.base import BaseEngine
from watcher.tracer.infra.router import InfraRouter
from watcher.plane.emitter import get_emitter

log = get_emitter("bound.proxy")

class AgentEventTransductor(EventTransductor[Any]):
    """네트워크 수신 패킷을 기저 인프라 제어 위상 신호(PsiEvent)로 정밀 변환"""
    
    def transduce(self, agent_event: Any, conversation_state: Any) -> PsiEvent:
        tag = "execute"
        carrier_type = CarrierType.LOCAL
        
        # 하이브리드 타입 검사: 로컬 인스턴스 형태 혹은 원격 원시 딕셔너리 형태 모두 유연하게 대응
        is_action_obj = isinstance(agent_event, ActionEvent)
        is_action_dict = isinstance(agent_event, dict) and "tool_name" in agent_event
        
        # 위험 감지 상태 조율 (에지 통제 분기점)
        if (is_action_obj or is_action_dict) and \
           conversation_state.execution_status == ConverStatus.WAITING_FOR_USER:
            tag = "await_confirmation"
            carrier_type = CarrierType.MODULATORY  # 외부 완충(인간 개입 지연) 채널 가동
            
        # 가공할 원시 카인드 식별 문자열 확보
        if isinstance(agent_event, dict):
            kind_str = agent_event.get("event_type", "RemoteNetworkEvent")
        else:
            kind_str = type(agent_event).__name__
            
        carrier = PsiCarrier(
            kind=kind_str,
            tag=tag,
            payload=agent_event,
            carrier_type=carrier_type,
            target_field=PhaseField.COHERENT
        )
        
        # 긴장 상태에 따른 위상 압력 제어 (위험 상태일 경우 임계 고위상 압력 10 부여)
        pressure_val = 10 if tag == "await_confirmation" else 0
        p_id = next_phase_id(topo=1, press=pressure_val)
        
        return PsiEvent(
            event_id=next_id(),
            parent_id=None,
            source_id="proxy.network.ingress",  # 출처를 네트워크 유입부로 명시
            scope="RUNTIME",
            tick=int(asyncio.get_event_loop().time()),
            carrier=carrier,
            phase_id=p_id
        )

class NotificationAtor(IPhaseAtor):
    """승인 대기 등의 특수 위상 캐리어를 청취하여 외부 알림 및 제어 스위치를 여는 독립 액터"""
    
    @property
    def actor_id(self) -> str:
        return "ator.slack_notifier"
        
    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus):
        if event.tag == "await_confirmation":
            payload = event.carrier.payload
            
            # 객체와 딕셔너리 추출 방식 일원화 안정화
            if isinstance(payload, dict):
                tool_name = payload.get("tool_name", "Unknown")
                risk = payload.get("security_risk", "High")
            else:
                tool_name = getattr(payload, 'tool_name', 'Unknown')
                risk = getattr(payload, 'security_risk', 'High')
            
            # 외부 시스템(Slack API, 공장 경고 램프 PLC 제어, 차량 CAN 신호) 연동 가능 영역
            log.info(f"[Infrastructure Bridge] ⚠️ Edge Safety Guard Triggered!")
            log.info(f"[Infrastructure Bridge] Tool Execution Blocked -> Name: {tool_name}, Security Risk Tier: {risk}")
            log.info(f"[Infrastructure Bridge] Event Ref Tracing ID: {event.event_id}, Phase Code: {hex(event.phase_id)}")

@dataclass
class ConvState:
    execution_status: ConverStatus = ConverStatus.RUNNING

class ProxyEngine(BaseEngine):
    def __init__(self, host_url: str, agent_usage: str, workspace_ref: str = None, session_api_key: str = None, router: Optional[InfraRouter] = None):
        self.router = router or InfraRouter(host_url, session_api_key)
        self.workspace = WorkspaceProxy(
            host_url=host_url, 
            workspace_ref=workspace_ref,
            session_api_key=session_api_key
        )
        self.bus = AsyncEventBus()
        self.bus.subscribe(NotificationAtor())
        self.transductor = AgentEventTransductor()
        self.conv_state = ConvState()

    def ask(self, prompt: str, callback: Callable[[BridgeEvent], None]):
        """기존 동기 시스템 호환성 레이어"""
        return asyncio.run(self._async_ask(prompt, callback))

    async def _async_ask(self, prompt: str, callback: Callable[[BridgeEvent], None]):
        conv_id = self.workspace.workspace_ref
        ws_path = self.router.get_ws_endpoint("events", conversation_id=conv_id)
        # ws_path = f"/sockets/events/{conv_id}?resend_mode=since"
        
        try:
            async with self.workspace.connect_ws(ws_path) as ws:
                request_msg = {"role": "user", "content": prompt}
                await ws.send(json.dumps(request_msg))
                async for response_str in ws:
                    event_data: Dict[str, Any] = json.loads(response_str)
                    if "code" in event_data and "detail" in event_data:
                        callback(BridgeEvent(
                            content=f"[Error {event_data['code']}] {event_data['detail']}", 
                            source="server_error"
                        ))
                        break
                    
                    asyncio.create_task(self._process_infrastructure_event(event_data))
                    content = event_data.get("content", "")
                    source = event_data.get("source", "remote_agent")
                    if content:
                        callback(BridgeEvent(content=content, source=source))
                        
                    if event_data.get("event_type") == "conversation_ended":
                        break
        except Exception as e:
            callback(BridgeEvent(content=f"Connection Error: {str(e)}", source="proxy_engine"))
        return ""

    async def _process_infrastructure_event(self, event_data: Dict[str, Any]):
        """원격 서버 패킷의 문맥을 해석하여 상태를 튜닝하고 PsiEvent를 버스에 발행"""
        if event_data.get("status") == "need_approval" or "tool_name" in event_data:
            self.conv_state.execution_status = ConverStatus.WAITING_FOR_USER
        else:
            self.conv_state.execution_status = ConverStatus.RUNNING

        if "tool_name" in event_data:
            agent_event = ActionEvent(
                tool_name=event_data["tool_name"],
                security_risk=event_data.get("security_risk", "High")
            )
        else:
            agent_event = event_data

        psi_event = self.transductor.transduce(agent_event, self.conv_state)
        await self.bus.publish(psi_event)