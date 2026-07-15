# anchor.phase.ingress.proxy.traffic
## @lineage: bound.ingress.proxy.traffic
## @lineage: xphi.proxy.traffic
import asyncio
import math
import random
import time
from typing import Optional, Any
from arch.bound.sandbox.tunnel import TunnelFactory
from arch.proto.event.psi import PsiEvent, PsiCarrier, CarrierType, PhaseField
from watcher.plane.emitter import get_emitter

class TrafficOracle:
    """
    @role: Macro-to-Micro Phase Translator (Traffic & Load Boundary)
    @desc: 
        외부 세계의 혼란스러운 트래픽 폭증(Macro Entropy)을 관측하여, 
        내부 워커 노드들에게 가벼운 프로브(Probe)를 던져 부하(Tension)를 측정하고, 
        공명(Resonance, 내부 시스템이 한계에 도달함)이 확인되면 
        클러스터 스케일아웃(Rupture)을 강제하는 능동적 오라클입니다.
    """
    def __init__(self):
        # mq_url 파라미터가 완전히 제거되었습니다. 오라클은 통신망의 주소를 알 필요가 없습니다.
        self.mq: Optional[Any] = None
        self.log = get_emitter("oracle.traffic")
        
        ## 도메인 상태: 인프라 트래픽 (RPS: Requests Per Second)
        self.baseline_rps = 100.0
        self.current_rps = 100.0
        
        ## 공명 및 위상 전이 임계값
        self.resonance_threshold = 0.80  # 80% 이상의 내부 긴장도 상승 시 스케일아웃 전이
        self.last_internal_tension = 0.0

    async def connect(self):
        """@flow: TunnelFactory를 통해 시스템 전역 기본 매트릭스에 투명하게 연결합니다."""
        self.mq = await TunnelFactory.get_default()
        self.log.info("[Oracle] Boundary successfully bound to the Global Matrix.")

    async def _perceive_macro_entropy(self) -> float:
        """
        @observation: 외부 세계의 트래픽 변동성을 샘플링합니다.
        (실제 환경에서는 API Gateway의 Access Log나 Ingress 지표를 연동)
        """
        await asyncio.sleep(0.1)
        
        # 15% 확률로 바이럴 마케팅 등 트래픽 폭증(Spike) 발생
        if random.random() > 0.85:
            multiplier = random.uniform(1.5, 3.0)
            self.log.warn(f"📈 [Macro Entropy] Traffic Spike Detected! (x{multiplier:.1f})")
        else:
            # 평범한 요동 및 자연 감소 (회귀)
            multiplier = random.uniform(0.8, 1.1)
            
        self.current_rps = (self.current_rps * multiplier + self.baseline_rps) / 2
        return self.current_rps

    def _collapse_to_phase(self, raw_rps: float) -> float:
        """
        @transduction: 현실의 거대한 트래픽 수치를 0 ~ 2π 사이의 순환적 위상(Phase) 공간으로 압축합니다.
        평시는 0, 부하가 극심할수록 π/2 (1.0) 에 수렴합니다.
        """
        momentum = (raw_rps - self.baseline_rps) / self.baseline_rps
        normalized_vector = max(-1.0, min(1.0, momentum / 5.0)) # 5배 이상 폭증 시 최대치
        
        phase = math.asin(normalized_vector)
        if phase < 0:
            phase += 2 * math.pi
            
        return phase

    async def _emit_micro_probe(self, tick: int, phase_val: float):
        """
        @probe: 내부 워커 노드들에게 현재의 외부 트래픽 압력을 전달(가벼운 핑)합니다.
        수동적인 딕셔너리 하드코딩을 버리고 정규화된 PsiEvent를 사용합니다.
        """
        event = PsiEvent(
            event_id=f"probe-tick-{tick}",
            parent_id=None,
            source_id="topology.oracle",
            scope="GLOBAL",
            tick=tick,
            carrier=PsiCarrier(
                kind="LOAD_PROBE", 
                tag="TRAFFIC_PING",
                payload={
                    "target_nodes": ["worker_0", "worker_1", "worker_2"],
                    "phase": phase_val,
                    "coupling_strength": 0.05  # 가벼운 인지적 자극
                },
                carrier_type=CarrierType.DIFFUSE,
                target_field=PhaseField.COHERENT
            ),
            context={"intent": "infrastructure_resonance_check"}
        )
        
        # event.to_json()을 통해 안전한 직렬화 보장
        await self.mq.lpush("runtime:queue", event.to_json())
        self.log.info(f"[Oracle: Ψ-Probe] Micro-pulse emitted at phase {phase_val:.3f} rad.")

    async def _measure_tension_echo(self) -> float:
        """
        @echo_monitoring: 프로브를 맞은 워커 노드들이 내뿜는 내부 압력(큐 길이 등)의 총합을 확인합니다.
        """
        raw_tension = await self.mq.get("runtime:field:pressure")
        current_tension = float(raw_tension) if raw_tension else 0.0
        
        # 내부 스트레스의 변화량(Gradient, ΔΦ) 계산
        tension_gradient = current_tension - self.last_internal_tension
        self.last_internal_tension = current_tension
        
        return max(0.0, tension_gradient)

    async def _trigger_mean_field_shift(self, tick: int, phase_val: float, raw_rps: float):
        """
        @rupture_catalyst: 오토스케일링(Scale-out) 위상 전이 발동.
        단순히 트래픽이 높다고 발동하는 것이 아니라, 내부 노드들이 트래픽(Probe)에 
        '공명하여 비명을 지를 때(High Tension Gradient)'만 구조를 확장합니다.
        """
        required_replicas = max(3, int(raw_rps / 50)) # 50 RPS당 1대 꼴로 산정
        
        shift_event = PsiEvent(
            event_id=f"shift-tick-{tick}",
            parent_id=None,
            source_id="topology.oracle",
            scope="GLOBAL",
            tick=tick,
            carrier=PsiCarrier(
                kind="ATTRACT_PHASE", 
                tag="SCALE_OUT_RUPTURE",
                payload={
                    "target_nodes": ["cluster_autoscaler"],
                    "phase": phase_val,
                    "target_rps": raw_rps,
                    "required_replicas": required_replicas,
                    "coupling_strength": 1.5  # 구조를 찢어발기는 강한 중력
                },
                carrier_type=CarrierType.FIXED,
                target_field=PhaseField.GLOBAL
            ),
            context={"intent": "ontological_scale_out"}
        )
        
        await self.mq.lpush("runtime:queue", shift_event.to_json())
        self.log.warn(f"\n🔥 [SINGULARITY] Mean-Field Shift Executed! Scaling to {required_replicas} replicas (Traffic: {raw_rps:,.1f} RPS) 🔥\n")

    async def run_probing_cycle(self, interval: float = 2.0):
        """
        @lifecycle: 관측 -> 프로빙 -> 반향 측정 -> (조건 충족 시) 위상 전이
        """
        await self.connect()
        self.log.info(">>> Traffic Oracle Initiating Probing Sequence... <<<")
        
        tick = 1
        try:
            while True:
                # 1. 외부 트래픽 압력 인지
                raw_rps = await self._perceive_macro_entropy()
                phase_val = self._collapse_to_phase(raw_rps)
                
                # 2. 내부 노드에 미세 충격(Probe) 발송
                await self._emit_micro_probe(tick, phase_val)
                
                # 3. 내부 시스템이 충격을 흡수하고 텐션을 갱신할 시간 부여 (지연 평가 대기)
                await asyncio.sleep(interval / 2)
                
                # 4. 구조적 압력(Echo) 측정
                tension_gradient = await self._measure_tension_echo()
                
                # 공명 비율: 텐션 그라디언트 5.0 상승을 100% 공명으로 가정
                resonance_ratio = min(1.0, tension_gradient / 5.0) 
                
                status = "🟢 NORMAL" if raw_rps <= self.baseline_rps * 1.5 else "🔴 OVERLOAD"
                self.log.info(f"  └ Echo Received: Δ Tension = {tension_gradient:.2f} | Resonance = {resonance_ratio*100:.1f}% [{status}]")
                
                # 5. 임계점 돌파 시 물리적 인프라 스케일아웃(위상 전이) 실행
                if resonance_ratio >= self.resonance_threshold:
                    await self._trigger_mean_field_shift(tick, phase_val, raw_rps)
                    
                    # 새로운 인프라 구조(New Epoch)가 안착되었으므로, 기준점(Baseline)을 현재 트래픽으로 승격시킴
                    self.baseline_rps = raw_rps 
                    self.last_internal_tension = 0.0
                    
                    # 쿨다운 (스케일아웃 완료 및 안정화 대기)
                    self.log.info("[Oracle] Cooling down to absorb structural changes...")
                    await asyncio.sleep(5.0) 
                
                tick += 1
                await asyncio.sleep(interval / 2)
                
        except asyncio.CancelledError:
            self.log.warn("[Oracle] Probing sequence collapsed.")
        except Exception as e:
            self.log.error(f"[Oracle] Ontological Error: {e}")


if __name__ == "__main__":
    oracle = TrafficOracle()
    try:
        asyncio.run(oracle.run_probing_cycle(interval=2.0))
    except KeyboardInterrupt:
        print("\n[Oracle] Boundary Disconnected.")