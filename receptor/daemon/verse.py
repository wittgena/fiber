# fiber.receptor.daemon.verse
## @lineage: fiber.kernel.daemon.verse
## @lineage: phase.kernel.daemon.verse
import os
import asyncio
import random
import urllib.request
import re
import textwrap
from typing import Callable, Optional, Dict, Any

from xphi.arch.local.llm import LLMEngine
from xphi.arch.contract.registry.unified import contract
from xphi.kernel.daemon.base import AbstractDaemon

from xphi.kernel.daemon.bootstrap import TOPIC_BUS_STREAM, KEY_HEARTBEAT_PATTERN
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter('verse.daemon')

class ContextSensor:
    """[1] 외부 환경 및 내부 시스템 메트릭을 감지하여 상황(Context)을 수집하는 클래스"""
    def __init__(self, tunnel, location: str = "Seoul"):
        self.tunnel = tunnel  # [개선] DI 주입을 통해 TunnelFactory 직접 호출 제거
        self.location = location

    async def fetch_weather(self) -> str:
        try:
            url = f"https://wttr.in/{self.location}?format=%c+%C+%t"
            req = urllib.request.Request(url, headers={'User-Agent': 'curl/7.68.0'})
            return await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=3).read().decode('utf-8').strip())
        except Exception:
            return "Void"

    async def sense_environment(self, cause: str) -> Dict[str, Any]:
        try:
            # 활성 상태를 나타내는 Heartbeat 기반 노드 카운트
            keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
            node_count = len(keys)
            
            # 통합 Stream(xlen)의 부하를 측정
            try:
                stream_len = await self.tunnel.state_store.xlen(TOPIC_BUS_STREAM)
            except Exception:
                stream_len = 0
                
            tension = stream_len / (node_count if node_count > 0 else 1)
        except Exception as e:
            log.warning(f"Failed to fetch metrics from tunnel: {e}")
            node_count, stream_len, tension = 12, 180, 15.0 

        return {
            "Weather": await self.fetch_weather(),
            "Nodes": node_count,
            "Tension": tension,
            "Persona": random.choice(["Architect", "Oracle", "Worker"]),
            "TriggeredBy": cause,
            "Symbol": "AuraResonance"
        }


class CognitiveEngine:
    PERSONAS = {
        "Architect": {
            "identity": "You are the 'Architect' who observes the structure of the system.",
            "normal": "Use a calm, intellectual, and polite tone. Show satisfaction with the arrangement.",
            "crisis": "You are highly sensitive to system load. Use an analytical, vigilant tone.",
            "color": "\033[94m" # Blue
        },
        "Oracle": {
            "identity": "You are the 'Oracle' who reads the flow of the universe.",
            "normal": "Use a mysterious, poetic, and polite tone. Sing of peace using metaphors.",
            "crisis": "Use an ominous, prophetic tone warning of great ruin or rapid tension shifts.",
            "color": "\033[95m" # Magenta
        },
        "Worker": {
            "identity": "You are a diligent 'Worker' who carries data.",
            "normal": "Use a tired but calm, everyday polite tone. Show relief that the system is running safely.",
            "crisis": "Use an urgent and clearly panicked polite tone due to the overflowing streams of data.",
            "color": "\033[93m" # Yellow
        }
    }

    def __init__(self, resolved_model: str, output_hook: Optional[Callable] = None):
        self.resolved_model = resolved_model
        self.output_hook = output_hook
        self.tension_threshold = 10.0
        log.info(f"[CognitiveEngine] Bootstrapping bounded to model: {self.resolved_model}")
        self.engine = LLMEngine() 

    async def process_signals(self, signals: Dict[str, Any]):
        persona_name = signals.get("Persona", "Worker")
        tension = float(signals.get("Tension", 0.0))
        p_config = self.PERSONAS.get(persona_name, self.PERSONAS["Worker"])
        
        # 1. 프롬프트 조립 (Assembler 역할)
        tone = p_config["crisis"] if tension > self.tension_threshold else p_config["normal"]
        messages = [
            {"role": "system", "content": f"You are the core cognitive decision engine.\n{p_config['identity']} {tone}"},
            {"role": "user", "content": f"Resolve intention for symbol: {signals['Symbol']}\n\n[Surface Signals]: {signals}\n\n[Instruction]\nAnswer in 1-2 natural sentences in Korean (polite form). Output pure dialogue only without stage directions. Express numbers metaphorically."}
        ]

        # 2. LLM 추론 (Client 역할)
        try:
            log.info(f"Projecting to LLM Engine ({self.resolved_model})...")
            response = await asyncio.to_thread(self.engine.chat, system_prompt=messages[0]['content'], user_prompt=messages[1]['content'])
        except Exception as e:
            log.error(f"[CognitiveEngine] LLM Failed: {e}")
            response = "Only the subtle vibrations of the system can be felt in the silence."

        # 3. 데이터 정제 및 관찰자 출력 (Observer 역할)
        dialogue = " ".join(" ".join(re.sub(r'[\(\{\[].*?[\)\}\]]', '', response).strip().splitlines()).split())
        await self._emit_output(signals, persona_name, dialogue, p_config["color"])

    async def _emit_output(self, signals: Dict[str, Any], persona: str, dialogue: str, color_code: str):
        if self.output_hook:
            msg = f"[{persona}] ({signals['TriggeredBy']}) : {dialogue}"
            await self.output_hook(msg) if asyncio.iscoroutinefunction(self.output_hook) else self.output_hook(msg)
        else:
            reset = "\033[0m"
            indented_text = textwrap.fill(f"\"{dialogue}\"", width=50).replace("\n", "\n   ")
            
            log.info("\n")
            log.info(f" 🌍 Macro(Weather) : {signals.get('Weather')}")
            log.info(f" 🧠 Symbol(Event)  : {signals.get('Symbol')} [Cause: {signals.get('TriggeredBy')}]")
            log.info("─" * 55)
            log.info(f" {color_code}❖ The {persona}{reset} : {indented_text}\n")


# =========================================================================
# 3. Verse Daemon (Top-Level Controller)
# =========================================================================
@contract.daemon("verse")
class VerseDaemon(AbstractDaemon):
    """센서와 엔진을 묶어 주기적으로 생태계를 구동하는 자율(Autonomous) 데몬"""
    
    def __init__(self, ctx):
        super().__init__("VerseDaemon")
        self.ctx = ctx
        
        # [개선] 런타임 컨텍스트(ctx)에서 터널 추출 주입
        tunnel = getattr(self.ctx, 'tunnel', None)
        if not tunnel:
            raise RuntimeError(f"[{self.name}] Tunnel dependency is missing in RuntimeContext.")
            
        self.sensor = ContextSensor(tunnel=tunnel)
        
        # 환경변수 기반 유연한 모델/주기 설정 지원
        resolved_model = os.getenv("VERSE_LLM_MODEL", "local-cognitive-engine")
        self.interval_seconds = float(os.getenv("VERSE_TICK_INTERVAL", "30.0"))
        self.engine = CognitiveEngine(resolved_model=resolved_model)

    async def run(self):
        log.info(f"🌌 [{self.name}] Initiating Cognitive Ecosystem (Interval: {self.interval_seconds}s)")
        
        try:
            # AbstractDaemon의 self.running 속성을 활용하여 통일된 생명주기 관리
            while self.running:
                signals = await self.sensor.sense_environment(cause="periodic_tick")
                await self.engine.process_signals(signals)
                await asyncio.sleep(self.interval_seconds)
                
        except asyncio.CancelledError:
            log.info(f"[{self.name}] Cancel signal received. Suspending cognitive loop.")
        except Exception as e:
            log.error(f"[{self.name}] Fatal anomaly detected in Verse loop: {e}", exc_info=True)
        finally:
            log.info(f"[{self.name}] Verse Daemon safely evaporated.")