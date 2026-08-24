# phase.kernel.daemon.verse
## @lineage: dphi.node.daemon.verse
## @lineage: phase.node.daemon.verse
import asyncio
import random
import urllib.request
import re
import textwrap
from typing import Callable, Optional, Dict, Any

from xphi.arch.local.llm import LLMEngine

from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.kernel.daemon.bootstrap import TOPIC_BUS_STREAM, KEY_HEARTBEAT_PATTERN
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter('verse.daemon')

class ContextSensor:
    """[1] 외부 환경 및 내부 시스템 메트릭을 감지하여 상황(Context)을 수집하는 클래스"""
    def __init__(self, location: str = "Seoul"):
        self.location = location

    async def fetch_weather(self) -> str:
        try:
            url = f"https://wttr.in/{self.location}?format=%c+%C+%t"
            req = urllib.request.Request(url, headers={'User-Agent': 'curl/7.68.0'})
            return await asyncio.to_thread(lambda: urllib.request.urlopen(req, timeout=3).read().decode('utf-8').strip())
        except Exception:
            return "Void"

    async def sense_environment(self, cause: str) -> Dict[str, Any]:
        tunnel = await TunnelFactory.get_default()
        try:
            # [개선] 레거시 node:* 대신 활성 상태를 나타내는 Heartbeat 기반 노드 카운트
            keys = await tunnel.keys(KEY_HEARTBEAT_PATTERN)
            node_count = len(keys)
            
            # [개선] 레거시 queue(llen) 대신 통합 Stream(xlen)의 부하를 측정
            try:
                stream_len = await tunnel.state_store.xlen(TOPIC_BUS_STREAM)
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
            # [개선] 메타포 변경: overflowing queues -> overflowing streams of data
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


class VerseDaemon:
    """[3] 센서와 엔진을 묶어 주기적으로 생태계를 구동하는 라이프사이클 컨트롤러"""
    def __init__(self, resolved_model: str):
        self._running = False
        self.sensor = ContextSensor()
        self.engine = CognitiveEngine(resolved_model=resolved_model)

    async def start(self, interval_seconds: float = 30.0):
        log.info(f"🌌 Starting Verse Daemon (Interval: {interval_seconds}s)")
        self._running = True
        
        try:
            while self._running:
                signals = await self.sensor.sense_environment(cause="periodic_tick")
                await self.engine.process_signals(signals)
                await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            log.info("Verse Daemon interrupted.")
        except Exception as e:
            log.error(f"Fatal error in Verse Daemon: {e}")

    def stop(self):
        self._running = False
        log.info("💤 Verse Daemon shutdown initiated.")