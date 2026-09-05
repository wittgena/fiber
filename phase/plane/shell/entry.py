# fiber.phase.plane.shell.entry
## @lineage: fiber.phase.kernel.shell.entry
import sys
import asyncio
import json
import uuid
from dataclasses import asdict
from typing import Optional

from fiber.phase.plane.shell.surge import MarketSurge, LedgerSurge, EcoSurge
from fiber.phase.plane.shell.inject import PhysicsInjector

from xphi.arch.wasm.builder import WasmBuilder
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.arch.event.psi import PsiEvent, PsiCarrier, CarrierType
from xphi.arch.event.next import next_id
from xphi.kernel.ops.daemon.bootstrap import KEY_HEARTBEAT_PATTERN, TOPIC_BUS_STREAM
from xphi.kernel.dphi.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("shell.entry")

class LiveManifold:
    """통합 콘솔의 엔진: 터널, 브로커, 클러스터 상태를 캡슐화"""
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.broker = DphiBroker()
        self.worker_count = 0
        self.total_capacity = 0

    async def refresh_cluster_state(self) -> bool:
        """클러스터 내 살아있는 워커와 전체 슬롯(Capacity)을 실시간으로 스캔"""
        active_keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
        if not active_keys:
            self.worker_count, self.total_capacity = 0, 0
            return False
            
        self.worker_count, self.total_capacity = 0, 0
        for key in active_keys:
            raw_meta = await self.tunnel.get(key)
            if not raw_meta: continue
            try:
                meta_str = raw_meta.decode('utf-8') if isinstance(raw_meta, bytes) else raw_meta
                meta = json.loads(meta_str)
                if meta.get("role") == "worker":
                    self.worker_count += 1
                    self.total_capacity += int(meta.get("capacity", 0))
            except Exception: 
                pass
            
        return self.total_capacity > 0

    async def close(self):
        await self.broker.close()
        log.info("🔌 [Manifold] Broker detached. System resources released.")

class EcosystemShell:
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.manifold = LiveManifold(tunnel)
        self.running = True
        self.physics_injector = PhysicsInjector(tunnel)

    async def _print_header(self):
        await self.manifold.refresh_cluster_state()
        log.info("\n" + "="*75)
        log.info(" 🌌 [\033[95mDPHI Ecosystem Observatory & God-Mode Console\033[0m]")
        log.info(f" ⚙️  Status: \033[92mOnline\033[0m | Workers: {self.manifold.worker_count} | Max Capacity: {self.manifold.total_capacity}")
        log.info("="*75)
        log.info(" [Pre-flight & Observation]")
        log.info("  \033[96mbuild\033[0m        : WASM 아티팩트 컴파일 및 무결성 추적")
        log.info("  \033[96mnodes\033[0m        : 클러스터 Heartbeat 및 가용량 스캔")
        log.info("  \033[96mping\033[0m         : 브로드캐스트 에코 (system:ping)")
        log.info(" [Surgical Interventions]")
        log.info("  \033[93mreload <fqn>\033[0m : 핫-리로드 주입 (예: reload eco.mesh)")
        log.info("  \033[93mexec <cmd>\033[0m   : Master 노드 CLI 인텐트 주입 (예: exec align)")
        log.info(" [Application Layer Chaos (Surge)]")
        log.info("  \033[91msurge <type>\033[0m : 비즈니스/경제망 트래픽 폭주 (type: \033[90meco, ledger, market\033[0m)")
        log.info(" [Physical Layer Perturbations (Deep Kernel)]")
        log.info("  \033[95minject <type>\033[0m: 물리망 중력파 주사 (type: \033[90mkinetic <mass>, ator <id> <st>, rupture\033[0m)")
        log.info("  \033[90mexit / quit\033[0m  : 콘솔 종료")
        log.info("-" * 75)

    async def run(self):
        await self._print_header()
        while self.running:
            cmd_line = await asyncio.to_thread(input, "\033[93mnexus>\033[0m ")
            if not cmd_line.strip(): 
                continue
            await self.process_command(cmd_line.strip())

    async def process_command(self, cmd_line: str):
        parts = cmd_line.split()
        cmd = parts[0].lower()

        try:
            if cmd in ["exit", "quit"]:
                self.running = False
                
            elif cmd == "build":
                log.info("🔨 [Pre-flight] Initiating WasmBuilder Trace...")
                builder = WasmBuilder()
                await builder.trace()
                if builder.rupture_confirmed: 
                    log.error("❌ WASM Artifact build failed. Structural rupture confirmed.")
                else: 
                    log.info("✅ WASM Artifacts (dphi.wasm, dvm.wasm) are armed and ready.")
                    
            elif cmd == "nodes":
                await self.manifold.refresh_cluster_state()
                log.info(f"📊 Active Compute Market: {self.manifold.worker_count} Workers / {self.manifold.total_capacity} Slots.")
            
            # -------------------------------------------------------------
            # 비즈니스 계층 교란 라우팅 (Surge)
            # -------------------------------------------------------------
            elif cmd == "surge":
                if len(parts) < 2:
                    log.info("⚠️ Usage: surge <eco | ledger | market>")
                    return
                target = parts[1].lower()
                await self.manifold.refresh_cluster_state()
                cap = max(self.manifold.total_capacity, 40)
                
                if target == "eco":
                    await self.manifold.broker.update_policy("SYSTEM")
                    await EcoSurge(self.manifold.broker, cap).ignite()
                elif target == "ledger":
                    await self.manifold.broker.update_policy("SYSTEM")
                    await LedgerSurge(self.manifold.broker, cap).ignite()
                elif target == "market":
                    await self.manifold.broker.update_policy("STANDARD")
                    await MarketSurge(self.manifold.broker, cap).ignite()
                    await self.manifold.broker.update_policy("SYSTEM") # 복구
                else:
                    log.info(f"⚠️ Unknown surge target: {target}")

            # -------------------------------------------------------------
            # 물리 계층 주사 라우팅 (Inject)
            # -------------------------------------------------------------
            elif cmd == "inject":
                if len(parts) < 2:
                    log.info("⚠️ Usage: inject <kinetic | ator | rupture> [args]")
                    return
                target = parts[1].lower()
                
                if target == "kinetic":
                    size = int(parts[2]) if len(parts) > 2 else 100
                    await self.physics_injector.inject_kinetic_pressure(size)
                elif target == "ator":
                    if len(parts) < 4:
                        log.info("⚠️ Usage: inject ator <node_id> <ATTRACTOR|REFLECTOR>")
                        return
                    await self.physics_injector.inject_ator_mutation(parts[2], parts[3].upper())
                elif target == "rupture":
                    await self.physics_injector.inject_forced_rupture()
                else:
                    log.info(f"⚠️ Unknown inject target: {target}")

            # -------------------------------------------------------------
            # 외과적 개입 및 디버깅 유틸리티 (Reload, Exec, Ping)
            # -------------------------------------------------------------
            elif cmd == "reload":
                if len(parts) < 2: 
                    log.info("⚠️ Usage: reload <module_fqn>")
                    return
                await self._inject_reload(parts[1])
                
            elif cmd == "exec":
                if len(parts) < 2: 
                    log.info("⚠️ Usage: exec <cli_command> [args...]")
                    return
                await self._inject_cli_command(parts[1], parts[2:])
                
            elif cmd == "ping":
                await self._inject_ping()
                
            else:
                log.info(f"⚠️ Unknown command: {cmd}")
                
        except Exception as e:
            log.info(f"❌ Error executing command: {e}")

    # =========================================================================
    # [Out-of-Band System Override Methods]
    # =========================================================================
    async def _inject_reload(self, module_fqn: str):
        """Worker들의 Dispatcher가 수신할 핫리로드 이벤트 발행"""
        sync_event = PsiEvent(
            event_id=next_id(), parent_id=None, source_id="debug.shell", scope="GLOBAL", tick=0, phase_id=0, context={},
            carrier=PsiCarrier(kind="system:topology", tag="reload", payload={"module_fqn": module_fqn}, carrier_type=CarrierType.FIXED)
        )
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(sync_event.__dict__)})
        log.info(f"📡 Broadcasted topology reload for '\033[96m{module_fqn}\033[0m'")

    async def _inject_ping(self):
        """시스템 전체 PubSub에 ping 발행 후 응답 대기"""
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("system:echo")
        log.info("🦇 Emitting system:ping...")
        await self.tunnel.publish("system:ping", json.dumps({"source": "debug.shell"}))
        try:
            async with asyncio.timeout(2.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        data = json.loads(msg["data"])
                        log.info(f"  [ECHO] Received from: {data}")
        except asyncio.TimeoutError:
            log.info("⏳ Echo collection timeout (2.0s).")
        finally:
            await pubsub.close()

    async def _inject_cli_command(self, command: str, args: list):
        """Master Node의 Control Bus가 수신할 COMMAND 이벤트 발행 및 로그 트래킹"""
        task_id = f"debug-{uuid.uuid4().hex[:8]}"
        response_channel = f"res:{task_id}"
        
        payload = { 
            "_context": {
                "command": command, 
                "cli_args": args,
                "timeout": 30.0
            } 
        }
        
        trigger_event = PsiEvent(
            event_id=task_id, source_id="debug.shell", scope="GLOBAL", parent_id=None, tick=1, phase_id=0,
            carrier=PsiCarrier(kind="COMMAND", tag=command, payload=payload),
            context={"response_channel": response_channel}
        )
        
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe(response_channel)
        
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        log.info(f"🚀 Injected COMMAND '{command}' -> Stream. Listening on '{response_channel}'...")
        
        try:
            async with asyncio.timeout(30.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        result = json.loads(msg["data"])
                        status = result.get("status", "UNKNOWN")
                        color = "\033[92m" if status == "SUCCESS" else "\033[91m"
                        log.info(f"\n{color}[{status}]\033[0m {result.get('summary', '')}")
                        break
        except asyncio.TimeoutError:
            log.info("⏳ Execution timeout (Node did not reply within 30s).")
        finally:
            await pubsub.close()


async def main():
    tunnel = await TunnelFactory.get_default()
    shell = EcosystemShell(tunnel)
    
    try:
        await shell.run()
    finally:
        await shell.manifold.close()
        await tunnel.close()
        log.info("System resources released.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n👋 Exiting Console...")