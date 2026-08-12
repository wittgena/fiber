# dphi.node.attach.debug
## @lineage: dphi.phase.attach.debug
## @lineage: phase.attach.debug
import asyncio
import json
import sys
import uuid
from dataclasses import asdict

from arch.topos.tunnel.factory import TunnelFactory
from arch.contract.event.psi import PsiEvent, PsiCarrier, CarrierType
from arch.contract.event.next import next_id
from kernel.daemon.bootstrap import KEY_HEARTBEAT_PATTERN, TOPIC_BUS_STREAM

class DebugShell:
    def __init__(self, tunnel):
        self.tunnel = tunnel
        self.running = True

    async def _print_header(self):
        print("\n" + "="*60)
        print(" 🌌 [\033[95mDPHI Phase Chain Debug Shell\033[0m] - \033[92mConnected\033[0m")
        print("="*60)
        print(" Available Commands:")
        print("  \033[96mnodes\033[0m        : 현재 살아있는 노드(Master/Worker) Heartbeat 조회")
        print("  \033[96mping\033[0m         : 전체 시스템에 system:ping 브로드캐스트")
        print("  \033[96mreload <fqn>\033[0m : 특정 모듈 분산 핫-리로드 (예: reload eco.mesh)")
        print("  \033[96mexec <cmd>\033[0m   : Master 노드에 CLI 명령 주입 (예: exec align.imports)")
        print("  \033[96mexit / quit\033[0m  : 디버그 셸 종료")
        print("-" * 60)

    async def run(self):
        await self._print_header()
        
        while self.running:
            # 비동기 루프를 블로킹하지 않기 위해 스레드에서 입력 대기
            cmd_line = await asyncio.to_thread(input, "\033[93mdebug>\033[0m ")
            if not cmd_line.strip():
                continue
                
            await self.process_command(cmd_line.strip())

    async def process_command(self, cmd_line: str):
        parts = cmd_line.split()
        cmd = parts[0].lower()

        try:
            if cmd in ["exit", "quit"]:
                self.running = False
                print("👋 Exiting debug shell...")
                
            elif cmd == "nodes":
                keys = await self.tunnel.keys(KEY_HEARTBEAT_PATTERN)
                print(f"📊 Active Nodes Detected ({len(keys)}):")
                for k in keys:
                    # 'runtime:heartbeat:node-1234-w0' 형태에서 추출
                    node_id = k.split(":")[-1]
                    timestamp = await self.tunnel.get(k)
                    print(f"  - \033[92m{node_id}\033[0m (Last pulse: {timestamp})")
                    
            elif cmd == "reload":
                if len(parts) < 2:
                    print("⚠️ Usage: reload <module_fqn>")
                    return
                target_module = parts[1]
                await self._inject_reload(target_module)
                
            elif cmd == "exec":
                if len(parts) < 2:
                    print("⚠️ Usage: exec <cli_command> [args...]")
                    return
                cli_cmd = parts[1]
                cli_args = parts[2:]
                await self._inject_cli_command(cli_cmd, cli_args)
                
            elif cmd == "ping":
                await self._inject_ping()
                
            else:
                print(f"⚠️ Unknown command: {cmd}")
                
        except Exception as e:
            print(f"❌ Error executing command: {e}")

    async def _inject_reload(self, module_fqn: str):
        """Worker들의 Dispatcher가 수신할 핫리로드 이벤트 발행"""
        sync_event = PsiEvent(
            event_id=next_id(), parent_id=None, source_id="debug.shell", scope="GLOBAL", tick=0, phase_id=0, context={},
            carrier=PsiCarrier(
                kind="system:topology", tag="reload", 
                payload={"module_fqn": module_fqn}, carrier_type=CarrierType.FIXED
            )
        )
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(sync_event.__dict__)})
        print(f"📡 Broadcasted topology reload for '\033[96m{module_fqn}\033[0m'")

    async def _inject_ping(self):
        """시스템 전체 PubSub에 ping 발행 후 응답 대기"""
        pubsub = self.tunnel.pubsub()
        await pubsub.subscribe("system:echo")
        
        print("🦇 Emitting system:ping...")
        await self.tunnel.publish("system:ping", json.dumps({"source": "debug.shell"}))
        
        try:
            async with asyncio.timeout(2.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        data = json.loads(msg["data"])
                        print(f"  [ECHO] Received from: {data}")
        except asyncio.TimeoutError:
            print("⏳ Echo collection timeout (2.0s).")
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
        
        # Stream에 주입하여 Master가 잡아가게 함
        event_dict = asdict(trigger_event) if hasattr(trigger_event, '__dataclass_fields__') else trigger_event.__dict__
        await self.tunnel.state_store.xadd(TOPIC_BUS_STREAM, {"data": json.dumps(event_dict)})
        print(f"🚀 Injected COMMAND '{command}' -> Stream. Listening on '{response_channel}'...")
        
        try:
            async with asyncio.timeout(30.0):
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        result = json.loads(msg["data"])
                        status = result.get("status", "UNKNOWN")
                        color = "\033[92m" if status == "SUCCESS" else "\033[91m"
                        print(f"\n{color}[{status}]\033[0m {result.get('summary', '')}")
                        break
        except asyncio.TimeoutError:
            print("⏳ Execution timeout (Node did not reply within 30s).")
        finally:
            await pubsub.close()


async def main():
    tunnel = await TunnelFactory.get_default()
    shell = DebugShell(tunnel)
    
    try:
        await shell.run()
    finally:
        await tunnel.close()
        print("System resources released.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Exiting...")