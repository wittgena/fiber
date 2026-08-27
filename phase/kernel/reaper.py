# phase.kernel.reaper
import asyncio
import json
import urllib.parse
import os
import signal
import argparse
from typing import List, Set
from redis.asyncio import Redis
import psutil

from xphi.kernel.phase.reactor import PhaseReactor
from xphi.kernel.daemon.task.supervisor import TaskSupervisor
from xphi.watcher.plane.emitter import get_emitter

DEFAULT_TARGET_TAGS = [
    "phase.kernel",
    "dphi",
    "deno",
    "workerd",
    "multiprocessing.spawn",           # OS 멀티프로세싱 워커 노드
    "multiprocessing.resource_tracker" # 자원 추적 데몬
]

log = get_emitter("node.reaper", phase="BOOT")

class SystemOps:
    """@desc: 시스템 프로세스의 생명주기를 통제(Kill)하거나, 커널 최적화 상태를 감사(Audit)하는 코어 유틸리티"""
    def __init__(self, redis_conn: Redis, tag: str):
        self.redis = redis_conn
        self.tag = tag
        self.registry_key = f"system:{self.tag}:pids"
        self.log = get_emitter(f"ops.{self.tag}", phase="EXEC")
        
        self.supervisor = TaskSupervisor(source=f"Commander-{self.tag}")
        self.supervisor.add_error_handler(self._on_task_error)

    def _on_task_error(self, task: asyncio.Task, exc: BaseException) -> None:
        self.log.error(f" [Supervisor] Task {task.get_name()} failed: {exc}")

    async def get_pids_from_port(self, port: int) -> List[str]:
        """@action: 포트를 점유 중인 프로세스 ID 반환 (lsof 기반)"""
        try:
            proc = await asyncio.create_subprocess_exec(
                "lsof", "-t", f"-i:{port}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            if stdout:
                return [pid.strip() for pid in stdout.decode().strip().split('\n') if pid.strip()]
            return []
        except Exception as e:
            self.log.error(f"lsof failed for port {port}: {e}")
            return []

    async def _discover_active_pids(self, wait_time: float = 2.0) -> Set[str]:
        """@flow: PubSub Ping-Pong을 통해 현재 살아있는 활성 프로세스들의 PID를 수집"""
        self.log.info(f"🦇 Broadcasting system:ping to locate [{self.tag}] nodes...")
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("system:echo")
        await self.redis.publish("system:ping", json.dumps({"ts": asyncio.get_running_loop().time(), "source": f"ops_{self.tag}"}))

        active_pids = set()
        try:
            async with asyncio.timeout(wait_time):
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True)
                    if msg:
                        try:
                            data = json.loads(msg["data"])
                            if "api_base" in data:
                                port = urllib.parse.urlparse(data["api_base"]).port
                                pids = await self.get_pids_from_port(port)
                                active_pids.update(pids)
                        except Exception:
                            continue
                    await asyncio.sleep(0.05)
        except asyncio.TimeoutError:
            self.log.info(f"🦇 Echo collection complete. Found {len(active_pids)} active processes.")
        finally:
            await pubsub.unsubscribe("system:echo")
            
        return active_pids

    # =========================================================================
    # [1] REAPER MODULE: 파괴적 통제 (Strike, Force, Nuke)
    # =========================================================================
    async def _execute_kill(self, pid: str, force: bool = True):
        signal_flag = "-9" if force else "-15"
        proc = await asyncio.create_subprocess_exec(
            "kill", signal_flag, pid,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await proc.wait()
        if proc.returncode == 0:
            self.log.warning(f" 💀 [Killed] PID {pid} cleanly terminated with {signal_flag}")

    async def surgical_strike(self):
        """@action: 활성 노드만 정확히 식별하여 타격"""
        pids = await self._discover_active_pids()
        for pid in pids:
            self.supervisor.create(self._execute_kill(pid, force=True), name=f"Strike-{pid}")
        return len(pids)

    async def scorched_earth(self):
        """@action: Process Tree 추적을 통한 고아/좀비 프로세스 완벽 청소"""
        self.log.warning(f"🔥 Initiating Scorched Earth for [{self.tag}]...")
        
        my_pid = os.getpid()
        killed_count = 0
        try:
            for proc in psutil.process_iter(['pid', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline') or []
                    cmd_str = " ".join(cmdline)
                    
                    if self.tag in cmd_str and "phase.node.reaper" not in cmd_str and proc.pid != my_pid:
                        children = proc.children(recursive=True)
                        procs_to_kill = children + [proc]
                        
                        for p in procs_to_kill:
                            try:
                                p.kill()  
                                killed_count += 1
                                self.log.info(f"   [Phase 1] 💀 Terminated PID {p.pid} (Command: {' '.join(p.cmdline()[:2])}...)")
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
                    
            if killed_count > 0:
                self.log.info(f"   [Phase 1] Process tree sweep completed. {killed_count} processes explicitly reaped.")
            else:
                self.log.info(f"   [Phase 1] No active processes matched for [{self.tag}].")
        except Exception as e:
            self.log.error(f"   [Phase 1] psutil sweep failed: {e}")

        try:
            pids = await self.redis.smembers(self.registry_key)
            if pids:
                self.log.info(f"   [Phase 2] Dispatching kill orders for {len(pids)} PIDs in registry.")
                for pid in pids:
                    self.supervisor.create(self._execute_kill(str(pid), force=True), name=f"ForceKill-{pid}")
            
            await self.redis.delete(self.registry_key)
            self.log.info(f"   [Phase 3] Registry {self.registry_key} cleared.")
        except Exception as e:
            self.log.error(f"   [Phase 2/3] Registry cleanup failed: {e}")

    async def nuke_redis_state(self):
        """@action: 현재 사용 중인 Redis Database의 모든 잔여물 완벽 삭제"""
        self.log.warning(f"🧨 NUKING REDIS STATE: Executing FLUSHDB...")
        try:
            await self.redis.flushdb(asynchronous=True)
            self.log.info(f"   └─ Redis database perfectly wiped clean.")
        except Exception as e:
            self.log.error(f"   └─ Redis FLUSHDB failed: {e}")

    async def nuke_flare(self, target_port: int = 8787):
        """
        @action: Cloudflare Wrangler (Node.js) 좀비 프로세스 완벽 사냥
        @desc: 지정된 포트(주로 8787)를 강제로 해방하여 Edge Hologram 부팅 충돌을 방지합니다.
        """
        self.log.warning(f"☄️  Initiating Tactical Flare Nuke on Port {target_port}...")
        pids = await self.get_pids_from_port(target_port)
        
        if not pids:
            self.log.info(f"   └─ Port {target_port} is already clean. No zombies found.")
            return 0
            
        self.log.warning(f"   └─ Detected {len(pids)} zombie process(es) holding Port {target_port}. Engaging...")
        for pid in pids:
            self.supervisor.create(self._execute_kill(pid, force=True), name=f"FlareNuke-{pid}")
        
        return len(pids)

    # =========================================================================
    # [2] PROBE/AUDITOR MODULE: 비파괴적 아키텍처 감사 (Audit)
    # =========================================================================
    async def _verify_cpu_pinning(self, pid: str) -> bool:
        try:
            proc = await asyncio.create_subprocess_exec(
                "taskset", "-cp", pid,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            output = stdout.decode().strip()
            
            if "affinity list:" in output:
                affinity = output.split(":")[-1].strip()
                if "," not in affinity and "-" not in affinity:
                    self.log.warning(f"   [PASS] ⚡ PID {pid} tightly bound to Core {affinity}")
                    return True
                else:
                    self.log.error(f"   [FAIL] ⚠️ Floating CPU detected for PID {pid}. Affinity: {affinity}")
            return False
        except Exception:
            return False

    async def _verify_syscall_multiplexing(self, pid: str, duration: int = 3) -> bool:
        try:
            proc = await asyncio.create_subprocess_exec(
                "strace", "-c", "-p", pid,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await asyncio.sleep(duration)
            proc.send_signal(signal.SIGINT)
            _, stderr = await proc.communicate()
            
            output = stderr.decode()
            if "epoll_wait" in output or "epoll_pwait" in output:
                self.log.warning(f"   [PASS] 🚀 High-Performance epoll I/O Verified on PID {pid}")
                return True
            else:
                self.log.error(f"   [FAIL] 🐌 Legacy I/O or no active epoll captured on PID {pid}")
                return False
        except Exception:
            return False

    async def _audit_node(self, pid: str):
        self.log.info(f" 🔍 [Audit] Profiling Architecture Compliance for PID: {pid}")
        pin_valid = await self._verify_cpu_pinning(pid)
        io_valid = await self._verify_syscall_multiplexing(pid, duration=3)
        
        if pin_valid and io_valid:
            self.log.info(f" 🎯 [SUCCESS] Node {pid} perfectly aligns with KernelReactor Specs.")
        else:
            self.log.error(f" ❌ [VIOLATION] Node {pid} failed compliance checks.")

    async def audit_system(self):
        pids = await self._discover_active_pids()
        for pid in pids:
            self.supervisor.create(self._audit_node(pid), name=f"Audit-{pid}")
        return len(pids)

    # =========================================================================
    # 오케스트레이션 실행 및 종료
    # =========================================================================
    async def execute_task(self, task_type: str) -> int:
        count = 0
        try:
            if task_type == "strike":
                count = await self.surgical_strike()
            elif task_type == "force":
                await self.scorched_earth()
            elif task_type == "clean":
                count = await self.surgical_strike()
                await self.scorched_earth()
            elif task_type == "nuke":
                await self.scorched_earth()
                await self.nuke_redis_state()
            elif task_type == "audit":
                count = await self.audit_system()
            elif task_type == "flare":
                count = await self.nuke_flare(target_port=8787)
            return count
        finally:
            self.log.info(f"Awaiting resolution of all {task_type} tasks for [{self.tag}]...")
            await self.supervisor.shutdown()


class CommandCLI:
    def __init__(self):
        self.redis_conn = None

    async def run(self):
        parser = argparse.ArgumentParser(description="System Operations Commander (Reaper & Auditor)")
        parser.add_argument("--tag", type=str, default=None, help="Target component tag (e.g. kernel.phase.runtime.node)")
        parser.add_argument("--task", type=str, default="clean", choices=["strike", "force", "clean", "nuke", "audit", "flare"], help="Task to run")
        args = parser.parse_args()

        self.redis_conn = Redis(host=os.getenv("REDIS_HOST", "localhost"), decode_responses=True)
        
        # [ALIGNMENT] --task flare일 경우 다른 태그들을 다 뒤지지 않고 오직 포트 청소만 수행
        if args.task == "flare":
            target_tags = ["flare.edge"]
        else:
            target_tags = [args.tag] if args.tag else DEFAULT_TARGET_TAGS
        
        log.info(f"Executing Commander [{args.task.upper()}] Sequence. Targets: {target_tags}")
        
        total_nodes_processed = 0
        for tag in target_tags:
            log.info(f"\n## Orchestrating Target: [{tag}]")
            commander = SystemOps(self.redis_conn, tag=tag)
            
            count = await commander.execute_task(task_type=args.task)
            total_nodes_processed += count
                
        log.info(f"Commander execution finished. Total targets processed: {total_nodes_processed}")

    async def teardown(self):
        if self.redis_conn:
            log.info("[Teardown] Releasing Redis connection pool cleanly...")
            await self.redis_conn.aclose()


if __name__ == "__main__":
    cli = CommandCLI()
    PhaseReactor.ignite(
        main_coro_func=cli.run,
        teardown_hook=cli.teardown
    )