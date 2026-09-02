# fiber.phase.kernel.plane.topos
import os
import yaml
import asyncio
import shutil
import httpx
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, Optional, Type, Tuple

from fiber.phase.kernel.tracer.infra import ContainerStateAuditor, EntropyAuditor, UniversalLogAuditor
from fiber.phase.kernel.tracer.router import InfraRouter

from xphi.watcher.plane.emitter import get_emitter, flow_scope
from xphi.watcher.tracer.bound import SystemBound

log = get_emitter("plane.topos")

# -----------------------------------------------------------------------------
# 1. Blueprint & Infrastructure Adapter
# -----------------------------------------------------------------------------

class ToposBlueprint:
    """@desc: 인프라에 구애받지 않는(Agnostic) 선언적 클러스터 위상 정의"""
    @staticmethod
    def get_cluster_spec(project_root: str) -> Dict[str, Any]:
        return {
            "version": "3.8",
            "services": {
                "redis-tunnel": {
                    "image": "redis:7-alpine",
                    "container_name": "fiber_tunnel",
                    "ports": ["6379:6379"],
                    "healthcheck": {"test": ["CMD", "redis-cli", "ping"], "interval": "3s", "retries": 5}
                },
                "gateway-edge": {
                    "build": {"context": project_root, "dockerfile": "Dockerfile"},
                    "container_name": "fiber_gateway",
                    "environment": ["REDIS_URL=redis://redis-tunnel:6379/0", "NODE_PROFILE=EDGE"],
                    "command": ["daemon", "-s", "rest_edge,gateway_edge"],
                    "ports": ["8000:8000"],
                    "depends_on": {"redis-tunnel": {"condition": "service_healthy"}}
                },
                "compute-worker": {
                    "build": {"context": project_root, "dockerfile": "Dockerfile"},
                    "container_name": "fiber_compute",
                    "environment": ["REDIS_URL=redis://redis-tunnel:6379/0", "NODE_PROFILE=COMPUTE", "DPHI_FIXED_WORKERS=2"],
                    "command": ["daemon", "-s", "rpc_worker,risk_vault"],
                    "depends_on": {"redis-tunnel": {"condition": "service_healthy"}}
                }
            }
        }

class DockerComposeAdapter:
    """@desc: Blueprint를 Docker Compose 인프라로 프로비저닝 및 해제하는 순수 어댑터"""
    def __init__(self, workspace: Path, boundary: SystemBound):
        self.workspace = workspace
        self.compose_file = self.workspace / "docker-compose.yml"
        self.boundary = boundary

    async def apply(self, spec: Dict[str, Any]) -> bool:
        self.workspace.mkdir(parents=True, exist_ok=True)
        with open(self.compose_file, "w") as f:
            yaml.dump(spec, f, sort_keys=False)
            
        log.info(f"[Adapter] Manifest materialized at {self.compose_file}. Igniting cluster...")
        
        cmd = ["docker-compose", "-f", str(self.compose_file), "up", "--build", "-d"]
        code, out, err = await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=True)
        if code != 0:
            log.error(f"[Adapter] Apply failed: {err}")
            return False
        return True

    async def teardown(self) -> None:
        if self.compose_file.exists():
            log.info("[Adapter] Gracefully collapsing physical manifolds...")
            cmd = ["docker-compose", "-f", str(self.compose_file), "down", "-v", "--remove-orphans"]
            await self.boundary.run_command(cmd, cwd=str(self.workspace), capture=False)
            shutil.rmtree(self.workspace, ignore_errors=True)


# -----------------------------------------------------------------------------
# 2. Topology Orchestrator & Context
# -----------------------------------------------------------------------------

@dataclass
class ToposContext:
    """@desc: E2E Scene(테스트 스위트)에 주입될 인프라 환경 제어 및 관측 컨텍스트"""
    router: InfraRouter
    boundary: SystemBound
    auditors: Dict[str, Any]


class ToposOrchestrator:
    """
    @desc: 클러스터 프로비저닝부터 다차원(State, Entropy, Network) 검증 및 테스트 스위트 
    실행까지 전 주기를 관장하는 단발성(One-shot) 오케스트레이터 클래스.
    """
    def __init__(self, target_name: str = "dphi-topos-sandbox", mode: str = "dev", timeout: int = 120, suites: Dict[str, Type] = None):
        self.worker_name = target_name
        self.mode = mode
        self.timeout = timeout
        self.suites = suites or {}
        self.keep_workspace = False  
        
        self.workspace = Path(f"/tmp/fiber_topos_{self.mode}")
        self.project_root = str(Path.cwd().absolute())
        
        # 1. 시스템 경계(Boundary) 및 어댑터 초기화
        self.boundary = SystemBound()
        self.adapter = DockerComposeAdapter(self.workspace, self.boundary)
        
        # 2. 다차원 Auditor 및 Router 설정 (tracer 연동)
        self.router = InfraRouter(host_url="http://localhost:8000")
        
        self.gateway_state = ContainerStateAuditor("fiber_gateway", self.boundary)
        self.compute_entropy = EntropyAuditor("fiber_compute", self.boundary)
        self.gateway_logs = UniversalLogAuditor("fiber_gateway", "boot_check", self.boundary)

    async def _verify_resonance(self) -> bool:
        """
        @desc: 부팅 과정에서의 입체적 공명 검증 (상태 -> 자원 -> 네트워크 레이어)
        참고: 센서(Auditor)의 부착(attach)과 분리(detach)는 상위 execute()에서 관리하여
        테스트 도중에도 센서가 계속 관측할 수 있도록 위임합니다.
        """
        log.info(f"\n>>> [PHASE] Stabilizing Topology & Verifying Resonance <<<")
        
        wait_count = 0
        is_network_ready = False
        
        while wait_count < self.timeout:
            # 1단계: 컨테이너 State 검증 (크래시 루프 감지)
            if not self.gateway_state.is_running:
                log.error(f"  [CRASH] Gateway container collapsed unexpectedly. (Exit: {self.gateway_state.exit_code})")
                return False

            # 2단계: 네트워크 Ingress 검증 (Router 동적 엔드포인트 활용)
            if not is_network_ready:
                try:
                    health_url = self.router.get_http_endpoint("health_check")
                    headers = self.router.build_headers()
                    
                    async with httpx.AsyncClient() as client:
                        res = await client.get(health_url, headers=headers, timeout=2.0)
                        if res.status_code == 200:
                            log.info("  [NETWORK] Gateway Ingress Accessibility: PASSED ✅")
                            is_network_ready = True
                except Exception:
                    pass # 아직 부팅 중

            # 3단계: 리소스(Entropy) 안정화 검증
            if is_network_ready and self.compute_entropy.last_cpu_usage < 80.0:
                log.info(f"  [ENTROPY] Compute Node CPU stabilized at {self.compute_entropy.last_cpu_usage}% ✅")
                log.crit(f"[{self.worker_name}] ✅ Topology Resonance Confirmed! Cluster is fully operational.")
                return True
            
            await asyncio.sleep(2)
            wait_count += 2
            
        log.error("  [TIMEOUT] Cluster failed to reach stable resonance in time.")
        return False

    async def _run_all_suites(self, broker: Any, context: ToposContext) -> int:
        """
        @desc: 주입된 비즈니스 테스트 시나리오 실행
        ToposContext를 넘겨주어 E2E Scene이 라우터와 센서 정보에 접근할 수 있게 합니다.
        """
        total_fails = 0
        for suite_name, suite_cls in self.suites.items():
            log.info(f"\n>>> [PHASE] Starting Integration Suite: {suite_name.upper()} <<<")
            try:
                # E2E Scene 초기화 시 ToposContext 의존성 주입
                suite_instance = suite_cls(broker=broker, context=context)
                await suite_instance.run_all()
                total_fails += getattr(suite_instance, 'fail_count', 0)
            except Exception as e:
                log.error(f"[ERROR] Suite '{suite_name}' crashed: {e}", exc_info=True)
                total_fails += 1
        return total_fails

    async def execute(self, broker: Any = None) -> Tuple[bool, str]:
        """오케스트레이터 메인 진입점"""
        log.info(f"\n--- [START] Orchestrating Topology ({self.mode.upper()}) ---")
        
        try:
            # 1. 인프라 프로비저닝
            spec = ToposBlueprint.get_cluster_spec(self.project_root)
            success = await self.adapter.apply(spec)
            if not success:
                return False, "Failed to apply physical topology via adapter."
            
            # 2. 다차원 센서 부착 (전 주기 동안 유지)
            self.gateway_state.attach()
            self.compute_entropy.attach()
            if self.gateway_logs: 
                self.gateway_logs.attach()
            
            # 3. 안정화 및 네트워크 검증
            with flow_scope(phase="RESONANCE_CHECK"):
                is_stable = await self._verify_resonance()
                if not is_stable:
                    return False, "Topology failed multidimensional verification (State/Entropy/Network)."
            
            # 4. 추가 비즈니스 로직(E2E Test Suite) 실행
            if self.suites:
                with flow_scope(phase="TEST_EXECUTION"):
                    # E2E 테스트에서 참조할 Context 조립
                    test_context = ToposContext(
                        router=self.router,
                        boundary=self.boundary,
                        auditors={
                            "state": self.gateway_state,
                            "entropy": self.compute_entropy,
                            "log": self.gateway_logs
                        }
                    )
                    total_fails = await self._run_all_suites(broker, test_context)
                    if total_fails > 0:
                        return False, f"Topology verified, but {total_fails} logical E2E tests failed."
            
            return True, ""
            
        except Exception as e:
            log.error(f"[FATAL] Orchestration crashed: {e}")
            return False, str(e)
            
        finally:
            log.info("\n[SYSTEM] Triggering Teardown Sequence...")
            
            # 5. 센서 분리 및 안전한 환경 회수
            self.gateway_state.detach()
            self.compute_entropy.detach()
            if self.gateway_logs: 
                self.gateway_logs.detach()
                
            await self.adapter.teardown()
            if not self.keep_workspace and self.workspace.exists():
                shutil.rmtree(self.workspace, ignore_errors=True)
            log.info("[SYSTEM] All manifolds collapsed and cleaned up.")