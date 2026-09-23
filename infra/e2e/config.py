# fiber.infra.e2e.config
import uvicorn
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, List

@dataclass
class Phase:
    """@desc: 파이프라인 내 개별 테스트 페이즈를 정의합니다."""
    name: str
    action: Callable[[], Coroutine[Any, Any, None]]


@dataclass
class TestResult:
    """@desc: E2E 테스트 시나리오의 실행 결과를 담습니다."""
    target: str
    scenario: str
    success: bool
    expected_success: bool

    @property
    def passed(self) -> bool:
        return self.success == self.expected_success


@dataclass
class E2EConfig:
    """@desc: E2E 파이프라인 구동 시 필요한 네트워크 및 환경 설정을 관리합니다."""
    host: str
    port: int
    protocol: str
    
    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"


class ManagedTestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass


class PipelineRunner:
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self) -> List[TestResult]:
        """하위 클래스에서 오버라이드하여 파이프라인 실행 로직을 구현합니다."""
        raise NotImplementedError("run_pipeline must be implemented by subclasses.")