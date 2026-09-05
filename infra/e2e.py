# fiber.infra.e2e
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
    """
    @desc: E2E 테스트 파이프라인에서 Uvicorn의 시그널 인터셉트를 방지하고 
           프로세스 내장형으로 구동하기 위한 관리형 서버입니다.
    """
    def install_signal_handlers(self):
        # 메인 프로세스의 시그널 헨들러를 덮어쓰지 않도록 무효화합니다.
        pass


class PipelineRunner:
    """
    @desc: 여러 Phase로 구성된 E2E 파이프라인을 실행하는 베이스 러너입니다.
    """
    def __init__(self, name: str, scope_name: str):
        self.name = name
        self.scope_name = scope_name
        self.phases: List[Phase] = []
        
    def set_phases(self, phases: List[Phase]):
        self.phases = phases
        
    async def run_pipeline(self) -> List[TestResult]:
        """하위 클래스에서 오버라이드하여 파이프라인 실행 로직을 구현합니다."""
        raise NotImplementedError("run_pipeline must be implemented by subclasses.")