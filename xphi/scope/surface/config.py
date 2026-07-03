# xphi.scope.surface.config
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Callable

from watcher.plane.emitter import get_emitter

log = get_emitter("surface.config")

def get_free_port(starting_port: int, max_port: int = 8999) -> int:
    """운영체제 바인딩 검증 방식을 사용하여 충돌 가능성이 전혀 없는 빈 포트를 정확히 탐색"""
    for port in range(starting_port, max_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free ports available between {starting_port} and {max_port}.")

@dataclass
class SurfaceConfig:
    """
    물리적/가상 인프라 표면(Surface) 설정을 담당하는 순수 데이터 클래스입니다.
    
    [주의] 
    논리적 파이프라인 설정(lm, callbacks, trace 등)은 이 클래스에 포함되지 않으며, 
    RunContext를 통해 별도로 관리 및 주입되어야 합니다.
    """
    # --- 1. 인프라 실행 모드 설정 ---
    use_proxy: bool = False
    use_dphi: bool = False
    use_thch: bool = False
    dphi_model: str = "local-gemma-3"
    
    # --- 2. 네트워크 및 포트 설정 ---
    host: str = "0.0.0.0"
    port: int = 8000
    timeout: int = 30
    
    # --- 3. 로깅 및 가시성 ---
    show_logs: bool = True
    
    # --- 4. 원격/샌드박스 연결 설정 (Sandbox/Remote Surface 용) ---
    server_url: str = "http://localhost:8000"
    workspace_ref: Optional[str] = None
    session_api_key: Optional[str] = None
    
    # --- 5. 인프라 엔진 팩토리 주입 ---
    engine_factory: Optional[Callable[..., Any]] = None

    def __post_init__(self):
        """인프라 설정 간의 무결성 검증"""
        if self.use_proxy and self.engine_factory is None:
            log.warning("[SurfaceConfig] `use_proxy`가 활성화되었으나 `engine_factory`가 제공되지 않았습니다.")


class BaseSurface(ABC):
    """실행 표면(Surface)의 수명 주기를 관리하는 추상 기반 클래스"""
    
    @abstractmethod
    def up(self) -> None: 
        """인프라 자원을 프로비저닝하고 연결을 수립합니다."""
        pass

    @abstractmethod
    def down(self) -> None: 
        """인프라 자원을 안전하게 해제하고 연결을 종료합니다."""
        pass

    @abstractmethod
    def get_engine(self) -> Any: 
        """해당 Surface 위에서 동작할 Engine 또는 Engine 팩토리를 반환합니다."""
        pass