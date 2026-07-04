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
    surface_type: str = "local" # 'local', 'sandbox', 'dphi' 등을 직접 지정
    dphi_model: str = "local-gemma-3"
    
    ## 네트워크 및 포트 설정
    host: str = "0.0.0.0"
    port: int = 8000
    timeout: int = 30
    
    ## 로깅 및 가시성
    show_logs: bool = True
    
    ## 원격/샌드박스 연결 설정
    use_proxy: bool = False
    server_url: str = "http://localhost:8000"
    workspace_ref: Optional[str] = None
    session_api_key: Optional[str] = None
    
    ## 인프라 엔진 팩토리 주입
    engine_factory: Optional[Callable[..., Any]] = None

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