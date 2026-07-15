# bound.watcher.audit.tracer
## @lineage: xphi.watcher.audit.tracer
import traceback

IGNORE_FILES = (
    "socket.py", 
    "ssl.py", 
    "warden.py", 
    "tracer.py",
    "http/client.py",
    "urllib",
    "asyncio",
)

def get_network_caller_origin(limit: int = 20) -> str:
    """
    호출 스택을 역추적하여 실제 네트워크 요청을 발생시킨 비즈니스 로직의 위치를 찾습니다.
    내부 소켓/HTTP 처리 라이브러리들은 무시합니다.
    """
    stack_summary = traceback.extract_stack(limit=limit)
    
    ## 맨 마지막(이 함수 자체)을 제외하고 역순으로 탐색
    for frame in reversed(stack_summary[:-1]):
        filename = frame.filename
        
        # 시스템 기본 모듈이나 무시 목록에 포함된 파일이면 건너뜀
        if any(ignored in filename for ignored in IGNORE_FILES):
            continue
            
        # 실제 외부 통신을 유발한 최초의 스택 발견
        return f"{filename}:{frame.lineno} (in {frame.name})"
        
    return "Unknown caller"