# xphi.xor.tester.mock.exception
## @lineage: anchor.tester.mock.exception
## @lineage: anchor.surface.testing.mock.exception
## @lineage: anchor.testing.mock.exception
"""
@phase: Mock Generation Boundary (Exceptions)
@desc: Safely generates standardized API exceptions for testing resilience and fallback topologies.
"""
from anchor.surface.provider.mapping.exception import exception_type

class MockAPIConnectionError(Exception):
    """Fallback 및 Retry 로직을 트리거하기 위한 순수 Mock Exception"""
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code
        self.message = message

def mock_api_error(status_code: int = 500, message: str = "Internal Server Error") -> Exception:
    """@desc: 시뮬레이터(SimulationStep)에서 주입할 가짜 API 에러를 생성"""
    return MockAPIConnectionError(message=message, status_code=status_code)