# bound.client.local.base
## @lineage: dphi.adapter.local.base
## @lineage: ext.client.local.base
## @lineage: engine.client.local.base
from abc import ABC, abstractmethod
from typing import Dict, Any
from arch.contract.model.resonance import BridgeEvent

class BaseEngine(ABC):
    @abstractmethod
    def ask(self, prompt: str, callback: callable) -> str:
        pass

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """엔진/원격 서버의 상태를 확인"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """엔진이 사용하는 자원(네트워크, 파일 등)을 정리"""
        pass