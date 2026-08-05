# exchange.chaos.injector
## @lineage: dphi.exchange.chaos.injector
from typing import Dict, Any, Callable
from exchange.mock.net import MockNetBuilder

class HttpChaosLibrary:
    """HTTP 방어벽(Membrane) 테스트를 위한 순수 바이트 페이로드 라이브러리"""
    OOM = lambda: b"A" * 1024 * 1024 * 10
    SMUGGLING = lambda: b"GET / HTTP/1.1\r\n\r\nGET /admin HTTP/1.1\r\n"
    MCP_PATH_TRAVERSAL = lambda: b"../../../../etc/passwd"

    @classmethod
    def get_all_vectors(cls) -> list[tuple[str, Callable]]:
        return [
            ("OOM_Exhaustion", cls.OOM),
            ("Protocol_Smuggling", cls.SMUGGLING),
            ("Path_Traversal", cls.MCP_PATH_TRAVERSAL)
        ]

class RpcChaosInjector:
    """P2P/RPC 계층의 무결성(Integrity)과 합의(Consensus) 파괴를 위한 데이터 조작기"""
    
    @staticmethod
    def corrupt_ap2_mandate(agent_pub_hex: str, agent_key: Any) -> Dict[str, Any]:
        """권한 위임장(AP2 Mandate)을 고의로 과거(만료) 시점으로 조작하여 반환"""
        return MockNetBuilder.ap2_mandate_params(
            agent_pub_hex=agent_pub_hex,
            agent_key=agent_key,
            is_expired=True
        )

    @staticmethod
    def corrupt_consensus_signatures(signatures: list[str]) -> list[str]:
        """M-of-N 다중 서명 배열 중 하나를 파괴하여 Byzantine Fault 유발"""
        if not signatures:
            return signatures
        corrupted = list(signatures)
        corrupted[0] = "0xBAD_SIGNATURE_CORRUPTED_BY_CHAOS_INJECTOR"
        return corrupted