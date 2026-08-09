# phase.epoch.config.eco
## @lineage: phase.epoch.flow.config.eco
## @lineage: epoch.flow.config.eco
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from watcher.plane.emitter import get_emitter
from kernel.dphi.adapter.state import StateAdapter
from kernel.dphi.adapter.exchange import ExchangeAdapter

log = get_emitter("config.eco")

class ActorIdentity:
    """단일 노드/에이전트의 암호학적 신원과 JCS 기반 결정론적 서명을 담당하는 공통 클래스"""
    def __init__(self, name: str = "Anonymous"):
        self.name = name
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.pubkey_hex = self._generate_pub_hex()

    def _generate_pub_hex(self) -> str:
        return self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, 
            format=serialization.PublicFormat.Raw
        ).hex()

    def sign(self, commit_dict: dict) -> str:
        """StateAdapter의 JCS 규격을 사용하여 결정론적 서명을 생성합니다."""
        canonical_bytes = StateAdapter.to_canonical_bytes(commit_dict)
        commit_hash = hashlib.sha256(canonical_bytes).hexdigest()
        return self.private_key.sign(commit_hash.encode('utf-8')).hex()


class EcoContext:
    """에코시스템 시나리오 실행에 필요한 주체들과 어댑터를 관리하는 컨텍스트"""
    def __init__(self):
        # 1. Main Eco Identity (Oracle, Protocol DAO, Eco A2A)
        self.system = ActorIdentity("System_Core")
        
        # 2. Exchange & P2P Identities (Decentralized Exchange)
        self.agent_a = ActorIdentity("Agent_A")
        self.agent_b = ActorIdentity("Agent_B")
        self.field = ActorIdentity("Clearing_Field")
        
        # 3. Adapters
        self.exchange_adapter = ExchangeAdapter(clearing_house_pub_key=self.field.pubkey_hex)