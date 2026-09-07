# fiber.agent.conver.conv.store.residue
## @lineage: surgent.conv.store.residue
## @lineage: xphi.xor.stream.store.residue
## @lineage: xphi.arch.xor.stream.store.residue
## @lineage: arch.xor.stream.store.residue
## @lineage: arch.xor.store.residue
## @lineage: arch.xor.bridge.store.residue
"""@flow: ψ → ResidueExecutor(Buffer → Minimal Tension Eval) → ResidueStore(rocks.db)"""
import asyncio
import time
import json
import hashlib
import sys
from typing import List, Set, Any, Dict, Optional
from dataclasses import dataclass, field, asdict
from rocksdict import Rdict, Options
from xphi.arch.event.psi import PsiType
from xphi.watcher.plane.emitter import get_logger
from xphi.kernel.space.bind.resolver import find_current_self, resolve_path
from xphi.state.runtime.node import NodeRuntime
from xphi.state.runtime.executor.base import BaseExecutor

log = get_logger("xor.store")

try:
    SELF_ROOT = find_current_self()
    XOR_ROOT = resolve_path("xor")
except Exception as e:
    log.error(f"[Critical] 시스템 경로 로드 실패: {e}")
    sys.exit(1)

ROCKS_PATH = XOR_ROOT / "xor.rocks.db"

@dataclass
class ResidueBlock:
    """단일 실행(psi)에서 추출된 잔여 블록 데이터 모델"""
    section: str = ""
    symbols: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ResidueSnapshot:
    """통합된 RocksDB 결정화 데이터 모델"""
    symbols: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    
    # xphi.xor.store 전용 (Stream Pressure)
    pressure: float = 0.0
    tension: float = 0.0
    topology_nodes: List[str] = field(default_factory=list)
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    
    # work.cache 전용 (Cache & Plane Projection) 및 기타 확장 필드
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

class ResidueStore:
    """위상/잔여 데이터의 물리적 저장소 (RocksDB)"""
    _instance = None

    def __new__(cls, path=ROCKS_PATH):
        if cls._instance is None:
            cls._instance = super(ResidueStore, cls).__new__(cls)
            opt = Options()
            opt.create_if_missing(True)
            cls._instance.db = Rdict(str(path), opt)
        return cls._instance

    def __init__(self, path=ROCKS_PATH):
        pass

    def deposit(self, snapshot: ResidueSnapshot, key_prefix: Optional[str] = None):
        """Data Model을 직렬화하여 저장. 
        key_prefix가 주어지면 계층형 검색이 가능하도록 Prefix 구조 사용"""
        json_data = snapshot.to_json()
        
        if key_prefix:
            # ex: "flow/dev:AGENT:168000000.123:abc123hash"
            content_hash = hashlib.md5(json_data.encode()).hexdigest()[:12]
            key = f"{key_prefix}:{snapshot.timestamp}:{content_hash}".encode('utf-8')
        else:
            # Default fallback (단순 무결성 저장)
            key = hashlib.sha1(json_data.encode()).hexdigest().encode('utf-8')
            
        self.db[key] = json_data
        return key.decode('utf-8')

    def retrieve_latest(self, prefix: str) -> Optional[ResidueSnapshot]:
        """특정 Prefix(예: Repo+Plane)의 가장 최근 위상 스냅샷 탐색 (O(log N))"""
        prefix_bytes = prefix.encode('utf-8')
        latest_snap = None
        latest_time = 0.0

        for key, value in self.db.items():
            if not key.startswith(prefix_bytes):
                continue
            
            data = json.loads(value.decode('utf-8'))
            if data['timestamp'] > latest_time:
                latest_time = data['timestamp']
                latest_snap = ResidueSnapshot(**data)
                
        return latest_snap

    def close(self):
        self.db.close()

class ResidueExecutor(BaseExecutor):
    """@flow: Buffer → Tension Eval → Deposit"""
    def __init__(self, store: ResidueStore, batch_size: int = 5, threshold: float = 0.65):
        super().__init__()
        self.store = store
        self.batch_size = batch_size
        self.threshold = threshold
        
        self.buffer: List[ResidueBlock] = []
        self.current_pressure: float = 0.0

    async def execute(self, psi: PsiType) -> List[PsiType]:
        ## Psi Payload 파싱 및 Data Model 매핑
        try:
            raw_dict = json.loads(psi.symbol)
            block = ResidueBlock(
                section=raw_dict.get("section", "unknown"),
                symbols=raw_dict.get("symbols", []),
                raw_data=raw_dict
            )
            self.buffer.append(block)
        except json.JSONDecodeError:
            log.warning("Psi symbol is not valid JSON. Skipped residue extraction.")
            return [psi]

        ## 배치 사이즈 도달 대기
        if len(self.buffer) < self.batch_size:
            return [psi]

        ## 위상 평가 (Tension & Topology Evaluation)
        batch = self.buffer
        self.buffer = []  # 버퍼 초기화

        topology_nodes: Set[str] = {f"section::{hash(b.section) % 5}" for b in batch}
        extracted_symbols: Set[str] = {sym for b in batch for sym in b.symbols}

        if not extracted_symbols:
            return [psi]

        ## 긴장도 산출 및 누적 압력 계산
        tension = min(len(extracted_symbols) / 10.0, 1.0)
        self.current_pressure = (self.current_pressure * 0.7) + tension

        ## 임계치 도달 시 결정화 (Crystallization)
        if self.current_pressure >= self.threshold:
            snapshot = ResidueSnapshot(
                pressure=self.current_pressure,
                tension=tension,
                topology_nodes=list(topology_nodes),
                symbols=list(extracted_symbols),
                blocks=[b.raw_data for b in batch]
            )
            
            self.store.deposit(snapshot)
            log.signal(f"[residue] DEPOSIT P={self.current_pressure:.3f} (Blocks: {len(batch)})")
            
            ## 압력 해소 (응결 완료)
            self.current_pressure = 0.0  
        
        return [psi]