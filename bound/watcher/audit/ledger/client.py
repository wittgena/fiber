# bound.watcher.audit.ledger.client
import hashlib
import json
import logging
from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Optional

from watcher.kernel.state.spec import NodeType
from watcher.kernel.store import KernelStore

log = logging.getLogger("ledger.client")

class LedgerClient:
    def __init__(self, store: Optional[KernelStore] = None):
        self.store = store or KernelStore()
        self.core_stream_id = "stream_core_infrastructure"

    def get_latest_merkle_root(self) -> str:
        head_hash = self.store.get_head_hash(self.core_stream_id)
        if not head_hash:
            log.warning("[LedgerClient] Void state detected in ledger. Breathing from GENESIS.")
            return "GENESIS_HASH"
        return head_hash

    def _read_from_ledger(self, key: str) -> Optional[Dict[str, Any]]:
        """원장(RocksDB)의 물리적 계층에 안전하게 접근하여 구조화된 데이터를 읽어옵니다."""
        if not hasattr(self.store, 'db') or self.store.db is None:
            return None
            
        db_key = key.encode('utf-8')
        if db_key in self.store.db:
            try:
                return json.loads(self.store.db[db_key].decode('utf-8'))
            except json.JSONDecodeError:
                return None
        return None

    def verify_node_kind(self, node_name: str) -> NodeType:
        if node_name in ["stable_core", "root_anchor", "kernel_vault"]:
            return NodeType.ANCHOR
            
        ledger_data = self._read_from_ledger(f"node:{node_name}")
        if ledger_data:
            try:
                kind_str = ledger_data.get("kind", "CORE")
                return NodeType[kind_str]
            except Exception as e:
                log.warning(f"[LedgerClient] Topology distortion in '{node_name}': {e}. Classifying as RUPTURE.")
                return NodeType.RUPTURE 

        log.debug(f"[LedgerClient] Unmapped chaos '{node_name}' entering through the boundary. Classifying as PULSE.")
        return NodeType.PULSE