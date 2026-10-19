# fiber.infra.bound.account.registry
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bound.account.registry")

DEFAULT_TENANT_ID = "accounting_dept"
ACCOUNT_REGISTRY_PATH = resolve_path("sign") / "local_accounts.json"

@dataclass
class AccountRecord:
    """Local Account State Data Model"""
    tenant_id: str
    is_default: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    ledger_balance: int = 0
    active_quota: float = 0.0
    policy_ver: int = 0
    last_op: str = "INIT"
    last_jwt: Optional[str] = None


class LocalAccountRegistry:
    """Persists created/modified local accounts using strict dataclasses."""

    @classmethod
    def _load_all(cls) -> Dict[str, AccountRecord]:
        """Loads accounts from disk with strict schema filtering for backward compatibility."""
        if ACCOUNT_REGISTRY_PATH.exists():
            try:
                raw_data = json.loads(ACCOUNT_REGISTRY_PATH.read_text(encoding="utf-8"))
                valid_keys = {f.name for f in fields(AccountRecord)}
                result = {}
                
                for k, v in raw_data.items():
                    filtered_v = {key: val for key, val in v.items() if key in valid_keys}
                    result[k] = AccountRecord(**filtered_v)
                    
                return result
            except json.JSONDecodeError:
                log.error("[AccountRegistry] Registry file is corrupted. Initializing empty state.")
            except Exception as e:
                log.warning(f"[AccountRegistry] Failed to read registry file: {e}")
        return {}

    @classmethod
    def _save_all(cls, data: Dict[str, AccountRecord]) -> None:
        """Saves accounts to disk using atomic writes to prevent corruption."""
        try:
            ACCOUNT_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
            raw_data = {k: asdict(v) for k, v in data.items()}
            json_content = json.dumps(raw_data, indent=2, ensure_ascii=False)
            
            temp_path = ACCOUNT_REGISTRY_PATH.with_suffix('.json.tmp')
            temp_path.write_text(json_content, encoding="utf-8")
            temp_path.replace(ACCOUNT_REGISTRY_PATH)
            
        except Exception as e:
            log.error(f"[AccountRegistry] Failed to write registry file: {e}")

    @classmethod
    def record(
        cls,
        tenant_id: str,
        ledger_balance: Optional[int] = None,
        active_quota: Optional[float] = None,
        policy_ver: Optional[int] = None,
        last_op: Optional[str] = None,
        last_jwt: Optional[str] = None
    ) -> AccountRecord:
        """Updates or initializes a tenant record and persists to disk."""
        data = cls._load_all()

        if tenant_id not in data:
            data[tenant_id] = AccountRecord(
                tenant_id=tenant_id,
                is_default=(tenant_id == DEFAULT_TENANT_ID)
            )

        entry = data[tenant_id]
        is_modified = False

        updates = {
            "ledger_balance": ledger_balance,
            "active_quota": active_quota,
            "policy_ver": policy_ver,
            "last_op": last_op,
            "last_jwt": last_jwt
        }
        
        for key, value in updates.items():
            if value is not None and getattr(entry, key) != value:
                setattr(entry, key, value)
                is_modified = True

        if is_modified or tenant_id not in data:
            entry.updated_at = datetime.now(timezone.utc).isoformat()
            cls._save_all(data)
        
        return entry

    @classmethod
    def list_accounts(cls) -> Dict[str, Any]:
        """Returns all accounts as serializable dictionaries."""
        data = cls._load_all()
        
        if DEFAULT_TENANT_ID not in data:
            cls.record(DEFAULT_TENANT_ID, last_op="DEFAULT_REGISTERED")
            data = cls._load_all()
            
        return {k: asdict(v) for k, v in data.items()}
    
    @classmethod
    def get_registry_path(cls) -> Path:
        """Returns the absolute path of the registry file."""
        return ACCOUNT_REGISTRY_PATH