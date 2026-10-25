# fiber.infra.ops.account.registry
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field, asdict, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from xphi.node.space.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("bound.account.registry")

DEFAULT_TENANT_ID = "accounting_dept"
SIGN_DIR = Path(str(resolve_path("sign")))
ACCOUNT_REGISTRY_PATH = SIGN_DIR / "local_accounts.json"
AUDIT_DB_PATH = SIGN_DIR / "deploy_audit.sqlite"
BASE_SPIFFE_DOMAIN = "spiffe://self/"


def _normalize_spiffe_id(spiffe_id: str) -> str:
    spiffe_id = spiffe_id.strip()
    if spiffe_id.startswith("spiffe://"):
        return spiffe_id
    return f"{BASE_SPIFFE_DOMAIN}{spiffe_id}"


def _warn_if_totp_unregistered(spiffe_id: str) -> None:
    """Checks if SPIFFE ID exists in deploy_audit.sqlite and logs a warning if missing."""
    if not AUDIT_DB_PATH.exists():
        log.warning(
            f"[AccountRegistry] TOTP DB not found. SPIFFE '{spiffe_id}' is bound, "
            f"but TOTP is not enrolled yet (Run: fiber sign enroll -u {spiffe_id})."
        )
        return

    try:
        conn = sqlite3.connect(str(AUDIT_DB_PATH), timeout=2.0)
        row = conn.execute(
            "SELECT 1 FROM admin_users WHERE user_id = ? LIMIT 1", (spiffe_id,)
        ).fetchone()
        conn.close()
        if not row:
            log.warning(
                f"[AccountRegistry] SPIFFE '{spiffe_id}' is not enrolled in TOTP DB "
                f"(Run: fiber sign enroll -u {spiffe_id})."
            )
    except sqlite3.OperationalError:
        # admin_users 테이블이 아직 생성되지 않은 경우
        log.warning(
            f"[AccountRegistry] TOTP table not initialized. SPIFFE '{spiffe_id}' has no TOTP enrolled."
        )
    except Exception as e:
        log.warning(f"[AccountRegistry] Could not verify TOTP status for '{spiffe_id}': {e}")


@dataclass
class AccountRecord:
    """Local Account State Data Model"""
    tenant_id: str
    spiffe_id: Optional[str] = None  # 단일 SPIFFE ID 매핑 필드 추가
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
        spiffe_id: Optional[str] = None,
        ledger_balance: Optional[int] = None,
        active_quota: Optional[float] = None,
        policy_ver: Optional[int] = None,
        last_op: Optional[str] = None,
        last_jwt: Optional[str] = None
    ) -> AccountRecord:
        """Updates or initializes a tenant record and persists to disk."""
        data = cls._load_all()
        is_new = tenant_id not in data

        normalized_spiffe = _normalize_spiffe_id(spiffe_id) if spiffe_id else None

        if is_new:
            data[tenant_id] = AccountRecord(
                tenant_id=tenant_id,
                spiffe_id=normalized_spiffe,
                is_default=(tenant_id == DEFAULT_TENANT_ID)
            )
            if normalized_spiffe:
                _warn_if_totp_unregistered(normalized_spiffe)

        entry = data[tenant_id]
        is_modified = False

        # 기존 계정에 새 spiffe_id가 들어오거나 변경된 경우에도 TOTP 체크
        if normalized_spiffe is not None and entry.spiffe_id != normalized_spiffe:
            _warn_if_totp_unregistered(normalized_spiffe)

        updates = {
            "spiffe_id": normalized_spiffe,
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

        if is_modified or is_new:
            entry.updated_at = datetime.now(timezone.utc).isoformat()
            cls._save_all(data)
        
        return entry

    @classmethod
    def list_accounts(cls) -> Dict[str, Any]:
        data = cls._load_all()
        
        if DEFAULT_TENANT_ID not in data:
            cls.record(DEFAULT_TENANT_ID, last_op="DEFAULT_REGISTERED")
            data = cls._load_all()
            
        return {k: asdict(v) for k, v in data.items()}
    
    @classmethod
    def get_registry_path(cls) -> Path:
        return ACCOUNT_REGISTRY_PATH