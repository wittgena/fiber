# bound.watcher.audit.warden
## @lineage: xphi.watcher.audit.warden
"""
@desc: 
- CPython Runtime Audit Hook (PEP 578) based outbound control module.
- Monitors runtime events based on externally injected security policies and 
  permanently records them to the immutable Merkle-backed store.
"""
import sys
import os
import threading
from typing import Set, Tuple, Any, Dict

from bound.watcher.audit.tracer import get_network_caller_origin

from watcher.kernel.compiler import ToposState
from watcher.kernel.store import KernelStore, ToposBlob
from watcher.plane.emitter import get_emitter

log = get_emitter("audit.warden", phase="KERNEL")

class WardenTLS(threading.local):
    """@desc: Thread-local storage to safely track audit hook reentrancy state."""
    def __init__(self):
        self.in_hook = False

class AuditWarden:
    """
    @desc: Core runtime warden that enforces network and OS-level boundaries.
    @security_model: Dynamic Egress Control & Shell Isolation
    """
    
    ## @state.policies: Internal repository for managing dynamic policies
    _policies: Dict[str, Set[str]] = {
        "allowed_hosts": {"nexus.next-phase.com"},
        "restricted_domains": set(),
        "dangerous_cmds": set()
    }
    _is_active: bool = False
    _store: KernelStore = None 
    _tls = WardenTLS()

    @classmethod
    def inject_policies(cls, policies: Dict[str, list], overwrite: bool = False) -> None:
        """
        @desc: Injects security policies from external sources (e.g., Agent, Config, Bootstrap).
        - overwrite=True: Completely replaces existing policies.
        - overwrite=False: Merges with existing policies.
        @injection.phase: Pre-flight or Dynamic Runtime
        """
        for key in cls._policies.keys():
            if key in policies:
                new_policy_set = set(policies[key])
                if overwrite:
                    cls._policies[key] = new_policy_set
                else:
                    cls._policies[key].update(new_policy_set)
                    
        log.info(f"[Warden] Security policies injected. Current allowed hosts: {len(cls._policies['allowed_hosts'])}")

    @classmethod
    def _resolve_host(cls, address: Any) -> str:
        """@desc: Safely extracts the host string from a socket address tuple."""
        if isinstance(address, tuple):
            return str(address[0])
        elif isinstance(address, str):
            return address
        return ""

    @classmethod
    def _record_to_store(cls, action: str, tension_penalty: float, details: str) -> None:
        """
        @desc: Records detected global security events directly to the Merkle DAG Store.
               Acts as an Out-of-Band (OOB) system anomaly log.
        @action: Synchronous Blob generation and storage (Supports Consensus FOLLOWER bypass)
        """
        if not cls._store:
            return

        blob = ToposBlob(
            action=f"SYS_WARDEN_GUARD::{action}", 
            from_state=ToposState.TRANSITIONAL.value, 
            to_state=ToposState.LINEAR.value,
            tension=tension_penalty, 
            details=details
        )
        
        try:
            blob_hash = cls._store.save_transition(blob)
            log.debug(f"[Warden: Ledger] System anomaly recorded. Blob Hash: {blob_hash[:8]}")
        except Exception as e:
            log.error(f"[Warden: Ledger] Failed to record anomaly to store: {e}")

    @classmethod
    def _audit_hook(cls, event: str, args: Tuple[Any, ...]) -> None:
        """
        @desc: Core callback hook triggered by CPython internal events.
        @guard.layer: CPython Interpreter Level
        """
        # Prevent reentrancy: Ignore events triggered by the Warden itself (e.g., Redis Sync Client)
        if cls._tls.in_hook:
            return

        cls._tls.in_hook = True
        try:
            ## @guard.check: Socket Connection Control
            if event == "socket.connect":
                if len(args) < 2:
                    return
                sock, address = args[:2]
                host = cls._resolve_host(address)

                ## @guard.check: Whitelist validation based on injected policies
                if host not in cls._policies["allowed_hosts"]:
                    port = address[1] if isinstance(address, tuple) and len(address) > 1 else "Unknown"
                    strict_mode = os.environ.get("BRANE_AIRGAP_MODE", "0") == "1"
                    caller_info = get_network_caller_origin()

                    if strict_mode:
                        ## @action: Fail-Closed (Strict Mode Enforcement)
                        msg = f"Unauthorized external network call blocked: {host}:{port} | Origin: {caller_info}"
                        log.critical(f"[WARDEN: BLOCK] {msg}")
                        cls._record_to_store("egress.block", -1.0, msg)
                        raise PermissionError(f"[Brane Warden Air-Gap] Connection to {host}:{port} is blocked. | Origin: {caller_info}")
                    else:
                        ## @action: Audit Logging & Telemetry (Audit Mode)
                        msg = f"Third-party external communication detected: {host}:{port} | Origin: {caller_info}"
                        log.warning(f"[WARDEN: AUDIT] {msg}")
                        cls._record_to_store("egress.audit", -0.2, msg)
                        
                        ## @guard.check: High-risk restricted domain inspection
                        if any(domain in host for domain in cls._policies["restricted_domains"]):
                            alert_msg = f"Direct connection attempt to restricted domain ({host}). Check proxy integration."
                            log.error(f"[WARDEN: ALERT] {alert_msg}")
                            cls._record_to_store("egress.alert", -0.5, alert_msg)

            ## @guard.check: High-level HTTP request monitoring (urllib)
            elif event == "urllib.Request":
                url = str(args[0]) if args else "Unknown"
                if not any(url.startswith(f"http://{h}") or url.startswith(f"https://{h}") for h in cls._policies["allowed_hosts"]):
                    msg = f"Outbound HTTP request detected: {url}"
                    log.info(f"[WARDEN: HTTP] {msg}")
                    cls._record_to_store("http.audit", -0.1, msg)

            ## @guard.check: Subprocess spawning and shell escape monitoring
            elif event in ("os.system", "subprocess.Popen"):
                cmd = str(args[0]) if args else "Unknown"
                msg = f"Subprocess execution detected: {cmd}"
                log.debug(f"[WARDEN: OS] {msg}")
                
                ## @guard.check: Dangerous command policy enforcement
                if any(d in cmd for d in cls._policies["dangerous_cmds"]):
                    cls._record_to_store("os.shell_escape_alert", -0.8, msg)
                    
        finally:
            # Release reentrancy guard regardless of exceptions
            cls._tls.in_hook = False

    @classmethod
    def install(cls, initial_policies: Dict[str, list] = None, store_instance: KernelStore = None) -> None:
        """
        @desc: Installs the Warden hook into the system runtime.
               Requires the direct injection of a KernelStore instance to decouple from compilation logic.
        @execution.lifecycle: System Initialization
        """
        if cls._is_active:
            log.debug("[Warden] Audit hook is already active.")
            return

        cls._store = store_instance or KernelStore()
        if initial_policies:
            cls.inject_policies(initial_policies, overwrite=True)

        try:
            ## @action: Register immutable runtime hook
            sys.addaudithook(cls._audit_hook)
            cls._is_active = True
            mode = "STRICT (AIR-GAPPED)" if os.environ.get("BRANE_AIRGAP_MODE") == "1" else "AUDIT (LOGGING)"
            log.info(f"[Warden] System Runtime Audit Hook established. Egress control mode: {mode}")
            cls._record_to_store("system.warden_init", 0.0, f"Warden initialized in {mode} mode.")
        except Exception as e:
            ## @failure.mode: Initialization Failure (Cannot guarantee system boundary)
            log.critical(f"[Warden] Failed to install audit hook: {e}")
            raise RuntimeError("Warden installation failed. Cannot guarantee system boundary.") from e