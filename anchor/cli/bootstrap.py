# anchor.cli.bootstrap
# anchor/cli/bootstrap.py
"""
@desc:
- Core System Bootstrap & Membrane Ignition Sequence.
- Can be executed immediately upon module import, or called explicitly via ignite() from the entry point
"""
import sys
import json
import os

from bound.watcher.audit.warden import AuditWarden
from phase.bind.redirector import PhaseAirlock
from watcher.plane.emitter import get_emitter

log = get_emitter("cli.bootstrap", phase="anchor")

# @state.membrane_flag: Global state to ensure singleton-like execution
_MEMBRANE_ESTABLISHED = False

# @mock.data: Extracted mock intelligence data for easier maintenance.
# Injected with 'nexus.next-phase.com' to resolve the SecureBuilder whitelist issue.
_MOCK_INTELLIGENCE_DATA = {
    "warden_policies": {
        "allowed_hosts": [
            "localhost", 
            "127.0.0.1", 
            "fiber.internal",
            "nexus.next-phase.com"
        ],
        "restricted_domains": [
            "api.openai.com", 
            "telemetry.litellm.ai", 
            "malware.cnc.net"
        ],
        "dangerous_cmds": [
            "nc", 
            "wget", 
            "curl", 
            "bash"
        ]
    },
    "quarantine_targets": [
        {
            "legacy_path": "vuln_lib", 
            "canonical_path": "bound.security.dummy",
            "reason": "CVE-2026-0001: RCE in vuln_lib"
        },
        {
            "legacy_path": "litellm.telemetry", 
            "canonical_path": "bound.security.blackhole",
            "reason": "Unauthorized phoning home detected"
        }
    ]
}

def _fetch_agent_intelligence() -> dict:
    """
    @desc: Fetches emergency security payloads from an external intelligence source 
           (e.g., Security Agent, Redis, Config file).
    @action: Simulates fetching a raw JSON string and parsing it.
    """
    # 1. Simulate the receipt of a raw JSON payload from an external source
    raw_json_payload = json.dumps(_MOCK_INTELLIGENCE_DATA)
    
    # 2. Parse the payload (representing the actual production logic)
    try:
        return json.loads(raw_json_payload)
    except Exception as e:
        ## @action: Fail-Safe (Return empty policy to trigger Strict Air-Gap default)
        log.error(f"[Bootstrap] Failed to parse agent intelligence: {e}")
        return {}


def _initialize_subsystems():
    """
    @desc: Constructs the system defense membrane based on external intelligence
    @injection.phase: Pre-Flight Subsystem Orchestration
    """
    intel = _fetch_agent_intelligence()
    
    ## @guard.1: Activate Outbound/OS Control Layer (Network & System Boundary)
    ## @action: Utilize the dynamic policy injection capability of AuditWarden
    warden_policies = intel.get("warden_policies", {})
    AuditWarden.install(initial_policies=warden_policies)
    
    ## @guard.2: Form Runtime Module Hijack/Bypass Membrane (Memory & Dependency Boundary)
    quarantine_targets = intel.get("quarantine_targets", [])
    if quarantine_targets:
        log.info(f"[Bootstrap] Constructing Phase Membrane. {len(quarantine_targets)} targets identified for quarantine.")
        
        for target in quarantine_targets:
            legacy = target.get("legacy_path")
            canonical = target.get("canonical_path")
            reason = target.get("reason", "Unknown security directive")
            
            if legacy and canonical:
                ## @action: Establish resonance to hijack legacy module routing
                PhaseAirlock.establish_resonance(
                    legacy_path=legacy,
                    canonical_path=canonical
                )
                log.warning(f"[Membrane] Hijacked '{legacy}' -> '{canonical}'. (Reason: {reason})")

def ignite():
    """
    @desc: Initializes the core defense membrane and orchestrates dependency injection.
    @execution.lifecycle: Entry Point Ignition
    """
    global _MEMBRANE_ESTABLISHED
    if _MEMBRANE_ESTABLISHED:
        log.debug("[Bootstrap] Phase membrane already active. Skipping.")
        return
    
    log.info("[Bootstrap] Igniting System Bootstrap Sequence...")
    
    try:
        ## @action: Construct membranes and initialize critical subsystems
        _initialize_subsystems()
        _MEMBRANE_ESTABLISHED = True
        log.info("[Bootstrap] System bootstrap complete. All membranes and wardens are active.")
    except Exception as e:
        ## @failure.mode: Fail-Closed (Halt startup to prevent state corruption on ignition failure)
        log.critical(f"[Bootstrap] Critical failure during ignition sequence: {e}")
        raise RuntimeError("System assimilation failed. Halting startup to prevent state corruption.") from e