# topos.gateway.ingress.sentinel
## @lineage: void.topos.gateway.ingress.sentinel
## @lineage: topos.edge.gateway.ingress.sentinel
## @lineage: edge.gateway.ingress.sentinel
## @lineage: fiber.gateway.ingress.sentinel
"""
@module: fiber.gateway.ingress.sentinel
@desc: 
- Unified Endogenous Security Sentinel & Membrane Projector.
- [PART 1 & 2: Passive Defense] Evaluates spatial meta-rules for external data streams (e.g., PyPI).
- [PART 3: Active Defense] Continuously injects controlled attack payloads to verify firewall resilience.
- Delegates all topological tension and sealing authorities to the WASM Kernel Store.
"""
import asyncio
import random
import re
from typing import Dict, Any, Callable, List, Optional
from dataclasses import dataclass
from aiohttp import web
from pydantic import BaseModel, Field

from watcher.kernel.bridge.gateway import ToposGateway
from topos.gateway.ingress.transducer import SpecValidator
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("ingress.sentinel", phase="DEFENSE")


# =========================================================================
# PART 1: SPATIAL FENCE SCHEMAS (Meta Rules)
# =========================================================================

class ActorRule(BaseModel):
    blacklist_ip: Optional[list[str]] = None
    require_auth: Optional[bool] = False

class VectorRule(BaseModel):
    max_uri_len: Optional[int] = 2048
    path_regex: Optional[str] = None

class AssetRule(BaseModel):
    target_nominal: str
    target_topology: Optional[str] = None
    target_substance_hash: Optional[str] = None

class MetaRuleDef(BaseModel):
    actor: Optional[ActorRule] = None
    vector: Optional[VectorRule] = None
    asset: Optional[AssetRule] = None
    action: str = Field(default="block", pattern="^(block|ledger_tension)$")

@dataclass
class SecurityContext:
    origin_ip: str
    auth_header: Optional[str]
    envelope_path: str
    envelope_method: str
    nominal_name: str
    topology_version: Optional[str]
    substance_hash: Optional[str] = None


# =========================================================================
# PART 2: PASSIVE MEMBRANE PROJECTOR (Live Traffic Firewall)
# =========================================================================

class MembraneProjector:
    """
    @desc: Evaluates external requests against injected MetaRules.
           Delegates rule violations to the WASM Kernel for structural tension assessment.
    """
    def __init__(self, gateway: ToposGateway):
        self.gateway = gateway
        self.rules: Dict[str, MetaRuleDef] = {}

    def load_rule(self, rule_id: str, rule_def: MetaRuleDef):
        self.rules[rule_id] = rule_def

    async def evaluate_pre_fetch(self, ctx: SecurityContext):
        """@desc: Phase 1 Evaluation - executed immediately upon request receipt."""
        for rule_id, rule in self.rules.items():
            if self._is_match(ctx, rule, phase="pre"):
                await self._trigger_action(rule.action, rule_id, ctx)

    async def evaluate_post_fetch(self, ctx: SecurityContext):
        """@desc: Phase 2 Evaluation - executed after payload substance (Hash) is confirmed."""
        for rule_id, rule in self.rules.items():
            if rule.asset and rule.asset.target_substance_hash:
                if self._is_match(ctx, rule, phase="post"):
                    await self._trigger_action(rule.action, rule_id, ctx)

    def _is_match(self, ctx: SecurityContext, rule: MetaRuleDef, phase: str) -> bool:
        """@desc: Inline logical AND evaluation of Actor, Vector, and Asset rules."""
        
        if rule.actor:
            actor_match = False
            if rule.actor.blacklist_ip and ctx.origin_ip in rule.actor.blacklist_ip: actor_match = True
            elif rule.actor.require_auth and not ctx.auth_header: actor_match = True
            if not actor_match: return False

        if rule.vector:
            vector_match = False
            if rule.vector.max_uri_len and len(ctx.envelope_path) > rule.vector.max_uri_len: vector_match = True
            elif rule.vector.path_regex and re.search(rule.vector.path_regex, ctx.envelope_path): vector_match = True
            if not vector_match: return False

        if rule.asset:
            if phase == "pre":
                if ctx.nominal_name != rule.asset.target_nominal: return False
                if rule.asset.target_topology and ctx.topology_version != rule.asset.target_topology: return False
            elif phase == "post":
                if rule.asset.target_substance_hash and ctx.substance_hash != rule.asset.target_substance_hash: return False

        return True

    async def _trigger_action(self, action: str, rule_id: str, ctx: SecurityContext):
        """@action: Reports violation to WASM Kernel and ruptures the HTTP context."""
        if action == "ledger_tension":
            await self.gateway.authorize(
                action_id=f"proxy.tension.{rule_id}",
                action="SECURITY_TENSION_ALERT",
                payload={"rule": rule_id, "ip": ctx.origin_ip, "nominal": ctx.nominal_name},
                metadata={"severity": "HIGH"}
            )
            raise web.HTTPForbidden(reason="Blocked by Kernel Topological Tension")
        raise web.HTTPForbidden(reason=f"Blocked by Meta Projection (Rule: {rule_id})")


# =========================================================================
# PART 3: ACTIVE CHAOS ENGINE (Endogenous Testing)
# =========================================================================

class ChaosPayloadLibrary:
    """@desc: Registry of generated attack vectors for endogenous testing."""
    
    OOM = [
        lambda: b"A" * 6 * 1024 * 1024,                                 ## 6MB flat string (Breaks 5MB limit)
        lambda: b'{"data": "' + b"B" * 5 * 1024 * 1024 + b'"}',         ## Massive JSON value parsing
        lambda: b"[" * 50000 + b"]" * 50000,                            ## Deeply nested array (Recursion depth)
        lambda: b'{"a":' * 25000 + b'"b"' + b'}' * 25000                ## Deeply nested object
    ]

    SMUGGLING = [
        lambda: b'{"version": "1.0", "smuggled": {"version": "2.0", "bypass": true}}', 
        lambda: b'{"method": "initialize", "params": {"__proto__": {"admin": true}}}', 
        lambda: b'GET / HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\n\r\n',
        lambda: b'{"jsonrpc": "2.0"} \n\n {"hidden_payload": "trigger_rce"}'
    ]

    INVALID_STATE = [
        lambda: b"MALFORMED_NON_JSON_STREAM_DATA",                      
        lambda: b"\x00\x01\x02\x03\x04\xff\xfe\x00",                    
        lambda: b"<?xml version='1.0'?><root>bypass</root>",            
        lambda: b"--boundary\r\nContent-Disposition: form-data\r\n\r\n" 
    ]


class IngressRouter:
    """@desc: Internal router acting as the live target for the Sentinel."""
    def __init__(self, gateway: ToposGateway):
        self.validator = SpecValidator()
        self.gateway = gateway

    async def handle_request(self, headers: dict, raw_body: bytes, client_ip: str):
        with flow_scope(phase="ingress.router.pipeline", auto_flush=True, client_ip=client_ip) as flow_ctx:
            try:
                safe_stream = self.validator.process_ingress(headers, raw_body, client_ip)
                flow_ctx["stream_id"] = str(safe_stream.meta.stream_id)
                flow_ctx["intent"] = safe_stream.payload.intent.value

                is_authorized = await self.gateway.authorize_ingress(safe_stream)
                
                if not is_authorized:
                    flow_ctx["ledger_status"] = "denied"
                    log.warning("Topological sealing denied by the WASM Kernel Spatial Fence.")
                    return {"status": "denied"}
                    
                flow_ctx["ledger_status"] = "authorized"
                log.info("Request successfully sealed into the manifold by Kernel Store.")
                return {"status": "success", "intent": safe_stream.payload.intent.value}
                
            except ValueError as ve:
                log.warning(f"Ingress boundary dropped payload: {ve}")
                return {"status": "dropped", "reason": "Spec invariant violation"}
            except Exception as e:
                log.error(f"Manifold routing collapsed: {e}")
                return {"status": "error", "reason": "Internal boundary failure"}


class DefenseSentinel:
    """@desc: Chaos simulator that constantly attacks the router to ensure defensive integrity."""
    def __init__(self, router: IngressRouter):
        self.router = router
        self.attack_categories = [
            ("OOM_Parser_Exhaustion_Attack", ChaosPayloadLibrary.OOM),
            ("Polymorphic_Protocol_Smuggling", ChaosPayloadLibrary.SMUGGLING),
            ("Invalid_State_Transition_Bypass", ChaosPayloadLibrary.INVALID_STATE)
        ]

    async def verify_boundary_resilience(self):
        """@flow: Continuous endogenous vulnerability injection and boundary verification"""
        while True:
            for vector_name, rule_list in self.attack_categories:
                payload_generator = random.choice(rule_list)
                payload = payload_generator()

                with flow_scope(execution_mode="SIMULATION", security_probe=vector_name):
                    result: Dict[str, Any] = await self.router.handle_request(
                        headers={"authorization": "Bearer SIMULATED_PROBE_TOKEN"},
                        raw_body=payload,
                        client_ip="127.0.0.1"
                    )
                    
                    if result.get("status") != "dropped":
                        log.critical(
                            f"[BREACH_ALERT] Boundary Defenses Compromised! "
                            f"Vector '{vector_name}' successfully bypassed the Ingress Validator. "
                            f"Response Status: {result.get('status')}"
                        )
            
            await asyncio.sleep(300)

_projector_instance = None

def get_projector() -> MembraneProjector:
    """Singleton factory for the MembraneProjector."""
    global _projector_instance
    if _projector_instance is None:
        _projector_instance = MembraneProjector(ToposGateway())
    return _projector_instance