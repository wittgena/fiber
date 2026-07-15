# anchor.phase.ingress.pypi.rule.projector
import re
from typing import Dict, Optional
from dataclasses import dataclass
from aiohttp import web
from pydantic import BaseModel, Field

from bound.adapter.compiler import CompilerBridge
from watcher.plane.emitter import get_emitter

log = get_emitter("pypi.projector")

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

# ---------------------------------------------------------
# 2. Unified Projector (Evaluator 통폐합)
# ---------------------------------------------------------
class MetaProjector:
    def __init__(self, bridge: CompilerBridge):
        self.bridge = bridge
        self.rules: Dict[str, MetaRuleDef] = {}

    def load_rule(self, rule_id: str, rule_def: MetaRuleDef):
        self.rules[rule_id] = rule_def

    async def evaluate_pre_fetch(self, ctx: SecurityContext):
        """요청 직후(데이터 수신 전) 평가"""
        for rule_id, rule in self.rules.items():
            if self._is_match(ctx, rule, phase="pre"):
                await self._trigger_action(rule.action, rule_id, ctx)

    async def evaluate_post_fetch(self, ctx: SecurityContext):
        """데이터 수신 후(Substance 해시 확정) 평가"""
        for rule_id, rule in self.rules.items():
            if rule.asset and rule.asset.target_substance_hash:
                if self._is_match(ctx, rule, phase="post"):
                    await self._trigger_action(rule.action, rule_id, ctx)

    def _is_match(self, ctx: SecurityContext, rule: MetaRuleDef, phase: str) -> bool:
        """Actor, Vector, Asset 조건을 하나의 함수에서 인라인으로 평가 (AND 조건)"""
        
        # 1. Actor Check
        if rule.actor:
            actor_match = False
            if rule.actor.blacklist_ip and ctx.origin_ip in rule.actor.blacklist_ip: actor_match = True
            elif rule.actor.require_auth and not ctx.auth_header: actor_match = True
            if not actor_match: return False

        # 2. Vector Check
        if rule.vector:
            vector_match = False
            if rule.vector.max_uri_len and len(ctx.envelope_path) > rule.vector.max_uri_len: vector_match = True
            elif rule.vector.path_regex and re.search(rule.vector.path_regex, ctx.envelope_path): vector_match = True
            if not vector_match: return False

        # 3. Asset Check
        if rule.asset:
            if phase == "pre":
                if ctx.nominal_name != rule.asset.target_nominal: return False
                if rule.asset.target_topology and ctx.topology_version != rule.asset.target_topology: return False
            elif phase == "post":
                if rule.asset.target_substance_hash and ctx.substance_hash != rule.asset.target_substance_hash: return False

        return True

    async def _trigger_action(self, action: str, rule_id: str, ctx: SecurityContext):
        """위반 시 aiohttp 내장 HTTPForbidden 예외 발생"""
        if action == "ledger_tension":
            await self.bridge.authorize(
                action_id=f"proxy.tension.{rule_id}",
                payload={"rule": rule_id, "ip": ctx.origin_ip},
                metadata={"severity": "HIGH"}
            )
            raise web.HTTPForbidden(reason="Blocked by Topological Tension")
        raise web.HTTPForbidden(reason=f"Blocked by Meta Projection (Rule: {rule_id})")

projector = MetaProjector(CompilerBridge())