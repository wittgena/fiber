# xphi.proxy.rule.projector
## @lineage: bound.watcher.server.rule.projector
## @lineage: xphi.proxy.pypi.projector
import sys
import re
import json
import asyncio
import hashlib
from typing import Dict, Any, Optional
from dataclasses import dataclass
from aiohttp import web, ClientSession
from pydantic import BaseModel, Field

from bound.adapter.bridge.ledger import LedgerBridge
from xphi.proxy.gatekeeper import GatekeeperRejection
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
    # Pre-fetch Contexts
    origin_ip: str
    auth_header: Optional[str]
    envelope_path: str
    envelope_method: str
    nominal_name: str
    topology_version: Optional[str]
    # Post-fetch Context
    substance_hash: Optional[str] = None

class ActorEvaluator:
    def evaluate(self, ctx: SecurityContext, rule: ActorRule) -> bool:
        if rule.blacklist_ip and ctx.origin_ip in rule.blacklist_ip: return True
        if rule.require_auth and not ctx.auth_header: return True
        return False

class VectorEvaluator:
    def evaluate(self, ctx: SecurityContext, rule: VectorRule) -> bool:
        if rule.max_uri_len and len(ctx.envelope_path) > rule.max_uri_len: return True
        if rule.path_regex and re.search(rule.path_regex, ctx.envelope_path): return True
        return False

class AssetEvaluator:
    def evaluate_pre(self, ctx: SecurityContext, rule: AssetRule) -> bool:
        """이름과 버전만으로 선제적 평가"""
        if ctx.nominal_name != rule.target_nominal: return False
        if rule.target_topology and ctx.topology_version != rule.target_topology: return False
        return True

    def evaluate_post(self, ctx: SecurityContext, rule: AssetRule) -> bool:
        """다운로드된 바이너리의 해시 평가"""
        if not rule.target_substance_hash: return False # 해시 룰이 없으면 무시
        return ctx.substance_hash == rule.target_substance_hash

class MetaProjector:
    def __init__(self, bridge):
        self.bridge = bridge
        self.rules: Dict[str, MetaRuleDef] = {}
        self.axes = {
            "actor": ActorEvaluator(),
            "vector": VectorEvaluator(),
            "asset": AssetEvaluator()
        }

    def load_rule(self, rule_id: str, rule_def: MetaRuleDef):
        self.rules[rule_id] = rule_def

    async def evaluate_pre_fetch(self, ctx: SecurityContext):
        """요청 직후(데이터 수신 전) 3차원 투영"""
        for rule_id, rule in self.rules.items():
            if self._is_projected(ctx, rule, phase="pre"):
                await self._trigger_action(rule.action, rule_id, ctx)

    async def evaluate_post_fetch(self, ctx: SecurityContext):
        """데이터 수신 후(Substance 확정) 재투영"""
        for rule_id, rule in self.rules.items():
            if rule.asset and rule.asset.target_substance_hash:
                if self._is_projected(ctx, rule, phase="post"):
                    await self._trigger_action(rule.action, rule_id, ctx)

    def _is_projected(self, ctx: SecurityContext, rule: MetaRuleDef, phase: str) -> bool:
        is_matched = True
        
        if rule.actor and not self.axes["actor"].evaluate(ctx, rule.actor): is_matched = False
        if rule.vector and not self.axes["vector"].evaluate(ctx, rule.vector): is_matched = False
        if rule.asset:
            if phase == "pre" and not self.axes["asset"].evaluate_pre(ctx, rule.asset):
                is_matched = False
            elif phase == "post" and not self.axes["asset"].evaluate_post(ctx, rule.asset):
                is_matched = False

        return is_matched

    async def _trigger_action(self, action: str, rule_id: str, ctx: SecurityContext):
        if action == "block":
            raise GatekeeperRejection(403, f"Blocked by Meta Projection (Rule: {rule_id})")
        elif action == "ledger_tension":
            await self.bridge.authorize(
                action_id=f"proxy.tension.{rule_id}",
                payload={"rule": rule_id, "ip": ctx.origin_ip},
                metadata={"severity": "HIGH"}
            )
            raise GatekeeperRejection(403, "Blocked by Topological Tension")

projector = MetaProjector(LedgerBridge())