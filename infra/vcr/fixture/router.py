# fiber.infra.vcr.fixture.router
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.infra.vcr.fixture.schema import VCRSynthRequest
from fiber.infra.vcr.fixture.synth import VCRFixtureSynthesizer
from fiber.infra.vcr.manager import VCRManager
from xphi.node.space.bind.resolver import resolve_path
from xphi.state.phase.channel import ChannelContext, DuplexChannel
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fixture.router")

RULE_ROOT = Path(str(resolve_path("io"))) / "rule"
DEFAULT_RULE_FILE = RULE_ROOT / "fixture_routes.json"


@dataclass(frozen=True)
class FixtureRoutingRule:
    """단일 픽스처 라우팅 규칙 및 합성 지시어 정의"""
    route_id: str
    pattern: re.Pattern
    version: int = 1
    directive: Optional[str] = None
    force_tool: Optional[str] = None

    @property
    def scenario_id(self) -> str:
        return f"fixture_{self.route_id.lower()}_v{self.version}"

    @property
    def filename(self) -> str:
        return f"{self.scenario_id}.json"

    @classmethod
    def from_dict(cls, data: Dict[str, Any], version_override: Optional[int] = None) -> "FixtureRoutingRule":
        flag_val = 0
        for f_name in data.get("flags", ["IGNORECASE"]):
            if hasattr(re, f_name.upper()):
                flag_val |= getattr(re, f_name.upper())

        route_id = str(data["route_id"]).strip()
        version = version_override if version_override is not None else int(data.get("version", 1))
        return cls(
            route_id=route_id,
            pattern=re.compile(str(data.get("pattern", ".*")), flag_val),
            version=version,
            directive=data.get("directive"),
            force_tool=data.get("force_tool"),
        )


class FixtureRuleMatcher:
    """JSON 룰셋 파일로부터 라우팅 규칙을 로드하고 프롬프트 매칭을 수행합니다."""

    FALLBACK_RULE = FixtureRoutingRule("R0_DEFAULT_FALLBACK", re.compile(r".*", re.DOTALL))

    def __init__(
        self,
        rules: List[FixtureRoutingRule],
        fallback: FixtureRoutingRule = FALLBACK_RULE,
        probe_prefix: str = "FIBER-PROBE-",
    ):
        self.rules = rules
        self.fallback = fallback
        self.probe_prefix = probe_prefix

    @classmethod
    def from_json(
        cls,
        rule_path: Path = DEFAULT_RULE_FILE,
        active_versions: Optional[Dict[str, int]] = None,
    ) -> "FixtureRuleMatcher":
        versions = active_versions or {}
        if not rule_path.exists():
            log.warning(f"[FixtureRouter] Rule file not found at '{rule_path}'. Using default fallback only.")
            fb_ver = versions.get(cls.FALLBACK_RULE.route_id, 1)
            fb = FixtureRoutingRule(cls.FALLBACK_RULE.route_id, cls.FALLBACK_RULE.pattern, fb_ver)
            return cls(rules=[], fallback=fb)

        try:
            raw = json.loads(rule_path.read_text(encoding="utf-8"))
            probe_prefix = str(raw.get("probe_prefix", "FIBER-PROBE-"))

            rules = [
                FixtureRoutingRule.from_dict(item, versions.get(item.get("route_id")))
                for item in raw.get("routes", [])
                if isinstance(item, dict) and item.get("route_id")
            ]

            fb_data = raw.get("fallback")
            if isinstance(fb_data, dict) and fb_data.get("route_id"):
                fallback = FixtureRoutingRule.from_dict(fb_data, versions.get(fb_data["route_id"]))
            else:
                fb_ver = versions.get(cls.FALLBACK_RULE.route_id, 1)
                fallback = FixtureRoutingRule(cls.FALLBACK_RULE.route_id, cls.FALLBACK_RULE.pattern, fb_ver)

            log.info(f"[FixtureRouter] Loaded {len(rules)} routing rules from '{rule_path.name}'.")
            return cls(rules=rules, fallback=fallback, probe_prefix=probe_prefix)

        except Exception as e:
            log.error(f"[FixtureRouter] Failed to parse rule file '{rule_path}': {e}")
            return cls(rules=[], fallback=cls.FALLBACK_RULE)

    def match(self, text: str) -> FixtureRoutingRule:
        for rule in self.rules:
            if rule.pattern.search(text):
                return rule
        return self.fallback


class VCRFixtureRouter(DuplexChannel):
    """
    [In-Memory Fixture Server Router]
    RULE_ROOT의 JSON 규칙에 따라 인바운드 요청을 매칭하고,
    VCRManager.memory_fixtures에서 디스크 I/O 없이 즉시 리플레이하도록 바인딩합니다.
    """
    target_slot = PipelineSlot.PRE_TRANSLATE

    def __init__(
        self,
        vcr_manager: VCRManager,
        rule_path: Path = DEFAULT_RULE_FILE,
        active_versions: Optional[Dict[str, int]] = None,
        matcher: Optional[FixtureRuleMatcher] = None,
        synthesizer: Optional[VCRFixtureSynthesizer] = None,
        external_store: Optional[Any] = None,
    ):
        super().__init__()
        self.vcr_manager = vcr_manager
        self.matcher = matcher or FixtureRuleMatcher.from_json(rule_path, active_versions)
        self.synthesizer = synthesizer or VCRFixtureSynthesizer(target_dir=vcr_manager.fixture_dir)
        self.external_store = external_store
        self.probe_hits: Counter[str] = Counter()

    @staticmethod
    def _extract_text(messages: List[Any]) -> str:
        return " ".join(
            content.strip()
            for m in messages
            if isinstance(content := (m.get("content", "") if isinstance(m, dict) else getattr(m, "content", "")), str)
            and content.strip()
        )

    def _ensure_in_memory_fixture(
        self,
        rule: FixtureRoutingRule,
        model: str,
        messages: List[Dict[str, Any]],
        stream: bool,
    ) -> None:
        filepath_key = os.path.join(self.vcr_manager.fixture_dir, rule.filename)
        if filepath_key in self.vcr_manager.memory_fixtures:
            return

        directive = rule.directive or f"Include probe token {self.matcher.probe_prefix}{rule.scenario_id.upper()}"
        req = VCRSynthRequest(
            target_model=model,
            messages=messages or [{"role": "user", "content": "ping"}],
            scenario_id=rule.scenario_id,
            instruction=directive,
            stream=stream,
            force_tool=rule.force_tool,
        )
        self.synthesizer.synthesize_to_memory(req, self.vcr_manager)
        log.debug(f"⚡ [FixtureRouter] In-memory fixture synthesized: {rule.filename}")

    def _record_probe_hit(self, rule: FixtureRoutingRule) -> int:
        metric_key = f"fixture:probe_hit:{rule.route_id}:v{rule.version}"
        self.probe_hits[metric_key] += 1
        current_score = self.probe_hits[metric_key]
        if self.external_store is not None:
            try:
                raw_key = metric_key.encode("utf-8")
                self.external_store.db.put(raw_key, str(current_score).encode("utf-8"))
            except Exception as e:
                log.warning(f"[FixtureRouter] Store flush skipped: {e}")

        return current_score

    @staticmethod
    def _bind_vcr_metadata(ctx: ChannelContext, payload: dict, vcr_meta: Dict[str, Any]) -> None:
        payload.setdefault("metadata", {}).update(vcr_meta)
        if (sys_meta := payload.get("system_meta") or ctx.get_attr("system_meta")) is not None:
            if isinstance(target_dict := getattr(sys_meta, "metadata", None), dict):
                target_dict.update(vcr_meta)
            else:
                setattr(sys_meta, "metadata", dict(vcr_meta))

    async def write(self, ctx: ChannelContext, payload: dict):
        messages = payload.get("messages", [])
        prompt_text = self._extract_text(messages)
        model = payload.get("model", "openai/gpt-4o-mini")
        is_stream = bool(payload.get("stream", False))

        rule = self.matcher.match(prompt_text)
        self._ensure_in_memory_fixture(rule, model, messages, is_stream)

        probe_hit = self.matcher.probe_prefix in prompt_text
        if probe_hit:
            self._record_probe_hit(rule)

        vcr_meta = {
            "vcr_mode": "replay",
            "vcr_scenario": rule.scenario_id,
            "vcr_filename": rule.filename,
            "vcr_fixture_dir": self.vcr_manager.fixture_dir,
            "fixture_route_audit": {
                "route_id": rule.route_id,
                "version": rule.version,
                "probe_consumed": probe_hit,
                "payload_sha256": hashlib.sha256(prompt_text.encode("utf-8")).hexdigest()[:16],
            },
        }
        self._bind_vcr_metadata(ctx, payload, vcr_meta)

        return await ctx.fire_write(payload)