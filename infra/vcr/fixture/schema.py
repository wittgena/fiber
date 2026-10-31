# fiber.infra.vcr.fixture.schema
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from fiber.infra.vcr.manager import (
    VCRExceptionBoundary,
    VCRFixtureTraceState,
    VCRNetworkMetrics,
    VCRRequestContext,
)
from fiber.llm.model.resolver import get_llm_provider
from xphi.arch.contract.event.next import next_trace_id
from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.node.space.bind.resolver import find_current_self, resolve_path

DEFAULT_SYNTH_TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "query_ledger_balance",
            "description": "Fetch the remaining fuel balance for a target tenant account.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tenant_id": {
                        "type": "string",
                        "description": "The tenant identifier (e.g. accounting_dept)",
                    }
                },
                "required": ["tenant_id"],
            },
        },
    }
]


@dataclass(frozen=True)
class SynthContext:
    """VCRManager.save_fixture() 및 llm_entry.acompletion이 공유하는 VCR 메타데이터 어댑터"""
    scenario: str
    filename: str
    invoker: str = "fixture.synth"

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            "vcr_scenario": self.scenario,
            "vcr_invoker": self.invoker,
            "vcr_filename": self.filename,
        }

    @property
    def system_meta(self) -> "SynthContext":
        return self


@dataclass(frozen=True)
class VCRTargetInfo:
    """모델 해석 결과, 동적 API 키 및 픽스처 타겟 경로 정보 (compat & synth 공용)"""
    full_model: str
    model_short: str
    provider: str
    fixture_dir: Path
    api_key: Optional[str] = None

    def fixture_path(self, scenario_id: str) -> Path:
        name = scenario_id if scenario_id.endswith(".json") else f"{scenario_id}.json"
        return self.fixture_dir / name

    def generate_trace_id(self, scenario_id: str) -> str:
        seed = f"synth_{self.provider}_{self.model_short}_{scenario_id}_{int(time.time() * 1000)}"
        return next_trace_id(seed)

    @staticmethod
    def parse_models(model_arg: str) -> List[str]:
        items = [m.strip() for m in (model_arg or "").split(",") if m.strip()]
        if len(items) == 1 and items[0].lower() == "all":
            try:
                from fiber.dev.console.tab.vcr import discover_compat_models
                return discover_compat_models() or items
            except Exception:
                pass
        return items

    @staticmethod
    def resolve_base_dir() -> Path:
        if env_dir := os.getenv("FIBER_COMPATS_DIR"):
            return Path(env_dir).resolve()
        if self_dir := find_current_self():
            cand = self_dir.parent / "fiber-compats"
            if cand.is_dir():
                return cand.resolve()
        for rel in (Path("fiber-compats"), Path("../fiber-compats")):
            if rel.is_dir():
                return rel.resolve()
        fixture_root = Path(str(resolve_path("fixture")))
        if (fixture_root / "adapters").is_dir():
            return fixture_root.resolve()
        return Path(os.getcwd()) / "fiber-compats"

    @classmethod
    def resolve(
        cls,
        model: str,
        custom_dir: Optional[str] = None,
        is_multi: bool = False,
        strict: bool = False,
    ) -> "VCRTargetInfo":
        try:
            _, provider, dynamic_api_key, _ = get_llm_provider(model=model)
        except Exception:
            if strict:
                raise
            provider = model.split("/")[0] if "/" in model else "openai"
            dynamic_api_key = None

        model_short = model.split("/", 1)[1] if "/" in model else model
        safe_name = model.replace("/", "_").replace(":", "_")

        if custom_dir:
            base = Path(custom_dir)
            final_dir = (base / provider / safe_name) if is_multi else base
        else:
            final_dir = cls.resolve_base_dir() / "adapters" / provider / safe_name

        return cls(
            full_model=model,
            model_short=model_short,
            provider=provider,
            fixture_dir=final_dir,
            api_key=dynamic_api_key,
        )


@dataclass
class VCRSynthRequest:
    """픽스처 합성 요청 파라미터 캡슐화"""
    target_model: str
    messages: List[Dict[str, Any]]
    scenario_id: str = "dynamic_emulation"
    instruction: Optional[str] = None
    stream: bool = True
    tools: Optional[List[Dict[str, Any]]] = None
    force_tool: Optional[str] = None
    is_multi: bool = False

    @classmethod
    def from_prompt(cls, target_model: str, prompt: str, **kwargs: Any) -> "VCRSynthRequest":
        return cls(target_model=target_model, messages=[{"role": "user", "content": prompt}], **kwargs)

    @property
    def prompt_text(self) -> str:
        return self.messages[-1].get("content", "") if self.messages else ""

    @property
    def effective_tools(self) -> Optional[List[Dict[str, Any]]]:
        return self.tools or (DEFAULT_SYNTH_TOOLS if self.force_tool else None)


@dataclass(frozen=True)
class VCRSynthResult:
    """합성 및 리플레이 검증 결과 객체"""
    trace_id: str
    provider: str
    fixture_path: Path
    content: str = ""
    tool_calls: Optional[List[Dict[str, Any]]] = None
    usage: Optional[Dict[str, Any]] = None


class VCRToolCallSpec(DynamicSurgeModel):
    """합성된 단일 Tool Call 명세"""
    name: str
    arguments: Dict[str, Any] = {}

    def to_payload(self, call_id: str) -> Dict[str, Any]:
        args_str = json.dumps(self.arguments, ensure_ascii=False)
        return {
            "id": call_id,
            "type": "function",
            "function": {"name": self.name, "arguments": args_str},
        }


class VCRSynthSpec(DynamicSurgeModel):
    """생성된 픽스처 스펙 및 VCRFixtureTraceState 변환 모델"""
    content: str = "\n\n"
    chunks: List[str] = []
    tool_call: Optional[VCRToolCallSpec] = None
    ttfb_ms: float = 100.0
    delta_ms: float = 10.0
    prompt_tokens: int = 64
    completion_tokens: int = 32

    @property
    def total_duration_ms(self) -> float:
        return round(self.ttfb_ms + len(self.chunks) * self.delta_ms, 2)

    @property
    def usage_dict(self) -> Dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.prompt_tokens + self.completion_tokens,
        }

    def to_trace_state(self, trace_id: str, target: VCRTargetInfo, req: VCRSynthRequest) -> VCRFixtureTraceState:
        formatted_tc = [self.tool_call.to_payload(trace_id[:9])] if self.tool_call else None
        timeline: List[Dict[str, Any]] = []

        if req.stream:
            pieces = self.chunks or [self.content]
            for idx, ch in enumerate(pieces):
                entry: Dict[str, Any] = {"delta_ms": self.delta_ms, "chunk": str(ch)}
                if idx == len(pieces) - 1:
                    entry["finish_reason"] = "tool_calls" if formatted_tc else "stop"
                    if formatted_tc:
                        entry["tool_calls"] = formatted_tc
                timeline.append(entry)
        else:
            entry = {"delta_ms": 0.0, "chunk": self.content}
            if formatted_tc:
                entry["tool_calls"] = formatted_tc
                entry["finish_reason"] = "tool_calls"
            timeline.append(entry)

        return VCRFixtureTraceState(
            trace_id=trace_id,
            request_context=VCRRequestContext(
                model=target.model_short,
                provider=target.provider,
                messages=req.messages,
                parameters={"temperature": None, "max_tokens": None, "stream": req.stream},
            ),
            network_metrics=VCRNetworkMetrics(
                ttfb_ms=self.ttfb_ms,
                total_duration_ms=self.total_duration_ms,
            ),
            response_timeline=timeline,
            usage=self.usage_dict,
            exception_boundary=VCRExceptionBoundary(occurred=False),
        )