# fiber.infra.vcr.fixture.synth
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import fiber.bridge.llm.entry as llm_entry
from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser
from fiber.infra.vcr.fixture.schema import (
    SynthContext,
    VCRSynthRequest,
    VCRSynthResult,
    VCRSynthSpec,
    VCRTargetInfo,
    VCRToolCallSpec,
)
from fiber.infra.vcr.injector import VCRInjector
from fiber.infra.vcr.manager import VCRManager, VCRPlaybackConfig
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fixture.synth")


class VCRSpecGenerator:
    """VCRSynthRequest로부터 결정론적 VCRSynthSpec 인스턴스를 생성합니다."""

    @classmethod
    def generate(cls, req: VCRSynthRequest, provider: str) -> VCRSynthSpec:
        tc_spec = cls._parse_force_tool(req.force_tool) or (
            cls._infer_tool(req.prompt_text, req.effective_tools) if req.effective_tools else None
        )

        if tc_spec:
            content = (
                req.instruction.split(":", 1)[1].strip()
                if (req.instruction and "content:" in req.instruction.lower())
                else "\n\n"
            )
            chunks = cls._split_chunks(content, 1)
        else:
            directive = f" [Directive: {req.instruction}]" if req.instruction else ""
            content = f"[{provider.upper()}] Processed: '{req.prompt_text.strip()}'.{directive}"
            chunks = cls._split_chunks(content, target_pieces=6 if req.stream else 1)

        return VCRSynthSpec(
            content=content,
            chunks=chunks,
            tool_call=tc_spec,
            prompt_tokens=max(32, len(req.prompt_text) // 3 + 32),
            completion_tokens=max(16, len(content) // 3 + (32 if tc_spec else 0)),
        )

    @staticmethod
    def _split_chunks(text: str, target_pieces: int = 6) -> List[str]:
        if not text or text == "\n\n":
            return [text or "\n\n"]
        words = re.findall(r"\S+\s*", text)
        if len(words) <= target_pieces:
            return words
        step = max(1, len(words) // target_pieces)
        return ["".join(words[i : i + step]) for i in range(0, len(words), step)]

    @staticmethod
    def _parse_force_tool(force_tool: Optional[str]) -> Optional[VCRToolCallSpec]:
        if not force_tool or not force_tool.strip():
            return None
        expr = force_tool.strip()
        if ":" in expr and not expr.endswith(")"):
            name, raw_args = expr.split(":", 1)
            try:
                args = json.loads(raw_args.strip())
            except Exception:
                args = {"raw": raw_args.strip()}
            return VCRToolCallSpec(name=name.strip(), arguments=args)

        m = re.match(r"^([a-zA-Z0-9_.-]+)\((.*)\)$", expr, flags=re.DOTALL)
        if m:
            name, inner = m.group(1).strip(), m.group(2).strip()
            if not inner:
                return VCRToolCallSpec(name=name, arguments={})
            if inner.startswith("{"):
                try:
                    return VCRToolCallSpec(name=name, arguments=json.loads(inner))
                except Exception:
                    pass
            args = {
                k.strip(): v.strip().strip("'\"")
                for pair in inner.split(",")
                if "=" in pair
                for k, v in [pair.split("=", 1)]
            }
            return VCRToolCallSpec(name=name, arguments=args)
        return VCRToolCallSpec(name=expr, arguments={"tenant_id": "accounting_dept"})

    @staticmethod
    def _infer_tool(prompt: str, tools: List[Dict[str, Any]]) -> VCRToolCallSpec:
        first_fn = (tools[0].get("function") or {}) if isinstance(tools[0], dict) else {}
        fn_name = first_fn.get("name", "query_ledger_balance")
        quoted = re.findall(r"'([^']+)'", prompt)
        req_keys = (first_fn.get("parameters") or {}).get("required") or ["tenant_id"]
        return VCRToolCallSpec(name=fn_name, arguments={req_keys[0]: quoted[-1] if quoted else "accounting_dept"})


@dataclass
class VCRFixtureSynthesizer:
    """픽스처 합성, VCRManager 저장, VCRInjector 리플레이 실행을 총괄하는 엔진"""
    vcr_config: VCRPlaybackConfig = field(default_factory=lambda: VCRPlaybackConfig(mode="replay", speed="max"))
    target_dir: Optional[str] = None

    def synthesize_and_save(self, req: VCRSynthRequest) -> VCRSynthResult:
        target = VCRTargetInfo.resolve(req.target_model, self.target_dir, is_multi=req.is_multi)
        spec = VCRSpecGenerator.generate(req, target.provider)
        trace_id = target.generate_trace_id(req.scenario_id)
        trace_state = spec.to_trace_state(trace_id, target, req)

        fpath = target.fixture_path(req.scenario_id)
        vcr_mgr = VCRManager(config=VCRPlaybackConfig(mode="record"), fixture_dir=str(target.fixture_dir))
        ctx = SynthContext(scenario=req.scenario_id, filename=fpath.name)
        vcr_mgr.save_fixture(trace_id, trace_state.model_dump(exclude_none=True), ctx)

        return VCRSynthResult(
            trace_id=trace_id,
            provider=target.provider,
            fixture_path=fpath,
            content=spec.content,
            usage=spec.usage_dict,
        )

    async def emulate_and_replay(self, req: VCRSynthRequest) -> VCRSynthResult:
        saved = self.synthesize_and_save(req)
        replay_cfg = VCRPlaybackConfig(
            mode="replay",
            speed=self.vcr_config.speed,
            chaos_latency_ms=self.vcr_config.chaos_latency_ms,
            include_raw_payload=False,
        )

        with VCRInjector.use_cassette(config=replay_cfg, fixture_dir=str(saved.fixture_path.parent)):
            kwargs: Dict[str, Any] = {"api_key": "sk-vcr-synth"}
            if req.stream:
                kwargs["stream_options"] = {"include_usage": True}
            if req.effective_tools:
                kwargs["tools"] = req.effective_tools

            response = await llm_entry.acompletion(
                model=req.target_model,
                messages=req.messages,
                stream=req.stream,
                metadata=SynthContext(scenario=req.scenario_id, filename=saved.fixture_path.name).metadata,
                **kwargs,
            )
            text, tools_out, usage = await self._consume_response(response, saved.provider, req.stream)

        return VCRSynthResult(
            trace_id=saved.trace_id,
            provider=saved.provider,
            fixture_path=saved.fixture_path,
            content=text,
            tool_calls=tools_out,
            usage=usage,
        )

    @staticmethod
    async def _consume_response(response: Any, provider: str, stream: bool):
        full_text, extracted_tools, usage_dict = "", None, None
        if stream:
            async for chunk in response:
                if parsed := StreamChunkParser.parse(provider, chunk):
                    if piece := parsed.get("text", ""):
                        full_text += piece
                        sys.stdout.write(piece)
                        sys.stdout.flush()
                    if parsed.get("usage"):
                        usage_dict = parsed["usage"]
                if tc := StateMapper.extract_tool_calls(chunk, provider):
                    extracted_tools = tc
        else:
            full_text, usage_dict = StateMapper.extract_response(response, provider)
            extracted_tools = StateMapper.extract_tool_calls(response, provider)
            if extracted_tools:
                for tc in extracted_tools:
                    fn = tc.get("function") or {}
                    sys.stdout.write(f"[TOOL_CALL] {fn.get('name')}({fn.get('arguments', '{}')})\n")
            elif full_text:
                sys.stdout.write(full_text)
            sys.stdout.flush()
        print()
        return full_text, extracted_tools, usage_dict