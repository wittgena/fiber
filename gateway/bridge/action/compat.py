# fiber.gateway.bridge.action.compat
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import Any, List, Optional

import fiber.llm.entry as llm_entry
from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser
from fiber.infra.vcr.fixture.schema import (
    DEFAULT_SYNTH_TOOLS,
    SynthContext,
    VCRTargetInfo,
)
from fiber.infra.vcr.injector import VCRInjector
from fiber.infra.vcr.manager import VCRPlaybackConfig
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("action.compat")

# 하위 호환성 유지를 위한 Alias
COMPAT_SAMPLE_TOOLS = DEFAULT_SYNTH_TOOLS

STANDARD_PROMPTS: List[dict[str, Any]] = [
    {
        "scenario": "Stream Compat Traverser",
        "stream": True,
        "messages": [
            {
                "role": "user",
                "content": "Briefly explain the role of a 'State Traverser' in an LLM gateway using one short sentence. Then, provide a minimal JSON example of a declarative extraction rule that maps the path 'choices.0.delta.content'.",
            }
        ],
    },
    {
        "scenario": "Sync Declarative Rule",
        "stream": False,
        "messages": [
            {
                "role": "user",
                "content": "In one concise sentence, what is the primary architectural benefit of using external declarative JSON rulesets instead of hardcoding Python parsing logic for new LLM providers?",
            }
        ],
    },
    {
        "scenario": "Tool Call Compat Traverser",
        "stream": False,
        "tools": COMPAT_SAMPLE_TOOLS,
        "messages": [
            {
                "role": "user",
                "content": "Call the 'query_ledger_balance' tool to check the balance for tenant_id 'accounting_dept'.",
            }
        ],
    },
]

LOCAL_PROVIDERS = {"lm_studio", "llama_server", "ollama", "vllm"}


@dataclass
class CompatSuiteStats:
    """_execute_suite의 (passed, total, skipped) 3-튜플 및 배치 요약을 대체하는 결과 데이터 클래스"""
    model: str
    passed: int = 0
    executed: int = 0
    skipped: int = 0
    location: str = ""

    @property
    def all_passed(self) -> bool:
        return self.passed == self.executed and self.executed > 0


def _format_tool_calls(tool_calls: Optional[list[dict[str, Any]]]) -> str:
    if not tool_calls:
        return ""
    return "\n".join(
        f"[TOOL_CALL] {fn['name']}({fn.get('arguments', '{}')})"
        for tc in tool_calls
        if isinstance(tc, dict) and isinstance(fn := tc.get("function"), dict) and fn.get("name")
    )


def _is_unsupported_capability_error(exc: Exception) -> bool:
    cls_name = type(exc).__name__
    msg = str(exc).lower()
    return (
        "Unsupported" in cls_name
        or "unsupported by" in msg
        or "does not support tool" in msg
        or "function calling unsupported" in msg
    )


async def _execute_suite(
    target: VCRTargetInfo,
    final_api_key: Optional[str],
    invoker_mode: str,
) -> CompatSuiteStats:
    stats = CompatSuiteStats(model=target.full_model, location=str(target.fixture_dir))

    for test in STANDARD_PROMPTS:
        scenario_id = test["scenario"].replace(" ", "_").lower()
        fixture_path = target.fixture_path(scenario_id)
        is_stream = bool(test["stream"])
        tools = test.get("tools")

        if invoker_mode == "test_fixture" and tools and not fixture_path.exists():
            stats.skipped += 1
            log.info(f"⚪ [Compat: SKIPPED] '{fixture_path.name}' not present for {target.full_model}.")
            continue

        stats.executed += 1
        ctx = SynthContext(
            scenario=scenario_id,
            filename=fixture_path.name,
            invoker=f"action.compat.{invoker_mode}",
        )

        print(f"\n{'='*80}")
        print(f"[COMPAT: {invoker_mode.upper()}] Executing Test: {test['scenario']}")
        print(f"   ├─ Model    : {target.full_model}")
        print(f"   ├─ Provider : {target.provider} (Rule targeting)")
        print(f"   ├─ Stream   : {is_stream}" + (" | Tools: Enabled" if tools else ""))
        print(f"   └─ Prompt   : {test['messages'][0]['content'][:60]}...")
        print(f"{'-'*80}")

        try:
            kwargs: dict[str, Any] = {"stream_options": {"include_usage": True}} if is_stream else {}
            if final_api_key:
                kwargs["api_key"] = final_api_key
            if tools:
                kwargs["tools"] = tools

            start_time = time.perf_counter()
            response = await llm_entry.acompletion(
                model=target.full_model,
                messages=test["messages"],
                stream=is_stream,
                metadata=ctx.metadata,
                **kwargs,
            )

            full_content, usage_info, chunk_count = "", "N/A", 0
            print("[NETWORK] Connection established. Receiving payload...\n\n[AI RESPONSE]\n" + "-" * 40)

            if is_stream:
                async for chunk in response:
                    chunk_count += 1
                    if not (parsed_chunk := StreamChunkParser.parse(target.provider, chunk)):
                        continue
                    if text := parsed_chunk.get("text", ""):
                        full_content += text
                        sys.stdout.write(text)
                        sys.stdout.flush()
                    if (chunk_usage := parsed_chunk.get("usage")) and (
                        any(v is not None for v in chunk_usage.values()) if isinstance(chunk_usage, dict) else True
                    ):
                        usage_info = str(chunk_usage)
            else:
                content, usage_dict = StateMapper.extract_response(response, target.provider)
                if tools:
                    extracted_tools = StateMapper.extract_tool_calls(response, target.provider)
                    full_content = _format_tool_calls(extracted_tools)
                else:
                    full_content = content or ""
                if usage_dict:
                    usage_info = str(usage_dict)
                if full_content:
                    sys.stdout.write(full_content)
                    sys.stdout.flush()

            duration = (time.perf_counter() - start_time) * 1000
            print("\n\n" + "-" * 40 + f"\n[VALIDATION PHASE]\nfull_content = {full_content}")
            if not full_content.strip():
                target_kind = "tool_calls" if tools else "content"
                raise ValueError(f"Extracted {target_kind} is empty! Missing rule for '{target.provider}'")

            print(f"   ├─ Extracted Output  : OK ({len(full_content)} chars)")
            print(f"   ├─ Extracted Usage   : {usage_info}")
            if is_stream:
                print(f"   └─ Total Chunks      : {chunk_count}")

            log.info(f"[Compat: {invoker_mode.upper()}] Scenario Passed! ({duration:.2f}ms)")
            stats.passed += 1

        except Exception as e:
            print("\n" + "-" * 40)
            if _is_unsupported_capability_error(e):
                stats.executed -= 1
                stats.skipped += 1
                fixture_path.unlink(missing_ok=True)
                log.info(f"⚪ [Compat: UNSUPPORTED] Skipped '{test['scenario']}' -> {e}")
            else:
                log.error(f"[Compat: {invoker_mode.upper()}] Failed: {type(e).__name__} - {e}")

    return stats


async def run_gen_fixture(
    model: str,
    api_key: Optional[str] = None,
    target_dir: Optional[str] = None,
    vcr_tick: float = 100.0,
):
    models = VCRTargetInfo.parse_models(model)
    is_multi = len(models) > 1
    summary: List[CompatSuiteStats] = []

    for m in models:
        log.info(f"[Compat: GEN-FIXTURE] Resolving configuration for {m}...")
        try:
            target = VCRTargetInfo.resolve(m, target_dir, is_multi=is_multi, strict=True)
        except Exception as e:
            log.error(f"[Compat: GEN-FIXTURE] Invalid model identifier '{m}': {e}")
            if not is_multi:
                sys.exit(1)
            summary.append(CompatSuiteStats(model=m, executed=len(STANDARD_PROMPTS), location="INVALID_MODEL"))
            continue

        final_api_key = api_key or target.api_key or ("sk-mock-local-key" if target.provider in LOCAL_PROVIDERS else None)
        target.fixture_dir.mkdir(parents=True, exist_ok=True)

        vcr_cfg = VCRPlaybackConfig(mode="record", record_tick_ms=vcr_tick, include_raw_payload=True)
        with VCRInjector.use_cassette(config=vcr_cfg, fixture_dir=str(target.fixture_dir)):
            stats = await _execute_suite(target, final_api_key, "gen_fixture")
            summary.append(stats)
            skip_note = f", {stats.skipped} unsupported skipped" if stats.skipped else ""
            print(f"\n{'='*80}")
            log.info(
                f"[COMPAT: FIXTURE GENERATION COMPLETE] [{m}] "
                f"({stats.passed}/{stats.executed} passed{skip_note}) saved at: {target.fixture_dir}"
            )

    if is_multi:
        print(f"\n{'='*80}")
        log.info(f"[COMPAT: BATCH GENERATION SUMMARY] ({len(models)} models processed)")
        for s in summary:
            skip_str = f" (⚪ {s.skipped} unsupported)" if s.skipped else ""
            log.info(f"  • {s.model:<42} : {s.passed}/{s.executed} scenarios{skip_str} -> {s.location}")


async def run_test_fixture(
    model: str,
    target_dir: Optional[str] = None,
    vcr_speed: str = "max",
    vcr_chaos: float = 0.0,
    vcr_targets: str = "",
):
    models = VCRTargetInfo.parse_models(model)
    is_multi = len(models) > 1
    all_passed = True
    summary: List[CompatSuiteStats] = []

    for m in models:
        log.info(f"[Compat: TEST-FIXTURE] Preparing Replay environment for {m} (--vcr-speed {vcr_speed} --vcr-chaos {vcr_chaos})...")
        try:
            target = VCRTargetInfo.resolve(m, target_dir, is_multi=is_multi, strict=True)
        except Exception as e:
            log.error(f"[Compat: TEST-FIXTURE] Invalid model identifier '{m}': {e}")
            all_passed = False
            if not is_multi:
                sys.exit(1)
            continue

        if not target.fixture_dir.exists():
            log.error(f"[Compat: TEST-FIXTURE] Fixture directory not found: {target.fixture_dir}")
            all_passed = False
            if not is_multi:
                sys.exit(1)
            summary.append(CompatSuiteStats(model=m, executed=len(STANDARD_PROMPTS)))
            continue

        vcr_cfg = VCRPlaybackConfig(
            mode="replay",
            speed=vcr_speed,
            chaos_latency_ms=vcr_chaos,
            target_traces=vcr_targets,
            include_raw_payload=False,
        )
        with VCRInjector.use_cassette(config=vcr_cfg, fixture_dir=str(target.fixture_dir)):
            stats = await _execute_suite(target, "sk-vcr-replay-mock-key", "test_fixture")
            summary.append(stats)
            skip_note = f" ({stats.skipped} unsupported skipped)" if stats.skipped else ""
            print(f"\n{'='*80}")
            if stats.all_passed:
                log.info(f"[COMPAT: TEST SUCCESS] All ({stats.passed}/{stats.executed}) supported rules verified for {m}{skip_note}.")
            else:
                all_passed = False
                log.error(f"[COMPAT: TEST FAILED] ({stats.passed}/{stats.executed}) scenarios passed for {m}{skip_note}.")

    if is_multi:
        print(f"\n{'='*80}")
        log.info(f"[COMPAT: BATCH REPLAY SUMMARY] ({len(summary)} models tested)")
        for s in summary:
            status_icon = "✅" if s.all_passed else "❌"
            skip_str = f" (⚪ {s.skipped} unsupported)" if s.skipped else ""
            log.info(f"  {status_icon} {s.model:<42} : {s.passed}/{s.executed} passed{skip_str}")

    if not all_passed:
        sys.exit(1)