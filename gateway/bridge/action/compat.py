# fiber.gateway.bridge.action.compat
import os
import sys
import asyncio
import time
from pathlib import Path
from typing import Any, Optional

from fiber.llm.model.resolver import get_llm_provider
import fiber.llm.entry as llm_entry

from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser
from fiber.infra.vcr.manager import VCRPlaybackConfig
from fiber.infra.vcr.injector import VCRInjector

from xphi.node.space.bind.resolver import find_current_self, resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("action.compat")

COMPAT_SAMPLE_TOOLS = [
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

STANDARD_PROMPTS = [
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


def _parse_model_list(model_arg: str) -> list[str]:
    items = [m.strip() for m in (model_arg or "").split(",") if m.strip()]
    if len(items) == 1 and items[0].lower() == "all":
        try:
            from fiber.dev.console.tab.vcr import discover_compat_models
            discovered = discover_compat_models()
            if discovered:
                return discovered
        except Exception:
            pass
    return items


def _resolve_default_compats_base() -> Path:
    """환경변수, 로컬 개발 레포지토리(fiber-compats), 또는 콘솔 다운로드 경로(fixture)를 순차 탐색합니다."""
    if env_dir := os.getenv("FIBER_COMPATS_DIR"):
        return Path(env_dir).resolve()

    self_dir = find_current_self()
    if self_dir:
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


def _resolve_target_dir(model: str, target_dir: Optional[str], is_multi: bool = False) -> tuple[str, str, str]:
    _, resolved_provider, dynamic_api_key, _ = get_llm_provider(model=model)
    safe_model_name = model.replace("/", "_").replace(":", "_")
    if target_dir:
        final_dir = os.path.join(target_dir, resolved_provider, safe_model_name) if is_multi else target_dir
    else:
        compats_base = _resolve_default_compats_base()
        final_dir = str(compats_base / "adapters" / resolved_provider / safe_model_name)
    return final_dir, resolved_provider, dynamic_api_key


def _format_tool_calls(tool_calls: Optional[list[dict[str, Any]]]) -> str:
    """StateMapper.extract_tool_calls가 반환한 정규화된 tool_calls 리스트를 검증 출력용 문자열로 포맷팅합니다."""
    if not tool_calls:
        return ""
    formatted = []
    for tc in tool_calls:
        fn = tc.get("function") if isinstance(tc, dict) else None
        if isinstance(fn, dict) and fn.get("name"):
            formatted.append(f"[TOOL_CALL] {fn['name']}({fn.get('arguments', '{}')})")
    return "\n".join(formatted)


def _is_unsupported_capability_error(exc: Exception) -> bool:
    """모델/프로바이더가 특정 기능(예: Function calling)을 지원하지 않는 정상 스펙 제약인지 판별합니다."""
    cls_name = type(exc).__name__
    msg = str(exc).lower()
    return (
        "Unsupported" in cls_name
        or "unsupported by" in msg
        or "does not support tool" in msg
        or "function calling unsupported" in msg
    )


def _init_vcr(
    mode: str,
    target_dir: str,
    *,
    vcr_speed: str = "max",
    vcr_chaos: float = 0.0,
    vcr_tick: float = 100.0,
    vcr_targets: str = "",
    include_raw: bool = False,
):
    os.environ["VCR_MODE"] = mode
    if include_raw:
        os.environ["VCR_INCLUDE_RAW"] = "true"

    config = VCRPlaybackConfig(
        mode=mode,
        speed=vcr_speed,
        chaos_latency_ms=vcr_chaos,
        target_traces=vcr_targets,
        record_tick_ms=vcr_tick,
        include_raw_payload=include_raw,
    )
    VCRInjector.apply(config=config, fixture_dir=target_dir)
    log.info(
        f"[Compat: VCR] Injector applied -> mode={mode}, speed={vcr_speed}, "
        f"chaos={vcr_chaos:.1f}ms, tick={vcr_tick:.1f}ms, dir={target_dir}"
    )


async def _execute_suite(
    model: str,
    final_api_key: Optional[str],
    invoker_mode: str,
    provider: str,
    target_dir: str,
) -> tuple[int, int, int]:
    """Returns (success_count, executed_count, skipped_unsupported_count)."""
    success_count = 0
    executed_count = 0
    skipped_count = 0

    for test in STANDARD_PROMPTS:
        scenario_id = test["scenario"].replace(" ", "_").lower()
        fixture_filename = f"{scenario_id}.json"
        fixture_path = Path(target_dir) / fixture_filename
        is_stream = test["stream"]
        tools = test.get("tools")

        # Replay 모드에서 미지원 모델 등으로 녹화되지 않은 선택적(Tool Call) 픽스처는 안전하게 스킵
        if invoker_mode == "test_fixture" and tools and not fixture_path.exists():
            skipped_count += 1
            log.info(
                f"⚪ [Compat: SKIPPED] '{fixture_filename}' not present for {model} "
                f"(Tool calling unsupported or not recorded)."
            )
            continue

        executed_count += 1

        print(f"\n{'='*80}")
        print(f"[COMPAT: {invoker_mode.upper()}] Executing Test: {test['scenario']}")
        print(f"   ├─ Model    : {model}")
        print(f"   ├─ Provider : {provider} (Rule targeting)")
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
                model=model,
                messages=test["messages"],
                stream=is_stream,
                metadata={
                    "vcr_scenario": scenario_id,
                    "vcr_invoker": f"action.compat.{invoker_mode}",
                    "vcr_filename": fixture_filename,
                },
                **kwargs,
            )

            full_content = ""
            usage_info = "N/A"
            chunk_count = 0

            print("[NETWORK] Connection established. Receiving payload...\n")
            print("[AI RESPONSE]")
            print("-" * 40)

            if is_stream:
                async for chunk in response:
                    chunk_count += 1
                    parsed_chunk = StreamChunkParser.parse(provider, chunk)
                    if not parsed_chunk:
                        continue

                    text = parsed_chunk.get("text", "")
                    if text:
                        full_content += text
                        sys.stdout.write(text)
                        sys.stdout.flush()

                    chunk_usage = parsed_chunk.get("usage")
                    if chunk_usage:
                        has_real_value = (
                            any(v is not None for v in chunk_usage.values())
                            if isinstance(chunk_usage, dict)
                            else True
                        )
                        if has_real_value:
                            usage_info = str(chunk_usage)
            else:
                content, usage_dict = StateMapper.extract_response(response, provider)
                if tools:
                    # 우회 로직 없이 StateMapper.extract_tool_calls로 파이프라인 응답 객체에서 직접 추출 검증
                    extracted_tools = StateMapper.extract_tool_calls(response, provider)
                    full_content = _format_tool_calls(extracted_tools)
                else:
                    full_content = content or ""

                if usage_dict:
                    usage_info = str(usage_dict)

                if full_content:
                    sys.stdout.write(full_content)
                    sys.stdout.flush()

            duration = (time.perf_counter() - start_time) * 1000
            print("\n\n" + "-" * 40)
            print("[VALIDATION PHASE]")
            print(f"full_content = {full_content}")
            if not full_content.strip():
                target_kind = "tool_calls" if tools else "content"
                raise ValueError(
                    f"Extracted {target_kind} is empty! Missing or invalid extraction rule for provider: '{provider}'"
                )

            print(f"   ├─ Extracted Output  : OK ({len(full_content)} chars)")
            print(f"   ├─ Extracted Usage   : {usage_info}")
            if is_stream:
                print(f"   └─ Total Chunks      : {chunk_count}")

            log.info(f"[Compat: {invoker_mode.upper()}] Scenario Passed! ({duration:.2f}ms)")
            success_count += 1

        except Exception as e:
            print("\n" + "-" * 40)
            if _is_unsupported_capability_error(e):
                executed_count -= 1
                skipped_count += 1
                fixture_path.unlink(missing_ok=True)
                log.info(f"⚪ [Compat: UNSUPPORTED] Skipped '{test['scenario']}' -> {e}")
            else:
                log.error(f"[Compat: {invoker_mode.upper()}] Failed: {type(e).__name__} - {e}")

    return success_count, executed_count, skipped_count


async def run_gen_fixture(
    model: str,
    api_key: Optional[str] = None,
    target_dir: Optional[str] = None,
    vcr_tick: float = 100.0,
):
    models = _parse_model_list(model)
    is_multi = len(models) > 1
    LOCAL_PROVIDERS = ["lm_studio", "llama_server", "ollama", "vllm"]
    summary: list[tuple[str, int, int, int, str]] = []

    for m in models:
        log.info(f"[Compat: GEN-FIXTURE] Resolving configuration for {m}...")
        try:
            resolved_dir, resolved_provider, dynamic_api_key = _resolve_target_dir(m, target_dir, is_multi=is_multi)
        except Exception as e:
            log.error(f"[Compat: GEN-FIXTURE] Invalid model identifier '{m}': {e}")
            if not is_multi:
                sys.exit(1)
            summary.append((m, 0, len(STANDARD_PROMPTS), 0, "INVALID_MODEL"))
            continue

        if api_key or dynamic_api_key:
            final_api_key = api_key or dynamic_api_key
        elif resolved_provider in LOCAL_PROVIDERS:
            final_api_key = "sk-mock-local-key"
        else:
            final_api_key = None

        os.makedirs(resolved_dir, exist_ok=True)
        _init_vcr(mode="record", target_dir=resolved_dir, vcr_tick=vcr_tick, include_raw=True)
        try:
            passed, total, skipped = await _execute_suite(
                m, final_api_key, "gen_fixture", resolved_provider, resolved_dir
            )
            summary.append((m, passed, total, skipped, resolved_dir))
            skip_note = f", {skipped} unsupported skipped" if skipped else ""
            print(f"\n{'='*80}")
            log.info(
                f"[COMPAT: FIXTURE GENERATION COMPLETE] [{m}] ({passed}/{total} passed{skip_note}) saved at: {resolved_dir}"
            )
        finally:
            VCRInjector.remove()

    if is_multi:
        print(f"\n{'='*80}")
        log.info(f"[COMPAT: BATCH GENERATION SUMMARY] ({len(models)} models processed)")
        for m, passed, total, skipped, loc in summary:
            skip_str = f" (⚪ {skipped} unsupported)" if skipped else ""
            log.info(f"  • {m:<42} : {passed}/{total} scenarios{skip_str} -> {loc}")


async def run_test_fixture(
    model: str,
    target_dir: Optional[str] = None,
    vcr_speed: str = "max",
    vcr_chaos: float = 0.0,
    vcr_targets: str = "",
):
    models = _parse_model_list(model)
    is_multi = len(models) > 1
    all_passed = True
    summary: list[tuple[str, int, int, int]] = []

    for m in models:
        log.info(
            f"[Compat: TEST-FIXTURE] Preparing Replay environment for {m} "
            f"(--vcr-speed {vcr_speed} --vcr-chaos {vcr_chaos})..."
        )
        try:
            resolved_dir, resolved_provider, _ = _resolve_target_dir(m, target_dir, is_multi=is_multi)
        except Exception as e:
            log.error(f"[Compat: TEST-FIXTURE] Invalid model identifier '{m}': {e}")
            all_passed = False
            if not is_multi:
                sys.exit(1)
            continue

        if not os.path.exists(resolved_dir):
            log.error(f"[Compat: TEST-FIXTURE] Fixture directory not found: {resolved_dir}")
            log.error(f"Run 'fiber compat fixture -m {m} --vcr record' first to generate required test data.")
            all_passed = False
            if not is_multi:
                sys.exit(1)
            summary.append((m, 0, len(STANDARD_PROMPTS), 0))
            continue

        _init_vcr(
            mode="replay",
            target_dir=resolved_dir,
            vcr_speed=vcr_speed,
            vcr_chaos=vcr_chaos,
            vcr_targets=vcr_targets,
            include_raw=False,
        )
        try:
            mock_api_key = "sk-vcr-replay-mock-key"
            passed, total, skipped = await _execute_suite(
                m, mock_api_key, "test_fixture", resolved_provider, resolved_dir
            )
            summary.append((m, passed, total, skipped))
            skip_note = f" ({skipped} unsupported skipped)" if skipped else ""
            print(f"\n{'='*80}")
            if passed == total and total > 0:
                log.info(f"[COMPAT: TEST SUCCESS] All ({passed}/{total}) supported rules verified for {m}{skip_note}.")
            else:
                all_passed = False
                log.error(
                    f"[COMPAT: TEST FAILED] ({passed}/{total}) scenarios passed for {m}{skip_note}. Review extraction rules."
                )
        finally:
            VCRInjector.remove()

    if is_multi:
        print(f"\n{'='*80}")
        log.info(f"[COMPAT: BATCH REPLAY SUMMARY] ({len(summary)} models tested)")
        for m, passed, total, skipped in summary:
            status_icon = "✅" if (passed == total and total > 0) else "❌"
            skip_str = f" (⚪ {skipped} unsupported)" if skipped else ""
            log.info(f"  {status_icon} {m:<42} : {passed}/{total} passed{skip_str}")

    if not all_passed:
        sys.exit(1)