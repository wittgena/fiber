# fiber.infra.vcr.proxy
import asyncio
import copy
import json
import re
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

from fiber.dev.console.bridge.signal import UIKey, UIMetric
from fiber.infra.vcr.manager import VCRManager, VCRPlaybackConfig
import fiber.llm.exception.eco as eco_exceptions
from fiber.llm.exception.mapping import SEMANTIC_ERROR_REGEX, STATUS_CODE_MAPPING
from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage
from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser

from xphi.arch.contract.event.next import next_trace_id
from xphi.arch.model.surge.model import DynamicSurgeModel, _melt_alien_objects
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("vcr.proxy", phase="LLM_INTER")


def _sanitize_for_json(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [_sanitize_for_json(v) for v in obj]
    elif isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return str(obj)
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)


def _estimate_saved_metrics(usage_data: Optional[Dict[str, Any]], chunk_count: int) -> tuple[int, float]:
    """리플레이된 픽스처의 usage 또는 청크 수로부터 절약된 토큰 수와 예상 비용($)을 산출"""
    tokens = 0
    if isinstance(usage_data, dict):
        tokens = int(
            usage_data.get("total_tokens")
            or (int(usage_data.get("prompt_tokens") or 0) + int(usage_data.get("completion_tokens") or 0))
        )
    if tokens <= 0:
        tokens = max(32, chunk_count * 12)
    saved_usd = round(tokens * 0.000015, 5)
    return tokens, saved_usd

class VCRRawPayloadState(DynamicSurgeModel):
    sync_response: Optional[Any] = None
    stream_chunks: List[Any] = []

def _reconstruct_vcr_exception(exc_data: dict, provider: str, model: str) -> Exception:
    error_type = exc_data.get("error_type", "Exception")
    message = exc_data.get("message", "")
    replay_msg = f"[VCR Replay] {message}"

    status_code = None
    match = re.search(r"\b([45]\d{2})\b", message)
    if match:
        status_code = int(match.group(1))

    for pattern, exception_class in SEMANTIC_ERROR_REGEX:
        if pattern.search(message):
            try:
                return exception_class(message=replay_msg, model=model, llm_provider=provider)
            except Exception:
                pass

    if status_code and status_code in STATUS_CODE_MAPPING:
        exception_class = STATUS_CODE_MAPPING[status_code]
        try:
            return exception_class(message=replay_msg, model=model, llm_provider=provider)
        except Exception:
            pass

    if hasattr(eco_exceptions, error_type):
        exception_class = getattr(eco_exceptions, error_type)
        try:
            return exception_class(message=replay_msg, llm_provider=provider, model=model)
        except Exception:
            pass

    error_class = (
        __builtins__.get(error_type, Exception)
        if isinstance(__builtins__, dict)
        else getattr(__builtins__, error_type, Exception)
    )
    return error_class(replay_msg)


async def stream_recorder_proxy(
    original_stream: AsyncGenerator,
    fixture_data: dict,
    manager: VCRManager,
    trace_id: str,
    start_time: float,
    ctx: Any,
    config: Optional[VCRPlaybackConfig] = None,
) -> AsyncGenerator:
    eff_config = config or manager.resolve_config(ctx)
    last_time = start_time
    is_first = True

    tick_ms = eff_config.record_tick_ms
    buffer_text = ""
    buffer_time = 0.0

    provider = getattr(ctx, "custom_llm_provider", None)
    if not provider:
        model_name = getattr(ctx, "model", "")
        provider = model_name.split("/")[0] if "/" in model_name else "openai"

    include_raw = getattr(eff_config, "include_raw_payload", False)
    payload_state = VCRRawPayloadState(stream_chunks=[]) if include_raw else None

    def flush_buffer(f_reason: Optional[str] = None):
        nonlocal buffer_text, buffer_time
        if buffer_text or f_reason:
            payload = {
                "delta_ms": buffer_time,
                "chunk": buffer_text,
            }
            if f_reason:
                payload["finish_reason"] = f_reason
            fixture_data["response_timeline"].append(payload)

        buffer_text = ""
        buffer_time = 0.0

    try:
        async for chunk in original_stream:
            if payload_state is not None:
                try:
                    payload_state.stream_chunks.append(_melt_alien_objects(chunk))
                except Exception:
                    pass

            current_time = time.perf_counter()
            delta_ms = (current_time - last_time) * 1000
            last_time = current_time

            parsed_chunk = StreamChunkParser.parse(provider, chunk)
            content = parsed_chunk.get("text", "") if parsed_chunk else ""
            chunk_usage = parsed_chunk.get("usage") if parsed_chunk else None
            finish_reason = parsed_chunk.get("finish_reason") if parsed_chunk else None

            if chunk_usage:
                has_real_value = (
                    any(v is not None for v in chunk_usage.values())
                    if isinstance(chunk_usage, dict)
                    else True
                )
                if has_real_value:
                    fixture_data["usage"] = chunk_usage

            if is_first:
                fixture_data["network_metrics"]["ttfb_ms"] = (current_time - start_time) * 1000
                is_first = False
                if content or finish_reason:
                    payload = {"delta_ms": delta_ms, "chunk": content}
                    if finish_reason:
                        payload["finish_reason"] = finish_reason
                    fixture_data["response_timeline"].append(payload)
                yield chunk
                continue

            buffer_text += content
            buffer_time += delta_ms

            if finish_reason:
                flush_buffer(f_reason=finish_reason)
            elif tick_ms <= 0 or buffer_time >= tick_ms:
                flush_buffer()

            yield chunk

    except Exception as e:
        fixture_data["exception_boundary"] = {
            "occurred": True,
            "error_type": type(e).__name__,
            "message": str(e),
        }
        log.warning(
            f"[VCR:Record] Stream exception captured into fixture '{trace_id[:8]}': {type(e).__name__}: {e}",
            trace_id=trace_id,
        )
        raise
    finally:
        flush_buffer()
        total_ms = round((time.perf_counter() - start_time) * 1000, 2)
        fixture_data["network_metrics"]["total_duration_ms"] = total_ms
        chunk_count = len(fixture_data.get("response_timeline", []))

        if payload_state is not None:
            try:
                fixture_data["raw_payload"] = payload_state.model_dump(exclude_unset=True)
            except Exception:
                pass
        manager.save_fixture(trace_id, _sanitize_for_json(fixture_data), ctx)

        log.info(
            f"[VCR:Record] Saved stream fixture '{trace_id[:8]}' ({chunk_count} coalesced chunks, {total_ms:.1f}ms live I/O).",
            **{
                UIKey.METRIC.value: UIMetric.VCR_LIVE.value,
                UIKey.ELAPSED_MS.value: total_ms,
                UIKey.CHUNKS.value: chunk_count,
                "trace_id": trace_id,
            },
        )


async def stream_player_emulator(
    fixture_data: dict,
    config: VCRPlaybackConfig,
    model_name: str,
) -> AsyncGenerator:
    t0 = time.perf_counter()
    metrics = fixture_data.get("network_metrics", {})
    timeline = fixture_data.get("response_timeline", [])
    trace_short = str(fixture_data.get("trace_id", "mock"))[:8]

    if config.chaos_latency_ms > 0:
        await asyncio.sleep(config.chaos_latency_ms / 1000.0)

    if config.speed == "real" and metrics.get("ttfb_ms", 0) > 0:
        await asyncio.sleep(metrics["ttfb_ms"] / 1000.0)

    for item in timeline:
        if config.speed == "real" and item.get("delta_ms", 0) > 0:
            await asyncio.sleep(item["delta_ms"] / 1000.0)

        yield ModelResponse(
            id=f"vcr-{trace_short}",
            model=model_name,
            choices=[
                {
                    "index": 0,
                    "delta": {"content": item.get("chunk", "")},
                    "finish_reason": item.get("finish_reason"),
                }
            ],
        )

    usage_data = fixture_data.get("usage")
    if usage_data:
        safe_usage = {k: (v if v is not None else 0) for k, v in usage_data.items()}
        yield ModelResponse(
            id=f"vcr-{trace_short}",
            model=model_name,
            choices=[],
            usage=Usage(**safe_usage),
        )

    elapsed_ms = max(0.1, round((time.perf_counter() - t0) * 1000, 2))
    chunk_count = len(timeline)
    saved_tok, saved_usd = _estimate_saved_metrics(usage_data, chunk_count)

    log.info(
        f"[VCR:Replay] Replayed stream fixture '{trace_short}' ({chunk_count} chunks in {elapsed_ms:.2f}ms, 0ms upstream I/O).",
        **{
            UIKey.METRIC.value: UIMetric.VCR_REPLAY.value,
            UIKey.ELAPSED_MS.value: elapsed_ms,
            UIKey.CHUNKS.value: chunk_count,
            UIKey.SAVED_TOKENS.value: saved_tok,
            UIKey.SAVED_USD.value: saved_usd,
        },
    )

    exc = fixture_data.get("exception_boundary", {})
    if exc.get("occurred"):
        provider = fixture_data.get("request_context", {}).get("provider", "unknown")
        log.warning(
            f"[VCR:Replay] Reconstructing recorded exception '{exc.get('error_type')}' for '{trace_short}'.",
            trace_id=trace_short,
        )
        raise _reconstruct_vcr_exception(exc, provider, model_name)


class VCRAdapterProxy:
    def __init__(self, original_adapter: Any, manager: VCRManager):
        self.original_adapter = original_adapter
        self.manager = manager
        self.config = manager.config

    async def execute(self, ctx: Any) -> Any:
        # 1. Resolve per-request VCR configuration (checks ctx.system_meta.metadata first)
        eff_config = self.manager.resolve_config(ctx)

        # 2. Fast-path for LIVE mode: bypass deepcopy and fixture overhead, still report live latency
        if eff_config.mode not in ("record", "replay"):
            t_live = time.perf_counter()
            res = await self.original_adapter.execute(ctx)
            if not getattr(ctx, "stream", False):
                live_ms = round((time.perf_counter() - t_live) * 1000, 2)
                log.info(
                    f"[VCR:Live] Upstream LLM call completed in {live_ms:.1f}ms.",
                    **{
                        UIKey.METRIC.value: UIMetric.VCR_LIVE.value,
                        UIKey.ELAPSED_MS.value: live_ms,
                    },
                )
            return res

        safe_ctx = copy.deepcopy(ctx)

        system_meta = getattr(safe_ctx, "system_meta", None)
        trace_id = getattr(system_meta, "trace_id", None)
        if not trace_id:
            trace_id = next_trace_id()

        is_stream = getattr(safe_ctx, "stream", False)
        model_name = getattr(safe_ctx, "model", "vcr-mock-model")

        # ==========================================
        # Mode A: Deterministic Offline Replay
        # ==========================================
        if eff_config.mode == "replay":
            t_replay = time.perf_counter()
            fixture = self.manager.get_fixture(trace_id, safe_ctx)
            if not fixture:
                log.error(f"[VCR:Replay] Fixture miss for trace_id='{trace_id}'.", trace_id=trace_id)
                raise ValueError(f"No VCR fixture found for trace_id: '{trace_id}'")

            vcr_latency = fixture["network_metrics"].get("total_duration_ms", 0)
            if system_meta and hasattr(system_meta, "metadata") and isinstance(system_meta.metadata, dict):
                system_meta.metadata["_vcr_injected_latency_ms"] = vcr_latency + eff_config.chaos_latency_ms

            if not is_stream and fixture["exception_boundary"]["occurred"]:
                if eff_config.chaos_latency_ms > 0:
                    await asyncio.sleep(eff_config.chaos_latency_ms / 1000.0)

                provider = getattr(safe_ctx, "custom_llm_provider", None)
                if not provider:
                    provider = model_name.split("/")[0] if "/" in model_name else "openai"
                log.warning(
                    f"[VCR:Replay] Replaying recorded exception '{fixture['exception_boundary'].get('error_type')}' ({trace_id[:8]}).",
                    trace_id=trace_id,
                )
                raise _reconstruct_vcr_exception(fixture["exception_boundary"], provider, model_name)

            if is_stream:
                return stream_player_emulator(fixture, eff_config, model_name)

            if eff_config.chaos_latency_ms > 0:
                await asyncio.sleep(eff_config.chaos_latency_ms / 1000.0)
            if eff_config.speed == "real":
                await asyncio.sleep(vcr_latency / 1000.0)

            timeline = fixture.get("response_timeline", [])
            content = "".join([t.get("chunk", "") for t in timeline])
            usage_data = fixture.get("usage")
            if usage_data:
                safe_usage = {k: (v if v is not None else 0) for k, v in usage_data.items()}
                usage_obj = Usage(**safe_usage)
            else:
                usage_obj = None

            elapsed_ms = max(0.1, round((time.perf_counter() - t_replay) * 1000, 2))
            saved_tok, saved_usd = _estimate_saved_metrics(usage_data, len(timeline))

            log.info(
                f"[VCR:Replay] Replayed sync fixture '{trace_id[:8]}' in {elapsed_ms:.2f}ms (original live: {vcr_latency:.1f}ms).",
                **{
                    UIKey.METRIC.value: UIMetric.VCR_REPLAY.value,
                    UIKey.ELAPSED_MS.value: elapsed_ms,
                    UIKey.CHUNKS.value: len(timeline),
                    UIKey.SAVED_TOKENS.value: saved_tok,
                    UIKey.SAVED_USD.value: saved_usd,
                },
            )

            return ModelResponse(
                id=f"vcr-{trace_id[:8]}",
                model=model_name,
                choices=[{"index": 0, "message": {"role": "assistant", "content": content}}],
                usage=usage_obj,
            )

        # ==========================================
        # Mode B: Live Capture & Fixture Recording
        # ==========================================
        start_time = time.perf_counter()
        fixture_data = self.manager.create_empty_fixture(trace_id, safe_ctx)
        try:
            response = await self.original_adapter.execute(safe_ctx)

            if eff_config.mode == "record":
                if is_stream:
                    return stream_recorder_proxy(
                        response, fixture_data, self.manager, trace_id, start_time, safe_ctx, config=eff_config
                    )

                duration = round((time.perf_counter() - start_time) * 1000, 2)
                provider = getattr(safe_ctx, "custom_llm_provider", None)
                if not provider:
                    model_name = getattr(safe_ctx, "model", "")
                    provider = model_name.split("/")[0] if "/" in model_name else "openai"

                content, usage_dict = StateMapper.extract_response(response, provider)
                if getattr(eff_config, "include_raw_payload", False):
                    try:
                        raw_data = getattr(response, "raw", response)
                        if isinstance(raw_data, bytes):
                            try:
                                raw_data = json.loads(raw_data.decode("utf-8"))
                            except Exception:
                                raw_data = raw_data.decode("utf-8", errors="ignore")

                        safe_raw_data = _melt_alien_objects(raw_data)
                        payload_state = VCRRawPayloadState(sync_response=safe_raw_data)
                        fixture_data["raw_payload"] = payload_state.model_dump(exclude_unset=True)
                    except Exception:
                        pass

                if usage_dict:
                    fixture_data["usage"] = usage_dict
                    if hasattr(response, "usage"):
                        for k, v in usage_dict.items():
                            if hasattr(response.usage, k):
                                setattr(response.usage, k, v)

                fixture_data["network_metrics"]["ttfb_ms"] = duration
                fixture_data["network_metrics"]["total_duration_ms"] = duration
                fixture_data["response_timeline"].append({"delta_ms": 0, "chunk": content})
                self.manager.save_fixture(trace_id, _sanitize_for_json(fixture_data), safe_ctx)

                if system_meta and hasattr(system_meta, "metadata") and isinstance(system_meta.metadata, dict):
                    system_meta.metadata["_vcr_injected_latency_ms"] = duration

                log.info(
                    f"[VCR:Record] Recorded sync fixture '{trace_id[:8]}' ({duration:.1f}ms live I/O).",
                    **{
                        UIKey.METRIC.value: UIMetric.VCR_LIVE.value,
                        UIKey.ELAPSED_MS.value: duration,
                        UIKey.CHUNKS.value: 1,
                        "trace_id": trace_id,
                    },
                )
            return response
        except Exception as e:
            if eff_config.mode == "record":
                duration = round((time.perf_counter() - start_time) * 1000, 2)
                fixture_data["network_metrics"]["total_duration_ms"] = duration
                fixture_data["exception_boundary"] = {
                    "occurred": True,
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
                self.manager.save_fixture(trace_id, _sanitize_for_json(fixture_data), safe_ctx)
                log.warning(
                    f"[VCR:Record] Recorded upstream exception '{type(e).__name__}' into fixture '{trace_id[:8]}' ({duration:.1f}ms).",
                    **{
                        UIKey.METRIC.value: UIMetric.VCR_LIVE.value,
                        UIKey.ELAPSED_MS.value: duration,
                        "trace_id": trace_id,
                    },
                )
            raise