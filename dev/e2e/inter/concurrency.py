# fiber.dev.e2e.inter.concurrency
"""
Validates core ChannelPipeline concurrency, context isolation, and stream interleaving determinism.
Integrates PipelineLoadProfiler (fiber.dev.trace.profile.metrics) to report k6-style latency percentiles
(p50/p90/p95/p99), TTFT, RPS/CPS throughput, memory peak, and enforce strict SLO regression gates.
"""
from __future__ import annotations

import os
import socket
import asyncio
from pathlib import Path
from typing import Any, Dict, List

os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from fiber.infra.e2e.config import Phase, E2EConfig
from fiber.infra.e2e.pipeline import BaseBridgePipeline, log

from fiber.llm.entry import acompletion
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.dev.trace.profile.metrics import PipelineLoadProfiler, LoadSLO
from fiber.dev.trace.vcr.manager import (
    VCRPlaybackConfig,
    VCRIdentityRule,
    add_vcr_arguments
)
from fiber.dev.trace.vcr.injector import VCRInjector
from fiber.dev.trace.llm.base import create_e2e_parser, build_e2e_context

from xphi.arch.bound.event.next import next_trace_id
from xphi.node.space.bind.resolver import get_invoker
from xphi.state.phase.reactor import PhaseReactor

FIXTURE_ROOT = "fiber-compats/suites/e2e"
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

CONCURRENCY_FANOUT = 50

UNARY_PROMPT = (
    "Summarize the core architectural benefits of a deterministic, event-driven "
    "duplex pipeline over nested callback chains in exactly two concise paragraphs."
)

STREAM_PROMPT = (
    "Write a detailed 4-part technical walkthrough of how asynchronous byte-stream "
    "framing, backpressure watermarks, and coroutine context propagation operate "
    "under high concurrency."
)


def _get_free_port(default_port: int = 8355) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("127.0.0.1", default_port)) != 0:
            return default_port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class PipelineConcurrencySuite(BaseBridgePipeline):
    def __init__(
        self,
        config: E2EConfig,
        vcr_config: VCRPlaybackConfig,
        vcr_manager: Any,
        target_model: str,
        concurrency: int = CONCURRENCY_FANOUT,
        iterations: int = 0
    ):
        super().__init__(
            config=config,
            name="Core Pipeline VCR Concurrency & Load Profile Suite",
            scope_name="PIPELINE_CONCURRENCY_SUITE"
        )
        self.vcr_config = vcr_config
        self.vcr_manager = vcr_manager
        self.target_model = target_model
        self.concurrency = concurrency
        self.iterations = iterations if iterations > 0 else concurrency

        self.set_phases([
            Phase("Phase 1: Concurrent Unary Burst & Context Isolation", self.phase_unary_concurrency),
            Phase("Phase 2: Concurrent Stream Interleaving & Throughput Profile", self.phase_stream_concurrency),
            Phase("Phase 3: Heterogeneous Sustained VU Pool (Unary + Stream + Aggregated)", self.phase_mixed_workload),
        ])

    @property
    def is_record(self) -> bool:
        return self.vcr_config.mode == "record"

    def _build_shared_trace_context(self, scenario_name: str, messages: list) -> str:
        seed = VCRIdentityRule.generate_seed(
            scenario_name=scenario_name,
            messages=messages,
            invoker=CURRENT_INVOKER
        )
        return next_trace_id(seed) if self.vcr_config.mode in ("record", "replay") else next_trace_id()

    @staticmethod
    def _extract_unary_content(response: Any) -> str:
        content = StateTraverser.resolve(response, "choices.0.message.content")
        if content:
            return str(content)
        reasoning = StateTraverser.resolve(response, "choices.0.message.reasoning_content")
        if reasoning:
            return str(reasoning)
        text_fallback = StateTraverser.resolve(response, "choices.0.text")
        if text_fallback:
            return str(text_fallback)
        if hasattr(response, "model_dump"):
            dump = response.model_dump()
            choices = dump.get("choices", [])
            if choices and isinstance(choices[0], dict):
                msg = choices[0].get("message", {})
                if isinstance(msg, dict):
                    return msg.get("content") or msg.get("reasoning_content") or ""
        return ""

    # =========================================================================
    # Phase 1: 단일 응답(Unary) 동시성 부하 계측 및 ContextBinder 격리 검증
    # =========================================================================
    async def phase_unary_concurrency(self):
        scenario = "concurrency_phase_1_unary"
        messages = [{"role": "user", "content": UNARY_PROMPT}]
        shared_trace_id = self._build_shared_trace_context(scenario, messages)

        vu_count = 1 if self.is_record else self.concurrency
        total_iters = 1 if self.is_record else self.iterations

        log.info(f"  └─ Executing {total_iters} unary requests across {vu_count} VUs (Mode: {self.vcr_config.mode})...")

        profiler = PipelineLoadProfiler("Phase 1 - Unary Pipeline Burst")

        async def _unary_worker(worker_idx: int) -> Dict[str, Any]:
            expected_session = f"unary_sess_{worker_idx:04d}"
            with profiler.track_request(worker_idx=worker_idx, mode="unary") as sw:
                res = await acompletion(
                    model=self.target_model,
                    messages=messages,
                    max_tokens=512,
                    stream=False,
                    trace_id=shared_trace_id,
                    session_id=expected_session,
                    metadata={
                        "vcr_scenario": scenario,
                        "vcr_invoker": CURRENT_INVOKER,
                        "worker_idx": worker_idx
                    }
                )
                content = self._extract_unary_content(res)
                sw.add_chunk(len(content))

            sys_meta = getattr(res, "system_meta", None)
            return {
                "worker_idx": worker_idx,
                "expected_session": expected_session,
                "actual_session": getattr(sys_meta, "session_id", None),
                "actual_trace_id": getattr(sys_meta, "trace_id", None),
                "call_id": getattr(sys_meta, "call_id", None),
                "content": content,
            }

        async with profiler:
            results = await profiler.run_vu_pool(
                total_iterations=total_iters,
                concurrency=vu_count,
                worker_fn=_unary_worker
            )

        baseline = results[0]
        if not baseline["content"]:
            raise RuntimeError("Phase 1 Failed: Baseline unary response returned empty content.")

        call_ids = set()
        for r in results:
            if r["actual_session"] != r["expected_session"]:
                raise RuntimeError(
                    f"Context Cross-Talk in Worker #{r['worker_idx']}: "
                    f"expected '{r['expected_session']}', got '{r['actual_session']}'."
                )
            if r["actual_trace_id"] != shared_trace_id:
                raise RuntimeError(f"Trace ID Mismatch in Worker #{r['worker_idx']}.")
            if r["content"] != baseline["content"]:
                raise RuntimeError(f"Payload Determinism Broken in Worker #{r['worker_idx']}.")
            if r["call_id"]:
                call_ids.add(r["call_id"])

        if len(call_ids) != total_iters:
            raise RuntimeError(f"Span ID Collision: expected {total_iters} unique IDs, got {len(call_ids)}.")

        # Replay 모드에서는 엄격한 SLO(에러율 0%, 메모리 피크 50MB 이하) 검증
        slo = LoadSLO(max_error_rate_pct=0.0, max_memory_peak_mb=50.0)
        profiler.assert_slo(slo)

    # =========================================================================
    # Phase 2: 비동기 스트림(Stream) 인터리빙, TTFT 및 CPS(Chunks/sec) 계측
    # =========================================================================
    async def phase_stream_concurrency(self):
        scenario = "concurrency_phase_2_stream"
        messages = [{"role": "user", "content": STREAM_PROMPT}]
        shared_trace_id = self._build_shared_trace_context(scenario, messages)

        vu_count = 1 if self.is_record else self.concurrency
        total_iters = 1 if self.is_record else self.iterations

        log.info(f"  └─ Launching {total_iters} interleaved streams across {vu_count} VUs (Mode: {self.vcr_config.mode})...")

        profiler = PipelineLoadProfiler("Phase 2 - Interleaved Stream Fan-Out")

        async def _stream_worker(worker_idx: int) -> Dict[str, Any]:
            expected_session = f"stream_sess_{worker_idx:04d}"
            pieces: List[str] = []

            with profiler.track_request(worker_idx=worker_idx, mode="stream") as sw:
                response_stream = await acompletion(
                    model=self.target_model,
                    messages=messages,
                    max_tokens=1024,
                    stream=True,
                    trace_id=shared_trace_id,
                    session_id=expected_session,
                    metadata={
                        "vcr_scenario": scenario,
                        "vcr_invoker": CURRENT_INVOKER,
                        "worker_idx": worker_idx
                    }
                )

                async for chunk in response_stream:
                    delta = StateTraverser.resolve(chunk, "choices.0.delta.content")
                    text_piece = str(delta) if delta else ""
                    if text_piece:
                        pieces.append(text_piece)
                    sw.add_chunk(len(text_piece))

                    if vu_count > 1 and (sw.chunks + worker_idx) % 2 == 0:
                        await asyncio.sleep(0)

            sys_meta = getattr(response_stream, "system_meta", None)
            return {
                "worker_idx": worker_idx,
                "expected_session": expected_session,
                "actual_session": getattr(sys_meta, "session_id", None),
                "call_id": getattr(sys_meta, "call_id", None),
                "chunks": sw.chunks,
                "text": "".join(pieces),
            }

        async with profiler:
            results = await profiler.run_vu_pool(
                total_iterations=total_iters,
                concurrency=vu_count,
                worker_fn=_stream_worker
            )

        baseline = results[0]
        if baseline["chunks"] == 0 or not baseline["text"]:
            raise RuntimeError("Phase 2 Failed: Baseline stream produced 0 chunks or empty text.")

        call_ids = set()
        for r in results:
            if r["actual_session"] != r["expected_session"]:
                raise RuntimeError(f"Stream Context Cross-Talk in Worker #{r['worker_idx']}.")
            if r["chunks"] != baseline["chunks"] or r["text"] != baseline["text"]:
                raise RuntimeError(
                    f"Stream Interleaving Corruption in Worker #{r['worker_idx']}: "
                    f"expected {baseline['chunks']} chunks, got {r['chunks']}."
                )
            if r["call_id"]:
                call_ids.add(r["call_id"])

        if len(call_ids) != total_iters:
            raise RuntimeError(f"Stream Span ID Collision: expected {total_iters} unique IDs, got {len(call_ids)}.")

        slo = LoadSLO(max_error_rate_pct=0.0, max_memory_peak_mb=64.0)
        profiler.assert_slo(slo)

    # =========================================================================
    # Phase 3: 이기종 혼합 워크로드 (Unary + Raw Stream + Aggregated Stream)
    # =========================================================================
    async def phase_mixed_workload(self):
        if self.is_record:
            log.info("  └─ Record mode: Skipping mixed fan-out (reuses Phase 1 & Phase 2 golden fixtures).")
            return

        unary_trace_id = self._build_shared_trace_context(
            "concurrency_phase_1_unary", [{"role": "user", "content": UNARY_PROMPT}]
        )
        stream_trace_id = self._build_shared_trace_context(
            "concurrency_phase_2_stream", [{"role": "user", "content": STREAM_PROMPT}]
        )

        vu_count = self.concurrency
        total_iters = self.iterations
        log.info(f"  └─ Running {total_iters} heterogeneous tasks across {vu_count} VUs...")

        profiler = PipelineLoadProfiler("Phase 3 - Mixed Workload (Unary/Stream/Aggregated)")

        async def _mixed_worker(idx: int) -> Dict[str, Any]:
            mode_type = idx % 3
            session_id = f"mixed_sess_{mode_type}_{idx:04d}"

            if mode_type == 0:
                with profiler.track_request(worker_idx=idx, mode="unary") as sw:
                    res = await acompletion(
                        model=self.target_model,
                        messages=[{"role": "user", "content": UNARY_PROMPT}],
                        max_tokens=512,
                        stream=False,
                        trace_id=unary_trace_id,
                        session_id=session_id,
                        metadata={"vcr_scenario": "concurrency_phase_1_unary", "vcr_invoker": CURRENT_INVOKER}
                    )
                    text = self._extract_unary_content(res)
                    sw.add_chunk(len(text))
                meta = getattr(res, "system_meta", None)
                return {"idx": idx, "mode": "unary", "session": getattr(meta, "session_id", None), "expected": session_id, "len": len(text)}

            elif mode_type == 1:
                with profiler.track_request(worker_idx=idx, mode="stream") as sw:
                    stream = await acompletion(
                        model=self.target_model,
                        messages=[{"role": "user", "content": STREAM_PROMPT}],
                        max_tokens=1024,
                        stream=True,
                        trace_id=stream_trace_id,
                        session_id=session_id,
                        metadata={"vcr_scenario": "concurrency_phase_2_stream", "vcr_invoker": CURRENT_INVOKER}
                    )
                    buf = []
                    async for ch in stream:
                        delta = StateTraverser.resolve(ch, "choices.0.delta.content")
                        piece = str(delta) if delta else ""
                        if piece:
                            buf.append(piece)
                        sw.add_chunk(len(piece))
                meta = getattr(stream, "system_meta", None)
                return {"idx": idx, "mode": "stream", "session": getattr(meta, "session_id", None), "expected": session_id, "len": len("".join(buf))}

            else:
                with profiler.track_request(worker_idx=idx, mode="stream_complete") as sw:
                    comp_res = await acompletion(
                        model=self.target_model,
                        messages=[{"role": "user", "content": STREAM_PROMPT}],
                        max_tokens=1024,
                        stream=True,
                        complete_response=True,
                        trace_id=stream_trace_id,
                        session_id=session_id,
                        metadata={"vcr_scenario": "concurrency_phase_2_stream", "vcr_invoker": CURRENT_INVOKER}
                    )
                    text = self._extract_unary_content(comp_res)
                    sw.add_chunk(len(text))
                meta = getattr(comp_res, "system_meta", None)
                return {"idx": idx, "mode": "stream_complete", "session": getattr(meta, "session_id", None), "expected": session_id, "len": len(text)}

        async with profiler:
            results = await profiler.run_vu_pool(
                total_iterations=total_iters,
                concurrency=vu_count,
                worker_fn=_mixed_worker
            )

        stream_lengths = set()
        for r in results:
            if r["session"] != r["expected"]:
                raise RuntimeError(f"Mixed Workload Session Leak in Worker #{r['idx']} ({r['mode']}).")
            if r["len"] == 0:
                raise RuntimeError(f"Mixed Workload Empty Output in Worker #{r['idx']} ({r['mode']}).")
            if r["mode"] in ("stream", "stream_complete"):
                stream_lengths.add(r["len"])

        if len(stream_lengths) != 1:
            raise RuntimeError(f"StreamAggregator Parity Mismatch: lengths={stream_lengths}")

        slo = LoadSLO(max_error_rate_pct=0.0, max_memory_peak_mb=64.0)
        profiler.assert_slo(slo)


class ConcurrencySuiteRunner:
    def __init__(
        self,
        vcr_config: VCRPlaybackConfig,
        vcr_manager: Any,
        target_model: str,
        concurrency: int,
        iterations: int
    ):
        self.vcr_config = vcr_config
        self.vcr_manager = vcr_manager
        self.target_model = target_model
        self.concurrency = concurrency
        self.iterations = iterations

    async def execute(self):
        free_port = _get_free_port(8355)
        net_config = E2EConfig(host="127.0.0.1", port=free_port, protocol="http")
        results = await PipelineConcurrencySuite(
            config=net_config,
            vcr_config=self.vcr_config,
            vcr_manager=self.vcr_manager,
            target_model=self.target_model,
            concurrency=self.concurrency,
            iterations=self.iterations
        ).run_pipeline()

        if all(r.passed for r in results):
            log.info("PIPELINE CONCURRENCY & LOAD PROFILE SUITE VALIDATED SUCCESSFULLY.")
        else:
            log.critical("PIPELINE CONCURRENCY FAILURE DETECTED.")


def main(args_list: list[str] = None):
    parser = create_e2e_parser("Core Pipeline VCR Concurrency & Load Test Runner")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=CONCURRENCY_FANOUT,
        help="Number of concurrent Virtual Users (VUs) during VCR replay (default: 50)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="Total requests to execute across VUs (default: equals --concurrency)"
    )
    add_vcr_arguments(parser)
    parsed_args, _ = parser.parse_known_args(args_list)
    _, run_context = build_e2e_context(parsed_args)
    resolved_model = run_context["target_model"]

    vcr_config = VCRPlaybackConfig.from_args(parsed_args)
    manager = VCRInjector.apply(config=vcr_config, fixture_dir=FIXTURE_ROOT)
    app = ConcurrencySuiteRunner(
        vcr_config=vcr_config,
        vcr_manager=manager,
        target_model=resolved_model,
        concurrency=parsed_args.concurrency,
        iterations=parsed_args.iterations
    )

    try:
        PhaseReactor.ignite(main_coro_func=app.execute)
    finally:
        VCRInjector.remove()


if __name__ == "__main__":
    main()