# fiber.dev.trace.profile.metrics
"""
Lightweight, zero-dependency load profiling and SLO enforcement engine for Fiber pipelines.
Captures latency percentiles (p50/p90/p95/p99), TTFT, throughput (RPS/CPS), peak memory delta,
and asyncio event-loop lag without external profiling libraries.
"""
from __future__ import annotations

import asyncio
import math
import time
import tracemalloc
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional

from xphi.watcher.plane.emitter import get_emitter

profile_log = get_emitter("trace.profile")


@dataclass
class RequestSample:
    """단일 파이프라인 실행에 대한 마이크로 계측 샘플"""
    worker_idx: int
    mode: str
    duration_ms: float
    ttft_ms: float
    chunks: int = 1
    chars: int = 0
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoadSLO:
    """k6 thresholds 스타일의 파이프라인 성능 방어선(SLO) 정의"""
    max_error_rate_pct: float = 0.0
    max_p95_ms: Optional[float] = None
    max_p99_ms: Optional[float] = None
    max_ttft_p95_ms: Optional[float] = None
    min_rps: Optional[float] = None
    min_cps: Optional[float] = None
    max_memory_peak_mb: Optional[float] = None
    max_loop_lag_ms: Optional[float] = None


@dataclass
class LoadProfileSummary:
    """집계된 부하 테스트 최종 메트릭 스냅샷"""
    title: str
    total_requests: int
    succeeded: int
    failed: int
    error_rate_pct: float
    wall_time_ms: float
    rps: float
    cps: float
    total_chunks: int
    total_chars: int
    p50_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    ttft_p50_ms: float
    ttft_p95_ms: float
    peak_memory_mb: float
    max_loop_lag_ms: float


class RequestStopwatch:
    """개별 워커 코루틴 내부에서 TTFT 및 E2E 소요 시간을 정밀 측정하는 컨텍스트 헬퍼"""
    def __init__(self, worker_idx: int, mode: str):
        self.worker_idx = worker_idx
        self.mode = mode
        self.start_ts: float = time.perf_counter()
        self.first_token_ts: Optional[float] = None
        self.end_ts: float = 0.0
        self.chunks: int = 0
        self.chars: int = 0
        self.success: bool = True
        self.error: Optional[str] = None
        self.metadata: Dict[str, Any] = {}

    def mark_first_token(self) -> None:
        if self.first_token_ts is None:
            self.first_token_ts = time.perf_counter()

    def add_chunk(self, char_count: int = 0) -> None:
        if self.chunks == 0:
            self.mark_first_token()
        self.chunks += 1
        self.chars += char_count

    def finish(self, **meta: Any) -> RequestSample:
        self.end_ts = time.perf_counter()
        if self.first_token_ts is None:
            self.first_token_ts = self.end_ts
        self.metadata.update(meta)

        return RequestSample(
            worker_idx=self.worker_idx,
            mode=self.mode,
            duration_ms=(self.end_ts - self.start_ts) * 1000.0,
            ttft_ms=(self.first_token_ts - self.start_ts) * 1000.0,
            chunks=max(1, self.chunks),
            chars=self.chars,
            success=self.success,
            error=self.error,
            metadata=self.metadata,
        )


class PipelineLoadProfiler:
    """
    비동기 파이프라인 동시성 및 부하 계측기.
    - 백그라운드에서 이벤트 루프 지연(Event Loop Lag)을 10ms 주기로 모니터링
    - tracemalloc을 통해 부하 구간 동안의 순수 메모리 피크 증가량(Delta Peak MB) 측정
    """
    def __init__(self, title: str, monitor_loop_lag: bool = True):
        self.title = title
        self.monitor_loop_lag = monitor_loop_lag
        self.samples: List[RequestSample] = []
        self.start_ts: float = 0.0
        self.end_ts: float = 0.0
        self.peak_memory_bytes: int = 0
        self.max_loop_lag_ms: float = 0.0
        self._lag_task: Optional[asyncio.Task] = None
        self._stop_lag_monitor = False
        self._tracemalloc_owned = False

    async def __aenter__(self) -> "PipelineLoadProfiler":
        if not tracemalloc.is_tracing():
            tracemalloc.start()
            self._tracemalloc_owned = True
        else:
            tracemalloc.reset_peak()

        self.start_ts = time.perf_counter()
        if self.monitor_loop_lag:
            self._stop_lag_monitor = False
            self._lag_task = asyncio.create_task(self._measure_event_loop_lag())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.end_ts = time.perf_counter()
        if self._lag_task is not None:
            self._stop_lag_monitor = True
            self._lag_task.cancel()
            try:
                await self._lag_task
            except asyncio.CancelledError:
                pass

        _, peak = tracemalloc.get_traced_memory()
        self.peak_memory_bytes = peak
        if self._tracemalloc_owned:
            tracemalloc.stop()

    async def _measure_event_loop_lag(self, interval: float = 0.01) -> None:
        loop = asyncio.get_running_loop()
        expected = loop.time() + interval
        while not self._stop_lag_monitor:
            await asyncio.sleep(interval)
            now = loop.time()
            lag_ms = max(0.0, (now - expected) * 1000.0)
            if lag_ms > self.max_loop_lag_ms:
                self.max_loop_lag_ms = lag_ms
            expected = now + interval

    @contextmanager
    def track_request(self, worker_idx: int, mode: str = "unary"):
        sw = RequestStopwatch(worker_idx=worker_idx, mode=mode)
        try:
            yield sw
        except Exception as exc:
            sw.success = False
            sw.error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            self.samples.append(sw.finish())

    async def run_vu_pool(
        self,
        total_iterations: int,
        concurrency: int,
        worker_fn: Callable[[int], Coroutine[Any, Any, Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """
        k6 스타일의 고정 가상 유저(Virtual Users) 풀 실행기.
        동시에 최대 `concurrency`개의 파이프라인만 인플라이트(In-flight) 상태를 유지하며
        총 `total_iterations` 횟수만큼 쉼 없이 파이프라인을 타격(Closed-loop).
        """
        sem = asyncio.Semaphore(concurrency)

        async def _bounded_call(idx: int) -> Dict[str, Any]:
            async with sem:
                return await worker_fn(idx)

        return await asyncio.gather(*[_bounded_call(i) for i in range(total_iterations)])

    @staticmethod
    def _percentile(sorted_vals: List[float], pct: float) -> float:
        if not sorted_vals:
            return 0.0
        k = (len(sorted_vals) - 1) * (pct / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

    def summarize(self, emit_log: bool = True) -> LoadProfileSummary:
        wall_sec = max(1e-6, self.end_ts - self.start_ts)
        total = len(self.samples)
        succeeded = sum(1 for s in self.samples if s.success)
        failed = total - succeeded
        error_rate = (failed / total * 100.0) if total > 0 else 0.0

        durations = sorted(s.duration_ms for s in self.samples)
        ttfts = sorted(s.ttft_ms for s in self.samples)
        total_chunks = sum(s.chunks for s in self.samples)
        total_chars = sum(s.chars for s in self.samples)

        summary = LoadProfileSummary(
            title=self.title,
            total_requests=total,
            succeeded=succeeded,
            failed=failed,
            error_rate_pct=error_rate,
            wall_time_ms=wall_sec * 1000.0,
            rps=total / wall_sec,
            cps=total_chunks / wall_sec,
            total_chunks=total_chunks,
            total_chars=total_chars,
            p50_ms=self._percentile(durations, 50),
            p90_ms=self._percentile(durations, 90),
            p95_ms=self._percentile(durations, 95),
            p99_ms=self._percentile(durations, 99),
            min_ms=durations[0] if durations else 0.0,
            max_ms=durations[-1] if durations else 0.0,
            ttft_p50_ms=self._percentile(ttfts, 50),
            ttft_p95_ms=self._percentile(ttfts, 95),
            peak_memory_mb=self.peak_memory_bytes / (1024.0 * 1024.0),
            max_loop_lag_ms=self.max_loop_lag_ms,
        )

        if emit_log:
            profile_log.info(
                f"\n  ┌── [📊 LOAD PROFILE: {summary.title}] ──────────────────────────────────────\n"
                f"  │ • Throughput : {summary.rps:8.1f} req/s  | {summary.cps:9.1f} chunks/s "
                f"(Reqs: {summary.succeeded}/{summary.total_requests}, Chunks: {summary.total_chunks})\n"
                f"  │ • Latency E2E: p50={summary.p50_ms:6.2f}ms | p90={summary.p90_ms:6.2f}ms | "
                f"p95={summary.p95_ms:6.2f}ms | p99={summary.p99_ms:6.2f}ms\n"
                f"  │ • TTFT       : p50={summary.ttft_p50_ms:6.2f}ms | p95={summary.ttft_p95_ms:6.2f}ms | "
                f"Min/Max={summary.min_ms:.1f}/{summary.max_ms:.1f}ms\n"
                f"  └ • Runtime    : MemPeak={summary.peak_memory_mb:5.2f}MB | "
                f"LoopLag={summary.max_loop_lag_ms:5.2f}ms | Wall={summary.wall_time_ms:.1f}ms"
            )
        return summary

    def assert_slo(self, slo: LoadSLO, summary: Optional[LoadProfileSummary] = None) -> LoadProfileSummary:
        """지정된 SLO 임계치를 위반하면 즉시 RuntimeError를 발생시켜 CI/CD 회귀를 차단"""
        s = summary or self.summarize(emit_log=True)
        violations: List[str] = []

        if s.error_rate_pct > slo.max_error_rate_pct:
            violations.append(f"error_rate {s.error_rate_pct:.2f}% > {slo.max_error_rate_pct:.2f}%")
        if slo.max_p95_ms is not None and s.p95_ms > slo.max_p95_ms:
            violations.append(f"p95_ms {s.p95_ms:.2f}ms > {slo.max_p95_ms:.2f}ms")
        if slo.max_p99_ms is not None and s.p99_ms > slo.max_p99_ms:
            violations.append(f"p99_ms {s.p99_ms:.2f}ms > {slo.max_p99_ms:.2f}ms")
        if slo.max_ttft_p95_ms is not None and s.ttft_p95_ms > slo.max_ttft_p95_ms:
            violations.append(f"ttft_p95_ms {s.ttft_p95_ms:.2f}ms > {slo.max_ttft_p95_ms:.2f}ms")
        if slo.min_rps is not None and s.rps < slo.min_rps:
            violations.append(f"rps {s.rps:.1f} < {slo.min_rps:.1f}")
        if slo.min_cps is not None and s.cps < slo.min_cps:
            violations.append(f"cps {s.cps:.1f} < {slo.min_cps:.1f}")
        if slo.max_memory_peak_mb is not None and s.peak_memory_mb > slo.max_memory_peak_mb:
            violations.append(f"peak_memory_mb {s.peak_memory_mb:.2f}MB > {slo.max_memory_peak_mb:.2f}MB")
        if slo.max_loop_lag_ms is not None and s.max_loop_lag_ms > slo.max_loop_lag_ms:
            violations.append(f"max_loop_lag_ms {s.max_loop_lag_ms:.2f}ms > {slo.max_loop_lag_ms:.2f}ms")

        if violations:
            raise RuntimeError(f"[SLO Violation in '{s.title}'] " + ", ".join(violations))
        return s