# fiber.dev.e2e.inter.stream
"""
@desc: Interactive Concurrency Stream & Console Kill-Switch Harness
- Reuses golden VCR fixtures from `fiber.dev.e2e.inter.concurrency` (Zero API cost / Zero network).
- Sustains staggered concurrent LLM streams without artificial pause loops: when budget hits 0,
  workers continue attempting calls and surface actual FuelBreaker rejections/severing until restored.
- ⚠️ Operational Notes:
  1. Requires local Redis (`127.0.0.1:6379`) to receive real-time Kill/Restore signals from Console.
  2. Ensure `--tenant` matches Console's Target Tenant ID (default: `accounting_dept`).
  3. Runs continuously until terminated via `Ctrl+C` (or pass `--once` to exit on first kill).
"""
from __future__ import annotations

import asyncio
import json
import os
import random
from typing import Any, Dict

os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")

from fiber.dev.base.workflow.builder import build_e2e_context, create_e2e_parser
from fiber.dev.base.workflow.pipeline import log
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.infra.ops.account.registry import DEFAULT_TENANT_ID
from fiber.infra.vcr.injector import VCRInjector
from fiber.infra.vcr.manager import (
    VCRIdentityRule,
    VCRPlaybackConfig,
    add_vcr_arguments,
)
from fiber.infra.ops.finops.watcher import default_finops_watcher, default_tenant_eco
from fiber.llm.entry import acompletion
from fiber.llm.pipeline.channel.fuel import BillingObserver, FuelBreaker

from xphi.arch.bound.adapter.pta import PtaAdapter
from xphi.arch.contract.event.next import next_trace_id
from xphi.arch.model.auth import NodeAuthPayload
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.node.space.tunnel.factory import TunnelFactory
from xphi.state.phase.reactor import PhaseReactor

FIXTURE_ROOT = "fiber-compats/suites/e2e"
CONCURRENCY_INVOKER = "fiber.dev.e2e.inter.concurrency"
SCENARIO_NAME = "concurrency_phase_2_stream"

STREAM_PROMPT = (
    "Write a detailed 4-part technical walkthrough of how asynchronous byte-stream "
    "framing, backpressure watermarks, and coroutine context propagation operate "
    "under high concurrency."
)


class InteractiveStreamKillHarness:
    def __init__(
        self,
        target_model: str,
        tenant_id: str = DEFAULT_TENANT_ID,
        concurrency: int = 4,
        pace_ms: float = 95.0,
        auto_kill_sec: float = 0.0,
        run_once: bool = False,
    ):
        self.target_model = target_model
        self.tenant_id = tenant_id
        self.concurrency = concurrency
        self.pace_sec = max(0.02, pace_ms / 1000.0)
        self.auto_kill_sec = auto_kill_sec
        self.run_once = run_once

        self.broker = DphiBroker(timeout=5.0)
        self.pta_adapter = PtaAdapter(broker=self.broker)

    def _build_shared_trace_id(self, messages: list) -> str:
        seed = VCRIdentityRule.generate_seed(
            scenario_name=SCENARIO_NAME,
            messages=messages,
            invoker=CONCURRENCY_INVOKER,
        )
        return next_trace_id(seed)

    async def _ensure_initial_budget(self) -> int:
        balance = await self.pta_adapter.get_balance(self.tenant_id, "fuel")
        actual_balance = max(0, int(balance))
        default_finops_watcher.local_budget_cache[self.tenant_id] = float(actual_balance)
        return actual_balance

    async def _run_single_stream_vu(
        self,
        worker_idx: int,
        loop_idx: int,
        shared_trace_id: str,
        messages: list,
        first_chunk_event: asyncio.Event,
    ) -> Dict[str, Any]:
        session_id = f"vu{worker_idx:02d}_req{loop_idx:03d}"
        active_budget = max(0.0, float(default_finops_watcher.local_budget_cache.get(self.tenant_id, 0.0)))

        if worker_idx == 0 or loop_idx == 1:
            log.info(
                f"▶ [acompletion] Dispatching stream -> model='{self.target_model}' "
                f"session='{session_id}' (budget={int(active_budget):,} Fuel)"
            )

        chunks = 0
        chars = 0
        rolling_buf = ""

        try:
            response_stream = await acompletion(
                model=self.target_model,
                messages=messages,
                max_tokens=1024,
                stream=True,
                trace_id=shared_trace_id,
                session_id=session_id,
                tenant_id=self.tenant_id,
                fuel_breaker=FuelBreaker(finops_watcher=default_finops_watcher),
                billing_observer=BillingObserver(tenant_eco=default_tenant_eco, pta_adapter=self.pta_adapter),
                metadata={
                    "vcr_scenario": SCENARIO_NAME,
                    "vcr_invoker": CONCURRENCY_INVOKER,
                    "worker_idx": worker_idx,
                    "node_auth": NodeAuthPayload(
                        fuel_budget=int(active_budget),
                        tenant_id=self.tenant_id,
                        is_enforced=True,
                    ),
                },
            )

            async for chunk in response_stream:
                chunks += 1
                delta = StateTraverser.resolve(chunk, "choices.0.delta.content")
                if delta:
                    piece = str(delta)
                    chars += len(piece)
                    rolling_buf += piece

                if chunks == 1:
                    first_chunk_event.set()

                if (chunks + worker_idx * 3) % 12 == 0 and rolling_buf.strip():
                    snippet = " ".join(rolling_buf.strip().split())[-48:]
                    log.info(
                        f"💬 [LLM Stream | VU #{worker_idx:02d}] chunk #{chunks:03d} ({chars:,} chars) "
                        f"-> \"...{snippet}\""
                    )
                    rolling_buf = ""

                await asyncio.sleep(self.pace_sec * random.uniform(0.8, 1.2))

        except Exception as exc:
            # Pre-flight 또는 스트림 도중 발생한 실제 예외를 그대로 에러 로그로 출력
            log.error(f"❌ [STREAM ERROR | VU #{worker_idx:02d}] session='{session_id}' failed: {exc}")
            return {"worker_idx": worker_idx, "chunks": chunks, "chars": chars, "severed": True, "error": str(exc)}

        rem_budget = default_finops_watcher.local_budget_cache.get(self.tenant_id, 0.0)
        severed = rem_budget <= 0.0

        if severed:
            if chunks <= 1:
                log.error(
                    f"❌ [HTTP 402 PRE-FLIGHT / IMMEDIATE BLOCK | VU #{worker_idx:02d}] "
                    f"Call '{session_id}' rejected immediately (budget={rem_budget:.1f} Fuel)."
                )
            else:
                log.warning(
                    f"🛑 [MID-FLIGHT SEVERED | VU #{worker_idx:02d}] "
                    f"Stream '{session_id}' physically cut via aclose() at chunk #{chunks} ({chars:,} chars)!"
                )

        return {
            "worker_idx": worker_idx,
            "chunks": chunks,
            "chars": chars,
            "severed": severed,
        }

    async def execute(self) -> None:
        os.environ["FIBER_ENFORCE_QUOTA"] = "true"
        messages = [{"role": "user", "content": STREAM_PROMPT}]
        shared_trace_id = self._build_shared_trace_id(messages)

        default_finops_watcher.start()
        vu_tasks: list[asyncio.Task] = []
        try:
            await default_finops_watcher.wait_until_ready()
            init_bal = await self._ensure_initial_budget()

            log.info("=" * 76)
            log.info(f"🚀 [Continuous Stream Kill Harness] Tenant: '{self.tenant_id}' | VUs: {self.concurrency}")
            log.info(f"   Model: '{self.target_model}' | Initial Balance: {init_bal:,} Fuel")
            log.info("   Mode : UNGATED CONTINUOUS CALLS (Fails/Severs on 0 Fuel, Ctrl+C to stop)")
            log.info("=" * 76)

            first_chunk_event = asyncio.Event()

            async def _sustained_vu_loop(w_idx: int) -> None:
                if w_idx > 0:
                    await asyncio.sleep(w_idx * (self.pace_sec * 1.8))

                loops = 0
                while True:
                    loops += 1
                    res = await self._run_single_stream_vu(
                        w_idx, loops, shared_trace_id, messages, first_chunk_event
                    )
                    if res["severed"]:
                        if self.run_once:
                            return
                        # 인위적인 복구 대기 없이 1.2초 후 곧바로 다음 호출을 시도하여 계속 차단 에러가 나도록 함
                        await asyncio.sleep(1.2)
                    else:
                        await asyncio.sleep(0.08)

            vu_tasks = [asyncio.create_task(_sustained_vu_loop(i)) for i in range(self.concurrency)]

            if init_bal > 0:
                try:
                    await asyncio.wait_for(first_chunk_event.wait(), timeout=10.0)
                    log.info(
                        f"⚡ {self.concurrency} concurrent LLM streams flowing! "
                        f"Click '🚨 LLM STREAM BREAK' or 'Zero (Kill)' in Console to observe live severing & errors."
                    )
                except asyncio.TimeoutError:
                    log.warning("⚠️ [Stream Harness] Timed out waiting for first chunk; continuing VU loops...")
            else:
                log.warning(
                    f"🛑 [Initial Balance = 0 Fuel] Starting in blocked state ({self.concurrency} VUs). "
                    f"Streams will continuously fail with HTTP 402 until budget is provisioned."
                )

            if self.auto_kill_sec > 0:
                await asyncio.sleep(self.auto_kill_sec)
                log.info("🚨 [Auto-Kill] Publishing REVOKE signal to 'finops:control_plane'...")
                tunnel = await TunnelFactory.get_default()
                next_ver = default_finops_watcher.policy_versions.get(self.tenant_id, 1) + 1
                await tunnel.publish(
                    "finops:control_plane",
                    json.dumps({
                        "type": "update_budget",
                        "tenant_id": self.tenant_id,
                        "fuel_budget": 0.0,
                        "finops": {
                            "op": "REVOKE",
                            "budget_usd": 0.0,
                            "exchange_rate": 1_000_000,
                            "policy_ver": next_ver,
                        },
                    }),
                )

            await asyncio.gather(*vu_tasks)

        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("\n🛑 [Ctrl+C Detected] Cancelling active LLM streams and shutting down harness cleanly...")
        finally:
            for t in vu_tasks:
                if not t.done():
                    t.cancel()
            if vu_tasks:
                await asyncio.gather(*vu_tasks, return_exceptions=True)
            default_finops_watcher.stop()
            await self.broker.close()


def main(args_list: list[str] | None = None) -> None:
    parser = create_e2e_parser("Interactive Concurrency Stream & Console Kill-Switch Harness")
    parser.add_argument("--tenant", type=str, default=DEFAULT_TENANT_ID, help="Target tenant ID")
    parser.add_argument("--concurrency", type=int, default=4, help="Concurrent streaming VUs (default: 4)")
    parser.add_argument("--pace-ms", type=float, default=95.0, help="Base delay per chunk in ms (default: 95)")
    parser.add_argument("--auto-kill-sec", type=float, default=0.0, help="Auto-fire REVOKE after N seconds")
    parser.add_argument("--once", action="store_true", help="Exit after first Kill-Switch severing")
    add_vcr_arguments(parser)
    parser.set_defaults(vcr="replay")

    parsed_args, _ = parser.parse_known_args(args_list)

    _, run_context = build_e2e_context(parsed_args)
    resolved_model = run_context["target_model"]

    vcr_config = VCRPlaybackConfig.from_args(parsed_args)
    VCRInjector.apply(config=vcr_config, fixture_dir=FIXTURE_ROOT)

    harness = InteractiveStreamKillHarness(
        target_model=resolved_model,
        tenant_id=parsed_args.tenant,
        concurrency=parsed_args.concurrency,
        pace_ms=parsed_args.pace_ms,
        auto_kill_sec=parsed_args.auto_kill_sec,
        run_once=parsed_args.once,
    )

    try:
        PhaseReactor.ignite(main_coro_func=harness.execute)
    except KeyboardInterrupt:
        pass
    finally:
        VCRInjector.remove()


if __name__ == "__main__":
    main()