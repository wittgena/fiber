# fiber.phase.fsm.traverser
from dataclasses import dataclass, field, replace
from typing import Dict, Any, List, Optional, Callable, Awaitable, Tuple, Sequence, Union

from fiber.phase.fsm.compiler import (
    ExFSMCompiler,
    ExCompiledBundle,
    CompactKey,
)
from fiber.phase.fsm.runtime import (
    ExBranchController,
    BranchCursor,
    DagJoinBarrier,
)
from fiber.phase.fsm.bridge import FiberExecutionBridge

from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fsm.traverser", phase="MULTIVERSE_TRAVERSER")

# Callback signature: (cursor, node_spec, task_io_spec) -> (meta_updates, heavy_offload_data, step_cost)
NodeExecutorFn = Callable[
    [BranchCursor, Dict[str, Any], Dict[str, Any]],
    Awaitable[Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]]
]

# Callback signature: (arrived_cursors) -> winner_idx
ArbiterFn = Callable[[List[BranchCursor]], int]


# TRAVERSAL RESULT
@dataclass
class TraversalResult:
    final_cursor: BranchCursor
    root_hash: str
    total_ticks: int
    reclaimed_fuel: int
    visited_history: List[str] = field(default_factory=list)
    parked_cursors: List[BranchCursor] = field(default_factory=list)  # HITL-parked & barrier-waiting sibling cursors
    join_collapses: int = 0

class ExGraphTraverser:
    """
    Orchestrates end-to-end multiverse traversal on top of ExFSMCompiler, ExBranchController, and FiberExecutionBridge.
    Unifies initial execution (`run` / `traverse_bundle`) and checkpoint continuation (`resume`) via `_drive_queue`.
    """

    def __init__(
        self,
        broker: Optional[DphiBroker] = None,
        node_executor: Optional[NodeExecutorFn] = None,
        arbiter: Optional[ArbiterFn] = None,
        tenant_id: str = "default_tenant",
        mcp_gateway_url: str = "http://127.0.0.1:8356/v1/mcp-gateway",
        max_ticks: int = 500,
    ):
        self.compiler = ExFSMCompiler()
        self.runtime = ExBranchController(broker=broker)
        self._default_bridge: Optional[FiberExecutionBridge] = None

        if node_executor is not None:
            self.node_executor = node_executor
        else:
            self._default_bridge = FiberExecutionBridge(
                runtime=self.runtime,
                tenant_id=tenant_id,
                mcp_gateway_url=mcp_gateway_url,
            )
            self.node_executor = self._default_bridge

        self.arbiter = arbiter or self._default_arbiter
        self.max_ticks = max_ticks
        self._active_cursors: Dict[str, BranchCursor] = {}
        self._parked_cursors: Dict[str, BranchCursor] = {}

    async def __aenter__(self) -> "ExGraphTraverser":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Closes bridge HTTP resources and auto-collapses dangling unspent branches (excluding parked ones)."""
        if self._default_bridge is not None:
            await self._default_bridge.aclose()

        dangling = [
            c for sid, c in self._active_cursors.items()
            if not c.is_terminated and sid not in self._parked_cursors
        ]
        if len(dangling) >= 2:
            log.warning(f"[Traverser] Auto-collapsing {len(dangling)} dangling branches on exit.")
            try:
                reclaimed = await self.runtime.collapse_and_merge(
                    cursors=dangling,
                    winner_idx=self.arbiter(dangling),
                    merged_stream_id=f"{dangling[0].stream_id}_reclaimed",
                )
                self._active_cursors = {reclaimed.stream_id: reclaimed}
            except Exception as e:
                log.error(f"[Traverser] Failed to reclaim dangling branches: {e}")

    # [Public Entrypoints] Run, Traverse Bundle & Resume from Checkpoints
    async def run(
        self,
        asl_schema: Dict[str, Any],
        base_stream_id: str,
        initial_fuel: int = 1000,
        initial_meta: Optional[Dict[str, Any]] = None,
    ) -> TraversalResult:
        bundle = self.compiler.compile_from_asl(asl_schema, trace_id=base_stream_id)
        return await self.traverse_bundle(
            bundle=bundle,
            base_stream_id=base_stream_id,
            initial_fuel=initial_fuel,
            initial_meta=initial_meta,
        )

    async def traverse_bundle(
        self,
        bundle: ExCompiledBundle,
        base_stream_id: str,
        initial_fuel: int = 1000,
        initial_meta: Optional[Dict[str, Any]] = None,
    ) -> TraversalResult:
        genesis = await self.runtime.mount_bundle_and_spawn(
            bundle=bundle,
            base_stream_id=base_stream_id,
            initial_fuel=initial_fuel,
            initial_meta=initial_meta,
        )
        return await self._drive_queue(
            initial_cursors=[genesis],
            base_stream_id=base_stream_id,
            root_hash=bundle.root_hash,
            is_resumed=False,
        )

    async def resume(
        self,
        targets: Sequence[Union[BranchCursor, str]],
        base_stream_id: str,
    ) -> TraversalResult:
        """
        Resumes multiverse execution directly from one or more BranchCursors, PTA pointers ('tx:idx'),
        or stream IDs without re-executing prior nodes. Automatically clears stale HITL yield flags.
        """
        if not targets:
            raise ValueError("resume() requires at least one BranchCursor or PTA pointer/stream ID.")

        if self.runtime.barrier:
            self.runtime.barrier.drain_waiting()

        restored: List[BranchCursor] = []
        for item in targets:
            cursor = self.runtime.restore_cursor(item) if isinstance(item, str) else item
            cleaned_meta = {k: v for k, v in cursor.meta_ctx.items() if k != "_hitl_yield"}
            restored.append(replace(cursor, meta_ctx=cleaned_meta))

        return await self._drive_queue(
            initial_cursors=restored,
            base_stream_id=base_stream_id,
            root_hash=restored[0].active_blob_hash,
            is_resumed=True,
        )

    # [Core Engine] Unified Multiverse Scheduling & Barrier Loop
    async def _drive_queue(
        self,
        initial_cursors: List[BranchCursor],
        base_stream_id: str,
        root_hash: str,
        is_resumed: bool = False,
    ) -> TraversalResult:
        barrier = self.runtime.barrier
        run_queue: List[BranchCursor] = []
        self._active_cursors = {c.stream_id: c for c in initial_cursors}
        self._parked_cursors.clear()
        completed_terminals: List[BranchCursor] = []

        tag = "(RESUMED)" if is_resumed else ""
        visited_history: List[str] = [f"{c.stream_id}:{c.curr_node}{tag}" for c in initial_cursors]

        total_ticks = 0
        join_collapses = 0
        expected_parallel = len(initial_cursors)
        last_completed = initial_cursors[0]

        # Route initial cursors through barrier first in case they start directly on a DAG join node
        for init_c in initial_cursors:
            collapsed = await self._route_or_collapse_at_barrier(
                cursor=init_c,
                barrier=barrier,
                expected_count=expected_parallel,
                base_stream_id=base_stream_id,
                run_queue=run_queue,
                visited_history=visited_history,
            )
            if collapsed:
                join_collapses += 1
                expected_parallel = 1

        while run_queue and total_ticks < self.max_ticks:
            cursor = run_queue.pop(0)
            if cursor.is_terminated:
                last_completed = cursor
                completed_terminals.append(cursor)
                self._active_cursors.pop(cursor.stream_id, None)
                continue

            total_ticks += 1
            node_spec = self.runtime.get_node_spec(cursor)
            task_io = self.runtime.resolve_task_io(node_spec)

            meta_updates, offload_data, step_cost = await self.node_executor(cursor, node_spec, task_io)
            merged_meta = {**cursor.meta_ctx, **(meta_updates or {})}

            # 1. Pre-Flight HITL Yield (HTTP 202): Park branch in-place without spending PTA
            if merged_meta.get("_hitl_yield"):
                parked = replace(cursor, meta_ctx=merged_meta)
                self._parked_cursors[parked.stream_id] = parked
                self._active_cursors.pop(parked.stream_id, None)
                visited_history.append(f"{parked.stream_id}:{parked.curr_node}(HITL_PARKED)")
                last_completed = parked
                continue

            cursor = replace(cursor, meta_ctx=merged_meta)

            # 2. 1:N Fan-Out Fork
            fanout_targets = self._detect_fanout_targets(node_spec, cursor.meta_ctx)
            if len(fanout_targets) >= 2:
                self._active_cursors.pop(cursor.stream_id, None)
                forked = await self.runtime.fork(parent=cursor, branch_targets=fanout_targets)
                expected_parallel = len(forked)

                for child in forked:
                    self._active_cursors[child.stream_id] = child
                    visited_history.append(f"{child.stream_id}:{child.curr_node}(FORK)")
                    collapsed = await self._route_or_collapse_at_barrier(
                        cursor=child,
                        barrier=barrier,
                        expected_count=expected_parallel,
                        base_stream_id=base_stream_id,
                        run_queue=run_queue,
                        visited_history=visited_history,
                    )
                    if collapsed:
                        join_collapses += 1
                        expected_parallel = 1
                continue

            # 3. Standard 1:1 Step (with automatic SYMLINK mount / stack RETURN)
            next_cursor = await self.runtime.step(
                cursor=cursor,
                step_cost=step_cost,
                meta_updates=meta_updates,
                offload_data=offload_data,
            )
            self._active_cursors[next_cursor.stream_id] = next_cursor
            visited_history.append(f"{next_cursor.stream_id}:{next_cursor.curr_node}")

            if next_cursor.is_terminated:
                last_completed = next_cursor
                completed_terminals.append(next_cursor)
                self._active_cursors.pop(next_cursor.stream_id, None)
                continue

            # 4. N:1 Join Barrier Arrival & Collapse
            collapsed = await self._route_or_collapse_at_barrier(
                cursor=next_cursor,
                barrier=barrier,
                expected_count=expected_parallel,
                base_stream_id=base_stream_id,
                run_queue=run_queue,
                visited_history=visited_history,
            )
            if collapsed:
                join_collapses += 1
                expected_parallel = 1

        # 5. If any branch was parked by HITL, drain barrier-waiting siblings into `parked_cursors` so they can resume together
        if self._parked_cursors and barrier:
            for waiting_sibling in barrier.drain_waiting():
                self._parked_cursors.setdefault(waiting_sibling.stream_id, waiting_sibling)
                self._active_cursors.pop(waiting_sibling.stream_id, None)

        # 6. Final Sweep: If no branches are parked, collapse multiple completed/active parallel branches to reclaim all fuel
        if not self._parked_cursors:
            unmerged_pool = list(self._active_cursors.values()) or completed_terminals
            if len(unmerged_pool) >= 2:
                last_completed = await self.runtime.collapse_and_merge(
                    cursors=unmerged_pool,
                    winner_idx=self.arbiter(unmerged_pool),
                    merged_stream_id=base_stream_id,
                )
                visited_history.append(f"{last_completed.stream_id}:{last_completed.curr_node}(FINAL_COLLAPSE)")
                join_collapses += 1
                self._active_cursors.clear()

        return TraversalResult(
            final_cursor=last_completed,
            root_hash=root_hash,
            total_ticks=total_ticks,
            reclaimed_fuel=last_completed.fuel,
            visited_history=visited_history,
            parked_cursors=list(self._parked_cursors.values()),
            join_collapses=join_collapses,
        )

    # [Internal Helpers] Barrier Synchronization, Fan-Out Detection & Arbiter
    async def _route_or_collapse_at_barrier(
        self,
        cursor: BranchCursor,
        barrier: Optional[DagJoinBarrier],
        expected_count: int,
        base_stream_id: str,
        run_queue: List[BranchCursor],
        visited_history: List[str],
    ) -> bool:
        if expected_count > 1 and barrier and barrier.is_join_point(cursor.curr_node):
            all_arrived, ready_cursors = barrier.arrive(cursor, expected_count=expected_count)
            if all_arrived:
                for rc in ready_cursors:
                    self._active_cursors.pop(rc.stream_id, None)

                merged = await self.runtime.collapse_and_merge(
                    cursors=ready_cursors,
                    winner_idx=self.arbiter(ready_cursors),
                    merged_stream_id=base_stream_id,
                )
                self._active_cursors[merged.stream_id] = merged
                visited_history.append(f"{merged.stream_id}:{merged.curr_node}(MERGED)")
                run_queue.append(merged)
                return True
            return False

        run_queue.append(cursor)
        return False

    @staticmethod
    def _detect_fanout_targets(
        node_spec: Dict[str, Any], meta_ctx: Dict[str, Any]
    ) -> List[Tuple[str, Dict[str, Any]]]:
        if "_fork_branches" in meta_ctx:
            branches = meta_ctx.pop("_fork_branches")
            if isinstance(branches, list) and len(branches) >= 2:
                return branches

        nx = node_spec.get(CompactKey.NEXT)
        if isinstance(nx, list) and len(nx) >= 2:
            return [(target, {"_branch_idx": idx}) for idx, target in enumerate(nx)]

        return []

    @staticmethod
    def _default_arbiter(cursors: List[BranchCursor]) -> int:
        return max(
            range(len(cursors)),
            key=lambda i: (float(cursors[i].meta_ctx.get("_score", 0.0)), cursors[i].fuel),
        )