# fiber.phase.fsm.runtime
## @lineage: fiber.dev.ex.fsm.runtime
import json
from dataclasses import dataclass, field, replace
from typing import Dict, Set, List, Optional, Tuple, Any

from xphi.arch.bound.adapter.pta import (
    PtaAdapter,
    PtaTransaction,
    PtaInput,
    PtaPointer,
    PhaseAnchorOutput,
    NodeSigner,
)
from xphi.arch.bound.adapter.state import StateAdapter
from xphi.state.anchor.store import PhaseStore, ToposBlob
from xphi.node.runtime.wasm.broker import DphiBroker
from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.compiler import (
    ExCompiledBundle,
    CompactKey,
    NodeType,
    CAS_PREFIX,
    BRANCH_PREFIX,
    END_TOKEN,
    RETURN_TOKEN,
    MAX_BLOB_BYTES,
)

log = get_emitter("fsm.runtime", phase="BRANCH_RUNTIME")


# ============================================================================
# 1. DAG TOPOLOGY & JOIN BARRIER
# ============================================================================
@dataclass
class DagTopology:
    entry_point: str
    back_edges: Set[Tuple[str, str]] = field(default_factory=set)
    join_nodes: Set[str] = field(default_factory=set)


class DagJoinBarrier:
    """Synchronizes parallel branches at shared DAG join nodes (forward in-degree >= 2)."""

    def __init__(self, root_blob: Dict[str, Any]):
        self.topology = self._analyze_blob(root_blob)
        self._waiting: Dict[str, List["BranchCursor"]] = {jn: [] for jn in self.topology.join_nodes}

    @staticmethod
    def _analyze_blob(blob: Dict[str, Any]) -> DagTopology:
        entry = blob.get(CompactKey.META, {}).get(CompactKey.ENTRY, "")
        graph = blob.get(CompactKey.GRAPH, {})
        in_edges: Dict[str, Set[str]] = {nid: set() for nid in graph}
        out_edges: Dict[str, Set[str]] = {nid: set() for nid in graph}

        for nid, spec in graph.items():
            if spec.get(CompactKey.TYPE) == NodeType.ROUTER.value:
                targets = [r[CompactKey.NEXT] for r in spec.get(CompactKey.RULES, [])]
                if spec.get(CompactKey.DEFAULT_NEXT):
                    targets.append(spec[CompactKey.DEFAULT_NEXT])
            else:
                nx = spec.get(CompactKey.NEXT, [])
                targets = nx if isinstance(nx, list) else [nx]

            for t in targets:
                if t in graph:
                    out_edges[nid].add(t)
                    in_edges[t].add(nid)

        color: Dict[str, int] = {nid: 0 for nid in graph}
        back_edges: Set[Tuple[str, str]] = set()

        def _dfs(u: str) -> None:
            color[u] = 1
            for v in out_edges.get(u, set()):
                if color.get(v, 0) == 1:
                    back_edges.add((u, v))
                elif color.get(v, 0) == 0:
                    _dfs(v)
            color[u] = 2

        if entry in color:
            _dfs(entry)

        join_nodes = {
            nid for nid, preds in in_edges.items()
            if len({p for p in preds if (p, nid) not in back_edges}) >= 2
        }
        return DagTopology(entry_point=entry, back_edges=back_edges, join_nodes=join_nodes)

    def is_join_point(self, node_id: str) -> bool:
        return node_id in self.topology.join_nodes

    def arrive(self, cursor: "BranchCursor", expected_count: int) -> Tuple[bool, List["BranchCursor"]]:
        bucket = self._waiting.setdefault(cursor.curr_node, [])
        if all(c.stream_id != cursor.stream_id for c in bucket):
            bucket.append(cursor)
        if len(bucket) >= expected_count:
            ready = list(bucket)
            bucket.clear()
            return True, ready
        return False, []

    def drain_waiting(self, node_id: Optional[str] = None) -> List["BranchCursor"]:
        """Collects and clears cursors waiting at a specific join node or across all join barriers."""
        if node_id is not None:
            bucket = self._waiting.get(node_id, [])
            stranded = list(bucket)
            bucket.clear()
            return stranded

        stranded: List["BranchCursor"] = []
        for bucket in self._waiting.values():
            stranded.extend(bucket)
            bucket.clear()
        return stranded


# ============================================================================
# 2. SELF-CONTAINED BRANCH CURSOR (Domain Model with PTA Serialization)
# ============================================================================
@dataclass
class BranchCursor:
    """Active branch state handle with built-in PTA input signing and output projection."""
    stream_id: str
    pointer: PtaPointer
    active_blob_hash: str
    curr_node: str
    fuel: int
    owner_pubkey: str
    meta_ctx: Dict[str, Any] = field(default_factory=dict)
    call_stack: List[str] = field(default_factory=list)
    tick: int = 0

    @property
    def is_terminated(self) -> bool:
        return self.curr_node == END_TOKEN

    def to_signed_input(self, signer: NodeSigner) -> PtaInput:
        canonical = StateAdapter.to_canonical_bytes({"payload": self.pointer.to_key()})
        return PtaInput(
            pointer=self.pointer,
            signature=signer.sign_payload(canonical),
            owner_address=self.owner_pubkey,
        )

    def to_anchor_output(self, status: str) -> PhaseAnchorOutput:
        payload: Dict[str, Any] = {
            "blob_ref": self.active_blob_hash,
            "node": self.curr_node,
            "meta": self.meta_ctx,
        }
        if self.call_stack:
            payload["stack"] = self.call_stack

        raw_bytes = StateAdapter.to_canonical_bytes(payload)
        if len(raw_bytes) > MAX_BLOB_BYTES:
            raise OverflowError(f"Anchor payload ({len(raw_bytes)}B) exceeds {MAX_BLOB_BYTES}B limit.")
        return PhaseAnchorOutput(
            amount=self.fuel,
            owner=self.owner_pubkey,
            handle_id=self.stream_id,
            phase_status=status,
            executable_payload=payload,
        )

    @classmethod
    def from_anchor_output(
        cls,
        pointer: PtaPointer,
        output: PhaseAnchorOutput,
        fallback_stream_id: str = "",
        tick: int = 1,
    ) -> "BranchCursor":
        payload = output.executable_payload
        return cls(
            stream_id=output.handle_id or fallback_stream_id,
            pointer=pointer,
            active_blob_hash=payload["blob_ref"],
            curr_node=payload["node"],
            fuel=output.amount,
            owner_pubkey=output.owner,
            meta_ctx=dict(payload.get("meta", {})),
            call_stack=list(payload.get("stack", [])),
            tick=tick,
        )


# ============================================================================
# 3. LEAN BRANCH CONTROLLER (Unified M:N PTA Engine & Checkpoint Rehydration)
# ============================================================================
class ExBranchController:
    """
    Executes 2KB FSM/DAG blobs via a single unified M-inputs-to-N-outputs PTA commit engine.
    Handles Genesis (0:1), Step (1:1), Fork (1:N), Merge (N:1), and O(1) Cursor Rehydration.
    """

    def __init__(self, broker: Optional[DphiBroker] = None):
        self.broker = broker or DphiBroker.get_shared()
        self.store = PhaseStore()
        self.pta = PtaAdapter(broker=self.broker)
        self.signer = NodeSigner.get_instance()
        self._blob_cache: Dict[str, Dict[str, Any]] = {}
        self.barrier: Optional[DagJoinBarrier] = None

    # ------------------------------------------------------------------------
    # [Core Engine] Unified M:N PTA Transaction Executor
    # ------------------------------------------------------------------------
    async def _commit_pta_transition(
        self,
        spent_cursors: List[BranchCursor],
        next_cursors: List[BranchCursor],
        phase_status: str,
        primary_stream_id: str,
        meta: Dict[str, Any],
    ) -> List[BranchCursor]:
        inputs = [c.to_signed_input(self.signer) for c in spent_cursors]
        outputs = [c.to_anchor_output(phase_status) for c in next_cursors]

        tx = PtaTransaction(inputs=inputs, outputs=outputs, metadata=meta)
        tx_hash = await self.pta.execute_transaction(tx, stream_id=primary_stream_id)
        commit_hash = self.pta._resolve_commit_hash(tx_hash)

        committed: List[BranchCursor] = []
        for idx, cursor in enumerate(next_cursors):
            self.store.update_head(cursor.stream_id, commit_hash)
            committed.append(replace(cursor, pointer=PtaPointer(tx_hash=tx_hash, output_index=idx)))
        return committed

    # ------------------------------------------------------------------------
    # [Public API] Spawn (0:1), Step (1:1), Fork (1:N), Collapse (N:1), Restore
    # ------------------------------------------------------------------------
    async def mount_bundle_and_spawn(
        self,
        bundle: ExCompiledBundle,
        base_stream_id: str,
        initial_fuel: int = 1000,
        initial_meta: Optional[Dict[str, Any]] = None,
    ) -> BranchCursor:
        for ptr_key, blob_data in bundle.cas_store.items():
            raw_hash = self.store._put_object("blob", blob_data)
            self._blob_cache[ptr_key] = blob_data
            self._blob_cache[f"{CAS_PREFIX}{raw_hash[:16]}"] = blob_data

        raw_root_hash = self.store._put_object("blob", bundle.root_blob)
        self._blob_cache[bundle.root_hash] = bundle.root_blob
        self._blob_cache[f"{BRANCH_PREFIX}{raw_root_hash[:16]}"] = bundle.root_blob

        self.barrier = DagJoinBarrier(bundle.root_blob)
        entry_node = bundle.root_blob[CompactKey.META][CompactKey.ENTRY]

        draft = BranchCursor(
            stream_id=base_stream_id,
            pointer=PtaPointer("", 0),
            active_blob_hash=bundle.root_hash,
            curr_node=entry_node,
            fuel=initial_fuel,
            owner_pubkey=self.signer.pubkey_hex,
            meta_ctx=dict(initial_meta or {}),
            tick=1,
        )
        res = await self._commit_pta_transition(
            spent_cursors=[],
            next_cursors=[draft],
            phase_status="GENESIS",
            primary_stream_id=base_stream_id,
            meta={"stream_id": base_stream_id, "event": "GENESIS_SPAWN"},
        )
        return res[0]

    async def step(
        self,
        cursor: BranchCursor,
        step_cost: int = 10,
        meta_updates: Optional[Dict[str, Any]] = None,
        offload_data: Optional[Dict[str, Any]] = None,
    ) -> BranchCursor:
        if cursor.is_terminated:
            return cursor
        if cursor.fuel < step_cost:
            raise RuntimeError(f"[Runtime:{cursor.stream_id}] Exhausted fuel ({cursor.fuel} < {step_cost})")

        meta = {**cursor.meta_ctx, **(meta_updates or {})}
        if offload_data:
            offload_hash = self.store._put_object("blob", offload_data)
            meta["_last_ptr"] = f"{CAS_PREFIX}{offload_hash[:16]}"

        node_spec = self.get_node_spec(cursor)
        raw_target = self._evaluate_next_target(node_spec, meta)
        next_blob, next_node, rule_kind, next_stack = self._resolve_target(cursor, raw_target, node_spec)

        await self._verify_wasm_transition(cursor, raw_target, rule_kind)

        draft = replace(
            cursor,
            active_blob_hash=next_blob,
            curr_node=next_node,
            fuel=cursor.fuel - step_cost,
            meta_ctx=meta,
            call_stack=next_stack,
            tick=cursor.tick + 1,
        )
        status = "SEALED" if draft.is_terminated else "ACTIVE"
        res = await self._commit_pta_transition(
            spent_cursors=[cursor],
            next_cursors=[draft],
            phase_status=status,
            primary_stream_id=cursor.stream_id,
            meta={"stream_id": cursor.stream_id, "from": cursor.curr_node, "to": next_node, "kind": rule_kind},
        )
        return res[0]

    async def fork(
        self,
        parent: BranchCursor,
        branch_targets: List[Tuple[str, Dict[str, Any]]],
        fuel_ratios: Optional[List[float]] = None,
    ) -> List[BranchCursor]:
        if len(branch_targets) < 2:
            raise ValueError("fork() requires at least 2 branch targets.")

        # Safely allow Time-Travel Forking even if parent cursor is currently at END_TOKEN
        node_spec = {} if parent.is_terminated else self.get_node_spec(parent)
        allocations = self._slice_fuel(parent.fuel, len(branch_targets), fuel_ratios)
        drafts: List[BranchCursor] = []

        for idx, ((raw_target, b_meta), alloc_fuel) in enumerate(zip(branch_targets, allocations)):
            next_blob, next_node, _, next_stack = self._resolve_target(parent, raw_target, node_spec)
            drafts.append(
                replace(
                    parent,
                    stream_id=f"{parent.stream_id}/b{idx}",
                    active_blob_hash=next_blob,
                    curr_node=next_node,
                    fuel=alloc_fuel,
                    meta_ctx={**parent.meta_ctx, **b_meta},
                    call_stack=next_stack,
                    tick=parent.tick + 1,
                )
            )

        return await self._commit_pta_transition(
            spent_cursors=[parent],
            next_cursors=drafts,
            phase_status="FORKED",
            primary_stream_id=parent.stream_id,
            meta={"event": "MULTIVERSE_FORK", "parent_stream": parent.stream_id, "fan_out": len(drafts)},
        )

    async def collapse_and_merge(
        self,
        cursors: List[BranchCursor],
        winner_idx: int,
        merged_stream_id: str,
    ) -> BranchCursor:
        if not cursors or not (0 <= winner_idx < len(cursors)):
            raise ValueError("Invalid cursors or winner_idx for merge.")

        winner = cursors[winner_idx]
        reclaimed_fuel = sum(c.fuel for c in cursors)
        draft = replace(
            winner,
            stream_id=merged_stream_id,
            fuel=reclaimed_fuel,
            meta_ctx=dict(winner.meta_ctx),
            call_stack=list(winner.call_stack),
            tick=winner.tick + 1,
        )

        res = await self._commit_pta_transition(
            spent_cursors=cursors,
            next_cursors=[draft],
            phase_status="MERGED",
            primary_stream_id=merged_stream_id,
            meta={"event": "MULTIVERSE_COLLAPSE", "winner": winner.stream_id, "reclaimed_fuel": reclaimed_fuel},
        )
        return res[0]

    def restore_cursor(self, pointer_or_stream: str) -> BranchCursor:
        """
        Rehydrates a live BranchCursor in O(1) directly from an unspent PTA pointer ('tx_hash:idx')
        or a PhaseStore branch head ('ref:<stream_id>') without re-running prior nodes.
        """
        if ":" in pointer_or_stream and pointer_or_stream in self.pta._unfold_pool:
            tx_hash, idx_str = pointer_or_stream.split(":", 1)
            out: PhaseAnchorOutput = self.pta._unfold_pool[pointer_or_stream]
            cursor = BranchCursor.from_anchor_output(
                pointer=PtaPointer(tx_hash=tx_hash, output_index=int(idx_str)),
                output=out,
            )
            if self.barrier is None:
                self.barrier = DagJoinBarrier(self._load_blob(cursor.active_blob_hash))
            return cursor

        commit_hash = self.store.get_head_hash(pointer_or_stream)
        if not commit_hash:
            raise KeyError(f"[Runtime] Unknown stream or PTA pointer: '{pointer_or_stream}'")

        raw_commit = json.loads(self.store.db[f"commit:{commit_hash}".encode("utf-8")].decode("utf-8"))
        tx_dict = raw_commit["executable_payload"]
        tx_hash = tx_dict["tx_hash"]

        for idx, out_dict in enumerate(tx_dict["outputs"]):
            if out_dict.get("handle_id") == pointer_or_stream or len(tx_dict["outputs"]) == 1:
                out = PhaseAnchorOutput(**out_dict)
                cursor = BranchCursor.from_anchor_output(
                    pointer=PtaPointer(tx_hash=tx_hash, output_index=idx),
                    output=out,
                    fallback_stream_id=pointer_or_stream,
                )
                if self.barrier is None:
                    self.barrier = DagJoinBarrier(self._load_blob(cursor.active_blob_hash))
                return cursor

        raise ValueError(f"[Runtime] Output for '{pointer_or_stream}' not found in commit '{commit_hash[:8]}'")

    # ------------------------------------------------------------------------
    # [Inspection & Helpers] Spec Dereferencing, Topology, WASM & Fuel Slicing
    # ------------------------------------------------------------------------
    def get_node_spec(self, cursor: BranchCursor) -> Dict[str, Any]:
        blob = self._load_blob(cursor.active_blob_hash)
        spec = blob.get(CompactKey.GRAPH, {}).get(cursor.curr_node)
        if not spec:
            raise KeyError(f"Node '{cursor.curr_node}' missing in blob '{cursor.active_blob_hash}'")
        return spec

    def resolve_task_io(self, node_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Dereferences off-ledger `io_ref` (`TaskIOSpec`) from CAS cache/store when needed by workers."""
        io_ref = node_spec.get(CompactKey.ATTRS, {}).get("io_ref")
        return self._load_blob(io_ref) if io_ref else {}

    def _load_blob(self, blob_hash: str) -> Dict[str, Any]:
        if blob_hash not in self._blob_cache:
            clean_hash = blob_hash.replace(BRANCH_PREFIX, "").replace(CAS_PREFIX, "")
            key = f"blob:{clean_hash}".encode("utf-8")
            if key not in self.store.db:
                raise KeyError(f"[Runtime] CAS Blob '{blob_hash}' not found.")
            self._blob_cache[blob_hash] = json.loads(self.store.db[key].decode("utf-8"))
        return self._blob_cache[blob_hash]

    @staticmethod
    def _evaluate_next_target(node_spec: Dict[str, Any], meta_ctx: Dict[str, Any]) -> str:
        if node_spec.get(CompactKey.TYPE) == NodeType.ROUTER.value:
            for rule in node_spec.get(CompactKey.RULES, []):
                cond = rule.get(CompactKey.IF_COND, "")
                if "==" in cond:
                    var, expected = cond.split("==", 1)
                    if str(meta_ctx.get(var, meta_ctx.get(var.lstrip("$."), ""))) == expected:
                        return rule[CompactKey.NEXT]
            return node_spec.get(CompactKey.DEFAULT_NEXT, END_TOKEN)

        nx = node_spec.get(CompactKey.NEXT, END_TOKEN)
        return nx[0] if isinstance(nx, list) and nx else str(nx)

    def _resolve_target(
        self, cursor: BranchCursor, raw_target: str, node_spec: Dict[str, Any]
    ) -> Tuple[str, str, str, List[str]]:
        stack = list(cursor.call_stack)

        if (
            raw_target == RETURN_TOKEN
            or raw_target.startswith(f"{RETURN_TOKEN}#")
            or (raw_target == END_TOKEN and stack)
        ):
            if not stack:
                return cursor.active_blob_hash, END_TOKEN, "ANCHOR", stack
            parent_blob, ret_node = stack.pop().split("#", 1)
            if "#" in raw_target:
                ret_node = raw_target.split("#", 1)[1]
            if ret_node == END_TOKEN and not stack:
                return parent_blob, END_TOKEN, "ANCHOR", stack
            return parent_blob, ret_node, "SYMLINK", stack

        if raw_target == END_TOKEN:
            return cursor.active_blob_hash, END_TOKEN, "ANCHOR", stack

        if raw_target.startswith((BRANCH_PREFIX, CAS_PREFIX)):
            if "#" in raw_target:
                target_blob, target_node = raw_target.split("#", 1)
            else:
                target_blob = raw_target
                target_node = self._load_blob(target_blob)[CompactKey.META][CompactKey.ENTRY]

            resume_node = (
                node_spec.get(CompactKey.FALLBACK)
                or node_spec.get(CompactKey.DEFAULT_NEXT)
                or END_TOKEN
            )
            stack.append(f"{cursor.active_blob_hash}#{resume_node}")
            return target_blob, target_node, "SYMLINK", stack

        return cursor.active_blob_hash, raw_target, "CORE", stack

    async def _verify_wasm_transition(self, cursor: BranchCursor, dest_node: str, rule_kind: str) -> None:
        if not (self.broker and hasattr(self.broker, "invoke")):
            return

        trans_rule = StateAdapter.build_trans_rule(src=cursor.curr_node, dest=dest_node, kind=rule_kind)
        child_node = (
            StateAdapter.build_symlink_node(cursor.curr_node, dest_node)
            if rule_kind == "SYMLINK"
            else StateAdapter.build_core_node(cursor.curr_node, rule_kind)
        )
        phase_root = StateAdapter.build_core_node(
            name=f"root_{cursor.stream_id}",
            content=cursor.active_blob_hash,
            children={cursor.curr_node: child_node},
        )
        payload = StateAdapter.build_transition_payload(
            intent_action="FSM_BRANCH_STEP",
            intent_payload={"from": cursor.curr_node, "to": dest_node, "tick": cursor.tick},
            evolution_ctx=StateAdapter.build_evolution_context(phase_root, [trans_rule]),
        )
        try:
            res = await self.broker.invoke(
                "execute_transition",
                StateAdapter.to_canonical_bytes(payload).decode("utf-8"),
            )
            if hasattr(res, "success") and not res.success:
                err = str(getattr(res, "error", ""))
                self.store.save_transition(ToposBlob("wasm.reject", cursor.curr_node, dest_node, 1.0, err))
                raise PermissionError(f"[WASM Rejected Transition] {err}")
        except NotImplementedError:
            pass

    @staticmethod
    def _slice_fuel(total_fuel: int, count: int, ratios: Optional[List[float]] = None) -> List[int]:
        if not ratios:
            base, rem = divmod(total_fuel, count)
            return [base + (1 if i < rem else 0) for i in range(count)]
        total_ratio = sum(ratios)
        sliced = [int(total_fuel * (r / total_ratio)) for r in ratios]
        sliced[0] += total_fuel - sum(sliced)
        return sliced