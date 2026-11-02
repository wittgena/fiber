# fiber.phase.fsm.compiler
import hashlib
from typing import List, Dict, Any, Optional, Union, Set, Tuple

from xphi.arch.bound.adapter.state import StateAdapter
from xphi.arch.bound.adapter.fsm import FsmAdapter, FsmTarget
from xphi.state.inter.fsm import FSMInterpreter
from xphi.node.runtime.wasm.broker import WasmBroker
from xphi.node.runtime.wasm.quota import QuotaPolicy
from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.contract import (
    MAX_BLOB_BYTES,
    CAS_PREFIX,
    BRANCH_PREFIX,
    END_TOKEN,
    RETURN_TOKEN,
    ASL_CHOICE_OP_MAP,
    NodeType,
    EdgeMode,
    CompactKey,
    ParsedResource,
    parse_resource_uri,
    VCRConfig,
    ProjectionConfig,
    TaskIOSpec,
    AgentAttributes,
    EdgeRelation,
    Fragment,
    FragmentSig,
    CompiledBundle,
)

log = get_emitter("fsm.compiler", phase="COMPILER")

class StateProjector:
    @staticmethod
    def project_compact_blob(signature: FragmentSig) -> Dict[str, Any]:
        graph_specs: Dict[str, Any] = {}

        for frag_id, frag in signature.nodes.items():
            spec: Dict[str, Any] = {CompactKey.TYPE: frag.type.value}

            compact_attrs = frag.attributes.model_dump(exclude_none=True, exclude_defaults=True)
            if compact_attrs:
                spec[CompactKey.ATTRS] = compact_attrs

            cond_edges = [e for e in frag.relations if e.edge_type == EdgeMode.CONDITIONAL]
            direct_edges = [e for e in frag.relations if e.edge_type == EdgeMode.DIRECT]
            fallback_edges = [e for e in frag.relations if e.edge_type == EdgeMode.FALLBACK]

            if fallback_edges:
                spec[CompactKey.FALLBACK] = fallback_edges[0].target

            if frag.type == NodeType.ROUTER:
                spec[CompactKey.RULES] = [
                    {CompactKey.IF_COND: e.condition, CompactKey.NEXT: e.target}
                    for e in cond_edges
                ]
                spec[CompactKey.DEFAULT_NEXT] = (
                    direct_edges[0].target if direct_edges else END_TOKEN
                )
            else:
                if direct_edges:
                    targets = [e.target for e in direct_edges]
                    spec[CompactKey.NEXT] = targets[0] if len(targets) == 1 else targets
                else:
                    spec[CompactKey.NEXT] = END_TOKEN

            graph_specs[frag_id] = spec

        return {
            CompactKey.META: {CompactKey.ENTRY: signature.entry_point, **signature.meta},
            CompactKey.GRAPH: graph_specs,
        }


class FSMCompiler:
    def __init__(self, byte_limit: int = MAX_BLOB_BYTES, use_wasm_linker: bool = True):
        self.byte_limit = byte_limit
        self.use_wasm_linker = use_wasm_linker
        self.projector = StateProjector()
        self._cas_pool: Dict[str, Dict[str, Any]] = {}

    def _inscribe_blob(self, data: Dict[str, Any], prefix: str = CAS_PREFIX) -> str:
        canonical = StateAdapter.to_canonical_bytes(data)
        digest = hashlib.sha256(canonical).hexdigest()[:16]
        ptr = f"{prefix}{digest}"
        self._cas_pool[ptr] = data
        return ptr

    def _build_fragment_sig(self, asl_schema: Dict[str, Any], trace_id: str) -> FragmentSig:
        """Parses ASL JSON, strips heavy Task I/O into CAS (`blob:<hash>`), and builds topology signature."""
        self._cas_pool.clear()
        start_at = asl_schema.get("StartAt", "unknown_entry")
        states = asl_schema.get("States", {})

        # Pre-scan Choice nodes to auto-infer variables required in `Projection.extract_keys`
        auto_extract_keys = self._scan_choice_variables(states)

        sig = FragmentSig(
            entry_point=start_at,
            meta={
                CompactKey.TRACE: trace_id,
                CompactKey.VERSION: asl_schema.get("Version", "1.0"),
            },
        )

        for state_name, state_def in states.items():
            state_type = state_def.get("Type")

            if state_type == "Task":
                resource_uri = state_def.get("Resource", "")
                parsed_uri = parse_resource_uri(resource_uri)

                raw_params = dict(state_def.get("Parameters", {}))
                if parsed_uri.model_hint and "model" not in raw_params:
                    raw_params["model"] = parsed_uri.model_hint

                fiber_ext = dict(raw_params.pop("__fiber_extensions", {}))

                vcr_raw = fiber_ext.pop("VCR", None)
                proj_raw = fiber_ext.pop("Projection", None)
                pressure = float(fiber_ext.pop("pressure", 0.0))
                max_failures = int(fiber_ext.pop("max_failures", 3))
                allow_parallel = bool(fiber_ext.pop("allow_parallel", False))

                # Merge user-specified `extract_keys` with auto-inferred Choice variables
                proj_cfg = self._build_projection_config(proj_raw, auto_extract_keys)

                io_spec = TaskIOSpec(
                    resource_uri=resource_uri,
                    result_path=state_def.get("ResultPath", "$"),
                    call_params=raw_params,
                    vcr=VCRConfig(**vcr_raw) if isinstance(vcr_raw, dict) else None,
                    projection=proj_cfg,
                    extras=fiber_ext,
                ).model_dump(exclude_none=True, exclude_defaults=True)

                io_ref = self._inscribe_blob(io_spec, prefix=CAS_PREFIX) if io_spec else None

                attrs = AgentAttributes(
                    io_ref=io_ref,
                    pressure=pressure,
                    max_failures=max_failures,
                    allow_parallel=allow_parallel,
                )
                frag = Fragment(id=state_name, type=parsed_uri.node_type, attributes=attrs)

                if "Next" in state_def:
                    nxt: Union[str, List[str]] = state_def["Next"]
                    targets = nxt if isinstance(nxt, list) else [nxt]
                    for t in targets:
                        frag.relations.append(EdgeRelation(target=t, edge_type=EdgeMode.DIRECT))

                fb_target = self._extract_catch_target(state_def.get("Catch"))
                if fb_target:
                    frag.relations.append(EdgeRelation(target=fb_target, edge_type=EdgeMode.FALLBACK))

                sig.nodes[state_name] = frag

            elif state_type == "Choice":
                frag = Fragment(id=state_name, type=NodeType.ROUTER)
                for choice in state_def.get("Choices", []):
                    cond_str = self._parse_choice_condition(choice)
                    frag.relations.append(
                        EdgeRelation(
                            target=choice["Next"],
                            edge_type=EdgeMode.CONDITIONAL,
                            condition=cond_str,
                        )
                    )
                if "Default" in state_def:
                    frag.relations.append(EdgeRelation(target=state_def["Default"], edge_type=EdgeMode.DIRECT))
                sig.nodes[state_name] = frag

            elif state_type in ("Succeed", "Fail"):
                frag = Fragment(id=state_name, type=NodeType.ACT)
                frag.relations.append(EdgeRelation(target=END_TOKEN, edge_type=EdgeMode.DIRECT))
                sig.nodes[state_name] = frag

        self._validate_phase(sig)
        return sig

    @classmethod
    def _scan_choice_variables(cls, states: Dict[str, Any]) -> List[str]:
        """Collects all variables referenced in Choice rules so Tasks can project them automatically."""
        keys: Set[str] = {"$._score"}
        for s_def in states.values():
            if s_def.get("Type") == "Choice":
                for c in s_def.get("Choices", []):
                    if "Variable" in c:
                        keys.add(str(c["Variable"]).strip())
                    elif "if" in c:
                        cond = cls._normalize_if_expr(str(c["if"]))
                        for op in (">=", "<=", "!=", "==", ">", "<"):
                            if op in cond:
                                lhs = cond.split(op, 1)[0].strip()
                                keys.add(lhs if lhs.startswith("$.") else f"$.{lhs}")
                                break
        return sorted(keys)

    @staticmethod
    def _build_projection_config(
        proj_raw: Optional[Dict[str, Any]], auto_keys: List[str]
    ) -> Optional[ProjectionConfig]:
        offload_path = None
        merged_keys: List[str] = []

        if isinstance(proj_raw, dict):
            offload_path = proj_raw.get("offload_path")
            for k in proj_raw.get("extract_keys", []):
                if k not in merged_keys:
                    merged_keys.append(k)

        for ak in auto_keys:
            if ak not in merged_keys:
                merged_keys.append(ak)

        if not offload_path and not merged_keys:
            return None
        return ProjectionConfig(offload_path=offload_path, extract_keys=merged_keys)

    @staticmethod
    def _extract_catch_target(catch_spec: Any) -> Optional[str]:
        if isinstance(catch_spec, str) and catch_spec.strip():
            return catch_spec.strip()
        if isinstance(catch_spec, list) and catch_spec:
            first = catch_spec[0]
            if isinstance(first, dict):
                return first.get("Next")
        if isinstance(catch_spec, dict):
            return catch_spec.get("Next")
        return None

    @staticmethod
    def _normalize_if_expr(expr: str) -> str:
        """Normalizes shorthand `"if": "$._score >= 0.95"` into compact `"$._score>=0.95"`."""
        for op in (">=", "<=", "!=", "==", ">", "<"):
            if op in expr:
                lhs, rhs = expr.split(op, 1)
                lhs_clean = lhs.strip()
                rhs_clean = rhs.strip().strip("'\"")
                return f"{lhs_clean}{op}{rhs_clean}"
        return expr.strip()

    @classmethod
    def _parse_choice_condition(cls, choice: Dict[str, Any]) -> str:
        if "if" in choice:
            return cls._normalize_if_expr(str(choice["if"]))

        var = choice.get("Variable", "")
        for asl_op, sym_op in ASL_CHOICE_OP_MAP.items():
            if asl_op in choice:
                val = choice[asl_op]
                val_str = ("true" if val else "false") if isinstance(val, bool) else str(val)
                return f"{var}{sym_op}{val_str}"
        return f"{var}=="

    @classmethod
    def _parse_resource_uri(cls, resource_uri: str) -> Tuple[NodeType, Optional[str]]:
        """Backward-compatible wrapper delegating to `contract.parse_resource_uri`."""
        parsed = parse_resource_uri(resource_uri)
        return parsed.node_type, parsed.model_hint

    def _finalize_bundle_from_wasm(self, wasm_output_str: str) -> CompiledBundle:
        """Unpacks `fsm.wasm` compile output and merges partitioned sub-branches into `cas_store`."""
        parsed = FsmAdapter.parse_broker_output(wasm_output_str)
        if not parsed.get("success", False):
            reason = parsed.get("revert_reason") or "Unknown WASM Compile Error"
            if "E_BLOB_OVERFLOW" in reason:
                raise OverflowError(reason)
            raise ValueError(f"[FSM Static Sandbox Rejected] {reason}")

        root_hash = parsed["state_root"]
        state_diff = parsed.get("state_diff", {})
        inner_out = parsed.get("parsed_output", {})

        for branch_key, branch_blob in state_diff.items():
            self._cas_pool[branch_key] = branch_blob

        root_blob = state_diff.get(root_hash, {})
        raw_bytes = StateAdapter.to_canonical_bytes(root_blob)
        total_root_bytes = int(inner_out.get("total_root_bytes", len(raw_bytes)))

        if total_root_bytes > self.byte_limit:
            raise OverflowError(
                f"Compiled FSM graph ({total_root_bytes}B) exceeds {self.byte_limit}B limit."
            )

        return CompiledBundle(
            root_hash=root_hash,
            root_blob=root_blob,
            cas_store=dict(self._cas_pool),
            total_root_bytes=total_root_bytes,
        )

    def _fallback_python_compile(self, raw_ir: Dict[str, Any]) -> CompiledBundle:
        """Fallback compiler when `fsm.wasm` binary is not yet built or disabled."""
        raw_bytes = StateAdapter.to_canonical_bytes(raw_ir)
        if len(raw_bytes) > self.byte_limit:
            raise OverflowError(
                f"Compiled FSM graph ({len(raw_bytes)}B) exceeds {self.byte_limit}B limit. "
                f"Modularize sub-branches via '{BRANCH_PREFIX}<hash>' targets."
            )
        root_hash = self._inscribe_blob(raw_ir, prefix=BRANCH_PREFIX)
        return CompiledBundle(
            root_hash=root_hash,
            root_blob=raw_ir,
            cas_store=dict(self._cas_pool),
            total_root_bytes=len(raw_bytes),
        )

    def compile_from_asl(self, asl_schema: Dict[str, Any], trace_id: str = "turn_0") -> CompiledBundle:
        sig = self._build_fragment_sig(asl_schema, trace_id)
        raw_ir = self.projector.project_compact_blob(sig)

        if self.use_wasm_linker and self.byte_limit == MAX_BLOB_BYTES:
            try:
                with FSMInterpreter(wasm_module_name="fsm.wasm", policy=QuotaPolicy.system()) as fsm_vm:
                    res = fsm_vm.execute(
                        vm_target=FsmTarget.COMPILE,
                        target_address=trace_id,
                        calldata=raw_ir,
                        state_snapshot={},
                        context={"fuel": 2000},
                    )
                    if res.output:
                        return self._finalize_bundle_from_wasm(res.output)
                    raise ValueError(f"FSM WASM Compile Failed: {res.error}")
            except (FileNotFoundError, ImportError) as e:
                log.warning(f"fsm.wasm unavailable ({e}), falling back to Python projector.")

        return self._fallback_python_compile(raw_ir)

    async def acompile_from_asl(self, asl_schema: Dict[str, Any], trace_id: str = "turn_0") -> CompiledBundle:
        """[Async] Distributed compilation and static verification via WasmBroker & TaskWasm daemon."""
        sig = self._build_fragment_sig(asl_schema, trace_id)
        raw_ir = self.projector.project_compact_blob(sig)

        payload = FsmAdapter.build_compile_payload(trace_id=trace_id, compact_ir=raw_ir)
        res = await WasmBroker.get_shared().execute(code=payload, context={"fuel": 2000})
        if res.output:
            return self._finalize_bundle_from_wasm(res.output)
        raise ValueError(f"FSM Broker Compile Failed: {res.error}")

    @staticmethod
    def _validate_phase(signature: FragmentSig) -> None:
        if signature.entry_point not in signature.nodes:
            raise ValueError(f"Entry point '{signature.entry_point}' is missing in nodes.")
        for frag_id, fragment in signature.nodes.items():
            for edge in fragment.relations:
                if not edge.is_external_or_terminal and edge.target not in signature.nodes:
                    raise ValueError(f"Fragment '{frag_id}' points to unknown target '{edge.target}'.")