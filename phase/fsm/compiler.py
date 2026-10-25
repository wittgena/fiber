# fiber.phase.fsm.compiler
import hashlib
from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, Field

from xphi.arch.bound.adapter.state import StateAdapter
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("fsm.compiler", phase="COMPILER")

MAX_BLOB_BYTES = 2048
CAS_PREFIX = "blob:"
BRANCH_PREFIX = "branch:"
END_TOKEN = "END"
RETURN_TOKEN = "RETURN"


# VOCAB & COMPACT KEYS (Unified NodeType & Terminal Tokens)
class NodeType(str, Enum):
    PROJECTOR = "projector"
    OPERSIG = "opersig"
    ACT = "act"
    PENDING = "pending"
    REFLECT = "reflect"
    TENSION = "tension"
    ROUTER = "router"


class EdgeMode(str, Enum):
    DIRECT = "direct"
    CONDITIONAL = "conditional"
    FALLBACK = "fallback"


class CompactKey:
    META = "_m"
    ENTRY = "ep"
    TRACE = "tr"
    VERSION = "v"
    GRAPH = "g"
    TYPE = "t"
    ATTRS = "a"
    NEXT = "nx"
    FALLBACK = "fb"
    RULES = "rl"
    IF_COND = "if"
    DEFAULT_NEXT = "dn"


# ============================================================================
# 2. OPTIMAL SCHEMAS (Off-Ledger Task IO vs On-Ledger Pointer IR)
# ============================================================================
class VCRConfig(BaseModel):
    capture_mode: str = Field(default="rpc_payload")
    coalescing_window_ms: int = Field(default=0, ge=0)


class PTAMintConfig(BaseModel):
    pointer_target: Optional[str] = None
    meta_projection: List[str] = Field(default_factory=list)


class TaskIOSpec(BaseModel):
    """Heavy I/O & extension config stripped from the graph and stored in CAS (`blob:<hash>`)."""
    resource_uri: str = ""
    result_path: str = "$"
    call_params: Dict[str, Any] = Field(default_factory=dict)
    vcr: Optional[VCRConfig] = None
    pta_mint: Optional[PTAMintConfig] = None
    extras: Dict[str, Any] = Field(default_factory=dict)


class AgentAttributes(BaseModel):
    """Lightweight on-ledger node attributes. Defaults are omitted during projection."""
    io_ref: Optional[str] = None  # Points to serialized TaskIOSpec in cas_store (`blob:<hash>`)
    pressure: float = Field(default=0.0, ge=0.0, le=1.0)
    max_failures: int = Field(default=3, ge=1)
    allow_parallel: bool = False


class EdgeRelation(BaseModel):
    target: str
    edge_type: EdgeMode = EdgeMode.DIRECT
    condition: Optional[str] = None

    @property
    def is_external_or_terminal(self) -> bool:
        return (
            self.target in (END_TOKEN, RETURN_TOKEN)
            or self.target.startswith((CAS_PREFIX, BRANCH_PREFIX, f"{RETURN_TOKEN}#"))
        )


class Fragment(BaseModel):
    id: str
    type: NodeType
    attributes: AgentAttributes = Field(default_factory=AgentAttributes)
    relations: List[EdgeRelation] = Field(default_factory=list)


class FragmentSig(BaseModel):
    entry_point: str
    nodes: Dict[str, Fragment] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)


class ExCompiledBundle(BaseModel):
    """Final compiled artifact consumed directly by ExBranchController and ExGraphTraverser."""
    root_hash: str
    root_blob: Dict[str, Any]
    cas_store: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    total_root_bytes: int = 0


# ============================================================================
# 3. COMPILER & PROJECTOR (ASL -> CAS Stripping -> 2KB Compact Blob)
# ============================================================================
class ExStateProjector:
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


class ExFSMCompiler:
    def __init__(self, byte_limit: int = MAX_BLOB_BYTES):
        self.byte_limit = byte_limit
        self.projector = ExStateProjector()
        self._cas_pool: Dict[str, Dict[str, Any]] = {}

    def _inscribe_blob(self, data: Dict[str, Any], prefix: str = CAS_PREFIX) -> str:
        canonical = StateAdapter.to_canonical_bytes(data)
        digest = hashlib.sha256(canonical).hexdigest()[:16]
        ptr = f"{prefix}{digest}"
        self._cas_pool[ptr] = data
        return ptr

    def compile_from_asl(self, asl_schema: Dict[str, Any], trace_id: str = "turn_0") -> ExCompiledBundle:
        self._cas_pool.clear()
        start_at = asl_schema.get("StartAt", "unknown_entry")
        states = asl_schema.get("States", {})

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
                node_type = self._infer_type_from_resource(resource_uri)

                raw_params = dict(state_def.get("Parameters", {}))
                fiber_ext = dict(raw_params.pop("__fiber_extensions", {}))

                vcr_raw = fiber_ext.pop("VCR", None)
                pta_mint_raw = fiber_ext.pop("PTA_Mint", None)
                pressure = float(fiber_ext.pop("pressure", 0.0))
                max_failures = int(fiber_ext.pop("max_failures", 3))
                allow_parallel = bool(fiber_ext.pop("allow_parallel", False))

                # Offload heavy I/O parameters & extensions to CAS blob
                io_spec = TaskIOSpec(
                    resource_uri=resource_uri,
                    result_path=state_def.get("ResultPath", "$"),
                    call_params=raw_params,
                    vcr=VCRConfig(**vcr_raw) if isinstance(vcr_raw, dict) else None,
                    pta_mint=PTAMintConfig(**pta_mint_raw) if isinstance(pta_mint_raw, dict) else None,
                    extras=fiber_ext,
                ).model_dump(exclude_none=True, exclude_defaults=True)

                io_ref = self._inscribe_blob(io_spec, prefix=CAS_PREFIX) if io_spec else None

                attrs = AgentAttributes(
                    io_ref=io_ref,
                    pressure=pressure,
                    max_failures=max_failures,
                    allow_parallel=allow_parallel,
                )
                frag = Fragment(id=state_name, type=node_type, attributes=attrs)

                if "Next" in state_def:
                    nxt: Union[str, List[str]] = state_def["Next"]
                    targets = nxt if isinstance(nxt, list) else [nxt]
                    for t in targets:
                        frag.relations.append(EdgeRelation(target=t, edge_type=EdgeMode.DIRECT))

                if "Catch" in state_def and state_def["Catch"]:
                    fb_target = state_def["Catch"][0].get("Next")
                    if fb_target:
                        frag.relations.append(EdgeRelation(target=fb_target, edge_type=EdgeMode.FALLBACK))

                sig.nodes[state_name] = frag

            elif state_type == "Choice":
                frag = Fragment(id=state_name, type=NodeType.ROUTER)
                for choice in state_def.get("Choices", []):
                    var = choice.get("Variable", "")
                    val = choice.get("StringEquals", choice.get("NumericEquals", ""))
                    frag.relations.append(
                        EdgeRelation(
                            target=choice["Next"],
                            edge_type=EdgeMode.CONDITIONAL,
                            condition=f"{var}=={val}",
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

        root_blob = self.projector.project_compact_blob(sig)
        raw_bytes = StateAdapter.to_canonical_bytes(root_blob)
        if len(raw_bytes) > self.byte_limit:
            raise OverflowError(
                f"Compiled FSM graph ({len(raw_bytes)}B) exceeds {self.byte_limit}B limit. "
                f"Modularize sub-branches via '{BRANCH_PREFIX}<hash>' targets."
            )

        root_hash = self._inscribe_blob(root_blob, prefix=BRANCH_PREFIX)
        return ExCompiledBundle(
            root_hash=root_hash,
            root_blob=root_blob,
            cas_store=dict(self._cas_pool),
            total_root_bytes=len(raw_bytes),
        )

    @staticmethod
    def _infer_type_from_resource(resource_uri: str) -> NodeType:
        uri = resource_uri.lower()
        if "reflect" in uri:
            return NodeType.REFLECT
        if "tension" in uri:
            return NodeType.TENSION
        if "pending" in uri:
            return NodeType.PENDING
        if "llm" in uri or "interllm" in uri:
            return NodeType.PROJECTOR
        if "sidecar" in uri or "tool" in uri or "mcp" in uri:
            return NodeType.OPERSIG
        if "io" in uri or "act" in uri:
            return NodeType.ACT
        return NodeType.OPERSIG

    @staticmethod
    def _validate_phase(signature: FragmentSig) -> None:
        if signature.entry_point not in signature.nodes:
            raise ValueError(f"Entry point '{signature.entry_point}' is missing in nodes.")
        for frag_id, fragment in signature.nodes.items():
            for edge in fragment.relations:
                if not edge.is_external_or_terminal and edge.target not in signature.nodes:
                    raise ValueError(f"Fragment '{frag_id}' points to unknown target '{edge.target}'.")