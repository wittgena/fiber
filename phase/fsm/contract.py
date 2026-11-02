# fiber.phase.fsm.contract
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from pydantic import BaseModel, Field

MAX_BLOB_BYTES = 2048
CAS_PREFIX = "blob:"
BRANCH_PREFIX = "branch:"
END_TOKEN = "END"
RETURN_TOKEN = "RETURN"

ASL_CHOICE_OP_MAP: Dict[str, str] = {
    "StringEquals": "==",
    "NumericEquals": "==",
    "BooleanEquals": "==",
    "NumericGreaterThanEquals": ">=",
    "NumericLessThanEquals": "<=",
    "NumericGreaterThan": ">",
    "NumericLessThan": "<",
    "StringNotEquals": "!=",
    "NumericNotEquals": "!=",
}


# Unified Vocabulary & Compact Keys
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

# Centralized Resource URI Parser (Shared by Compiler & Execution Bridges)
NATIVE_MCP_SCHEMES = ("mcp", "fiber", "tool", "sidecar")
POLYGLOT_OPERSIG_SCHEMES = ("fn", "py", "http", "https", "exec")

@dataclass(frozen=True)
class ParsedResource:
    raw_uri: str
    scheme: str
    target: str
    target_id: str
    node_type: NodeType
    model_hint: Optional[str] = None

    @property
    def is_native_mcp(self) -> bool:
        return self.node_type in (NodeType.OPERSIG, NodeType.PENDING) and (
            self.scheme in NATIVE_MCP_SCHEMES or not self.scheme
        )

    @property
    def is_polyglot_opersig(self) -> bool:
        return self.node_type == NodeType.OPERSIG and self.scheme in POLYGLOT_OPERSIG_SCHEMES

def parse_resource_uri(resource_uri: str) -> ParsedResource:
    """
    Single source of truth for parsing ASL `"Resource"` URIs across compile-time and runtime.
    Preserves `<provider>/<model>` (e.g. `llm://openai/gpt-4o`) intact for cognitive LLM nodes.
    """
    raw = (resource_uri or "").strip()
    if "://" in raw:
        scheme_part, rest = raw.split("://", 1)
        scheme = scheme_part.lower().strip()
        target = rest.strip() if scheme in ("http", "https", "exec") else rest.strip("/")
    else:
        scheme = ""
        target = raw.strip("/")

    target_id = target.rstrip("/").split("/")[-1] if "/" in target else target

    # Cognitive LLM schemes preserve full `<provider>/<model>` in both `target` and `target_id`
    if scheme == "llm":
        model = target if target and target not in ("default", "llm_proxy") else None
        return ParsedResource(raw, scheme, target, target, NodeType.PROJECTOR, model)

    if scheme == "reflect":
        model = target if target and target not in ("default", "reflect_llm") else None
        return ParsedResource(raw, scheme, target, target, NodeType.REFLECT, model)

    if scheme == "tension":
        model = target if target and target != "default" else None
        return ParsedResource(raw, scheme, target, target, NodeType.TENSION, model)

    if scheme in NATIVE_MCP_SCHEMES and scheme != "fiber":
        return ParsedResource(raw, scheme, target, target_id, NodeType.OPERSIG, None)

    if scheme in POLYGLOT_OPERSIG_SCHEMES:
        return ParsedResource(raw, scheme, target, target_id, NodeType.OPERSIG, None)

    if scheme == "pending":
        return ParsedResource(raw, scheme, target, target_id, NodeType.PENDING, None)

    if scheme in ("act", "io", "noop"):
        return ParsedResource(raw, scheme, target, target_id, NodeType.ACT, None)

    # Legacy `fiber://...` or unprefixed keyword fallback
    lower_raw = raw.lower()
    if "reflect" in lower_raw:
        nt = NodeType.REFLECT
    elif "tension" in lower_raw:
        nt = NodeType.TENSION
    elif "pending" in lower_raw:
        nt = NodeType.PENDING
    elif "llm" in lower_raw:
        nt = NodeType.PROJECTOR
    elif "sidecar" in lower_raw or "tool" in lower_raw or "mcp" in lower_raw:
        nt = NodeType.OPERSIG
    elif "io" in lower_raw or "act" in lower_raw:
        nt = NodeType.ACT
    else:
        nt = NodeType.OPERSIG

    return ParsedResource(raw, scheme or "fiber", target, target_id, nt, None)

# Shared JSONPath (`$.`), Dynamic Parameter (`.$`), and Projection Utilities
def extract_json_path(data: Dict[str, Any], path: Optional[str]) -> Any:
    """Resolves `$.a.b.0` or `a.b` paths from nested dictionaries/lists."""
    if not path or not isinstance(data, dict):
        return None
    if path in data:
        return data[path]
    clean = path.lstrip("$.")
    if clean in data:
        return data[clean]

    curr: Any = data
    for part in clean.split("."):
        if isinstance(curr, dict) and part in curr:
            curr = curr[part]
        elif isinstance(curr, list) and part.isdigit() and int(part) < len(curr):
            curr = curr[int(part)]
        else:
            return None
    return curr

def resolve_dynamic_params(params: Dict[str, Any], meta_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Resolves ASL dynamic parameter keys ending with `.$` against runtime `meta_ctx`."""
    resolved: Dict[str, Any] = {}
    for k, v in params.items():
        if k.endswith(".$") and isinstance(v, str):
            clean_key = k[:-2]
            val = extract_json_path(meta_ctx, v)
            resolved[clean_key] = val if val is not None else meta_ctx.get(v, v)
        else:
            resolved[k] = v
    return resolved

def apply_projection(
    raw_output: Any,
    projection: Optional[Dict[str, Any]],
    base_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Applies `ProjectionConfig` (`extract_keys` & `offload_path`) consistently across
    LLM, MCP, and Polyglot actuators. Returns `(meta_updates, offload_payload)`.
    """
    payload_dict = raw_output if isinstance(raw_output, dict) else {"data": raw_output}
    proj = projection or {}

    meta_updates: Dict[str, Any] = dict(base_meta or {})
    for key_path in proj.get("extract_keys", []):
        val = extract_json_path(payload_dict, key_path)
        if val is not None:
            meta_updates[key_path] = val
            meta_updates[key_path.split(".")[-1]] = val

    offload_path = proj.get("offload_path")
    offload_target = extract_json_path(payload_dict, offload_path) if offload_path else payload_dict
    if not isinstance(offload_target, dict):
        offload_target = {"data": offload_target}

    return meta_updates, offload_target

# Shared IR & CAS Schemas
class VCRConfig(BaseModel):
    capture_mode: str = Field(default="rpc_payload")
    coalescing_window_ms: int = Field(default=0, ge=0)

class ProjectionConfig(BaseModel):
    offload_path: Optional[str] = None
    extract_keys: List[str] = Field(default_factory=list)

class TaskIOSpec(BaseModel):
    resource_uri: str = ""
    result_path: str = "$"
    call_params: Dict[str, Any] = Field(default_factory=dict)
    vcr: Optional[VCRConfig] = None
    projection: Optional[ProjectionConfig] = None
    extras: Dict[str, Any] = Field(default_factory=dict)

class AgentAttributes(BaseModel):
    io_ref: Optional[str] = None
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

class CompiledBundle(BaseModel):
    root_hash: str
    root_blob: Dict[str, Any]
    cas_store: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    total_root_bytes: int = 0