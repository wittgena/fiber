# fiber.phase.fsm.dataset.template
"""
ASL Topology Lock & TaskIOSpec Rebinding Validator based on `fiber.phase.fsm.contract` and `compiler`.
Uses existing contract schemas (`FragmentSig`, `TaskIOSpec`, `ProjectionConfig`, `CompiledBundle`)
without introducing external schema layers.
"""
import copy
from typing import Any, Dict, List, Optional, Set, Tuple

from fiber.phase.fsm.compiler import FSMCompiler
from fiber.phase.fsm.contract import (
    MAX_BLOB_BYTES,
    NATIVE_MCP_SCHEMES,
    POLYGLOT_OPERSIG_SCHEMES,
    CompiledBundle,
    FragmentSig,
    NodeType,
    parse_resource_uri,
)
from fiber.phase.fsm.dataset.filter import sanitize_node_id

VALID_OPERSIG_SCHEMES = frozenset(NATIVE_MCP_SCHEMES + POLYGLOT_OPERSIG_SCHEMES)


def extract_topology_lock(asl: Dict[str, Any]) -> Dict[str, Any]:
    """
    기존 ASL에서 불변 위상 요소(`StartAt`, 각 노드의 `Type`, `Next`, `Catch`, `Choices`, `Default`,
    `allow_parallel`, `Projection.extract_keys`, 그리고 `.$` 동적 바인딩 키)만 추출합니다.
    """
    states = asl.get("States", {})
    locked_states: Dict[str, Any] = {}

    for state_id, state_def in states.items():
        s_type = state_def.get("Type")
        entry: Dict[str, Any] = {"Type": s_type}

        if "Next" in state_def:
            entry["Next"] = copy.deepcopy(state_def["Next"])
        if "Catch" in state_def:
            entry["Catch"] = copy.deepcopy(state_def["Catch"])
        if "Choices" in state_def:
            entry["Choices"] = copy.deepcopy(state_def["Choices"])
        if "Default" in state_def:
            entry["Default"] = state_def["Default"]

        if s_type == "Task":
            params = state_def.get("Parameters") or {}
            ext = params.get("__fiber_extensions") or {}
            proj = ext.get("Projection") or {}

            dynamic_bindings = {
                k: v for k, v in params.items() if k.endswith(".$") and isinstance(v, str)
            }
            static_keys = [
                k for k in params.keys() if not k.endswith(".$") and k != "__fiber_extensions"
            ]

            entry["node_type"] = parse_resource_uri(state_def.get("Resource", "")).node_type.value
            entry["allow_parallel"] = bool(ext.get("allow_parallel", False))
            entry["extract_keys"] = list(proj.get("extract_keys") or [])
            entry["offload_path"] = proj.get("offload_path")
            entry["dynamic_params"] = dynamic_bindings
            entry["static_param_keys"] = static_keys

        locked_states[state_id] = entry

    return {
        "Version": asl.get("Version", "1.0"),
        "StartAt": asl.get("StartAt"),
        "States": locked_states,
    }


def validate_asl_contract(
    compiler: FSMCompiler,
    asl: Dict[str, Any],
    trace_id: str = "template_check",
) -> Tuple[bool, Optional[CompiledBundle], Optional[str]]:
    """
    `contract.py` 및 `compiler.py` 규칙에 따라 ASL을 정적 검증합니다:
    1. 모든 `.$` 동적 참조 변수가 선행/전체 `Projection.extract_keys` 또는 컴파일러 자동 추출 키에 포함되어 있는지 확인
    2. `compiler.compile_from_asl()`을 호출하여 `_validate_phase`, WASM 정적 샌드박스, `<= 2048B` 검증
    """
    states = asl.get("States", {})
    if not isinstance(states, dict) or not states:
        return False, None, "Empty States"

    # 1. 컴파일러 자동 스캔 Choice 변수 및 모든 Task의 extract_keys 집합 확인
    emitted_keys: Set[str] = {"$._score", "$._last_ptr"}
    for auto_k in FSMCompiler._scan_choice_variables(states):
        emitted_keys.add(auto_k if auto_k.startswith("$.") else f"$.{auto_k}")

    for s_def in states.values():
        if s_def.get("Type") == "Task":
            ext = (s_def.get("Parameters") or {}).get("__fiber_extensions") or {}
            proj = ext.get("Projection") or {}
            for k in proj.get("extract_keys") or []:
                emitted_keys.add(k if k.startswith("$.") else f"$.{k}")

    # 2. Task 노드의 `.$` 참조가 실제 emitted_keys에 존재하는지 확인
    for state_id, s_def in states.items():
        if s_def.get("Type") != "Task":
            continue
        params = s_def.get("Parameters") or {}
        for k, v in params.items():
            if k.endswith(".$") and isinstance(v, str):
                norm_v = v if v.startswith("$.") else f"$.{v}"
                if norm_v not in emitted_keys:
                    return False, None, f"State '{state_id}' references unprojected key '{v}' in '{k}'"

    # 3. FSMCompiler 정적 검증 및 2KB 바이트 제한 확인
    try:
        bundle = compiler.compile_from_asl(asl_schema=asl, trace_id=trace_id)
        if bundle.total_root_bytes > MAX_BLOB_BYTES:
            return False, None, f"Root bytes ({bundle.total_root_bytes}B) > {MAX_BLOB_BYTES}B"
        return True, bundle, None
    except Exception as e:
        return False, None, f"{type(e).__name__}: {e}"


def verify_topology_equivalence(
    compiler: FSMCompiler,
    original_asl: Dict[str, Any],
    candidate_asl: Dict[str, Any],
) -> bool:
    """
    두 ASL의 `FragmentSig` 위상(엔트리포인트, 노드 집합, NodeType, EdgeRelation,
    allow_parallel, pressure, max_failures)이 100% 동일한지 `compiler._build_fragment_sig`를 통해 검증합니다.
    """
    try:
        sig_orig: FragmentSig = compiler._build_fragment_sig(original_asl, trace_id="eq_check")
        sig_cand: FragmentSig = compiler._build_fragment_sig(candidate_asl, trace_id="eq_check")
    except Exception:
        return False

    if sig_orig.entry_point != sig_cand.entry_point:
        return False
    if set(sig_orig.nodes.keys()) != set(sig_cand.nodes.keys()):
        return False

    for nid, f_orig in sig_orig.nodes.items():
        f_cand = sig_cand.nodes[nid]
        if f_orig.type != f_cand.type:
            return False
        if f_orig.attributes.allow_parallel != f_cand.attributes.allow_parallel:
            return False
        if f_orig.attributes.pressure != f_cand.attributes.pressure:
            return False
        if f_orig.attributes.max_failures != f_cand.attributes.max_failures:
            return False
        if f_orig.relations != f_cand.relations:
            return False

    return True


def _normalize_rebound_uri(orig_uri: str, cand_uri_raw: str) -> str:
    orig_parsed = parse_resource_uri(orig_uri)
    cand_parsed = parse_resource_uri(cand_uri_raw)

    # NodeType이 동일한 경우: OPERSIG 노드는 타겟 식별자 정규화 수행
    if cand_parsed.node_type == orig_parsed.node_type:
        if orig_parsed.node_type == NodeType.OPERSIG and cand_parsed.scheme in ("mcp", "fn", "tool", "sidecar"):
            scheme = cand_parsed.scheme or orig_parsed.scheme or "mcp"
            return f"{scheme}://{sanitize_node_id(cand_parsed.target_id)}"
        return cand_parsed.raw_uri

    # LLM이 스킴을 누락하거나 잘못 적어 NodeType이 달라진 경우 원본 스킴으로 강제 복원
    if orig_parsed.node_type == NodeType.OPERSIG:
        scheme = orig_parsed.scheme if orig_parsed.scheme in VALID_OPERSIG_SCHEMES else "mcp"
        clean_target = sanitize_node_id(cand_parsed.target_id or orig_parsed.target_id)
        return f"{scheme}://{clean_target}"

    return orig_uri


def rebind_task_params(
    base_asl: Dict[str, Any],
    node_updates: Dict[str, Dict[str, Any]],
    compiler: Optional[FSMCompiler] = None,
    trace_id: str = "rebind_0",
) -> Tuple[Dict[str, Any], Optional[CompiledBundle]]:
    """
    기존 `convert.py`가 생성한 `base_asl`의 위상(`Next`, `Catch`, `Choices`, `Default`,
    `allow_parallel`, `Projection`, `VCR`, `.$` 동적 파라미터)을 그대로 고정한 상태에서,
    각 Task 노드의 `Resource`와 정적 `Parameters`(및 필요한 경우 `extras`)만 교체합니다.

    `node_updates` 포맷:
    {
        "<state_id>": {
            "Resource": "mcp://new_tool_name",      # Optional (NodeType 불변 범위 내에서 변경)
            "Parameters": {"arg1": "val1", ...},    # 정적 파라미터만 반영 ('.$' 키는 기존 base_asl 값 강제 유지)
            "extras": {...},                        # Optional (`__fiber_extensions` 내 비제어 속성 병합용)
        }
    }
    """
    mutated = copy.deepcopy(base_asl)
    states = mutated.get("States", {})

    for state_id, s_def in states.items():
        if s_def.get("Type") != "Task" or state_id not in node_updates:
            continue

        upd = node_updates[state_id]
        if not isinstance(upd, dict):
            continue

        # 1. Resource URI 업데이트 (컴파일된 NodeType 불변 보장)
        if upd.get("Resource"):
            s_def["Resource"] = _normalize_rebound_uri(
                orig_uri=s_def.get("Resource", ""),
                cand_uri_raw=str(upd["Resource"]),
            )

        # 2. 기존 동적 바인딩(`.$`) 및 `__fiber_extensions`의 제어 속성 보존
        orig_params = dict(s_def.get("Parameters") or {})
        locked_dynamic = {k: v for k, v in orig_params.items() if k.endswith(".$")}
        orig_ext = copy.deepcopy(orig_params.get("__fiber_extensions") or {})

        locked_ext_keys = {
            k: orig_ext[k]
            for k in ("Projection", "VCR", "allow_parallel", "pressure", "max_failures")
            if k in orig_ext
        }

        # 3. 새로운 정적 파라미터 추출 ('.$' 및 '__fiber_extensions' 제외)
        new_static_raw = upd.get("Parameters")
        if isinstance(new_static_raw, dict):
            static_params = {
                k: v
                for k, v in new_static_raw.items()
                if not str(k).endswith(".$") and k != "__fiber_extensions"
            }
        else:
            static_params = {
                k: v
                for k, v in orig_params.items()
                if not str(k).endswith(".$") and k != "__fiber_extensions"
            }

        # 4. extras 병합 및 잠긴 제어 속성 복원
        if isinstance(upd.get("extras"), dict):
            orig_ext.update(copy.deepcopy(upd["extras"]))
        orig_ext.update(locked_ext_keys)

        rebound_params: Dict[str, Any] = {
            **static_params,
            **locked_dynamic,
        }
        if orig_ext:
            rebound_params["__fiber_extensions"] = orig_ext

        s_def["Parameters"] = rebound_params

    if compiler is not None:
        ok, bundle, _ = validate_asl_contract(compiler, mutated, trace_id=trace_id)
        if not ok or not verify_topology_equivalence(compiler, base_asl, mutated):
            return mutated, None
        return mutated, bundle

    return mutated, None