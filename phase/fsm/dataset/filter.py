# fiber.phase.fsm.dataset.filter
import json
import re
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple
from pydantic import BaseModel, Field

from fiber.phase.fsm.contract import (
    CAS_PREFIX,
    BRANCH_PREFIX,
    END_TOKEN,
    RETURN_TOKEN,
)

RESERVED_TOKENS: Set[str] = {END_TOKEN, RETURN_TOKEN}
READ_PREFIXES: Tuple[str, ...] = ("get_", "search_", "fetch_", "list_", "read_", "check_")
WRITE_PREFIXES: Tuple[str, ...] = ("delete_", "create_", "update_", "send_", "pay_", "book_", "purge_")
SCALAR_STATUS_KEYS: Set[str] = {"status", "state", "error", "verified", "code"}


def sanitize_node_id(raw_id: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", str(raw_id).strip()).strip("_")
    if not clean:
        return "Step_Unnamed"
    if clean in RESERVED_TOKENS or clean.startswith((CAS_PREFIX, BRANCH_PREFIX)):
        clean = f"Step_{clean}"
    return clean


class FilteredXLAMRecord(BaseModel):
    record_id: str
    query: str
    calls: List[Dict[str, Any]]
    is_hitl_pair: bool = False


class FilteredHermesRecord(BaseModel):
    record_id: str
    prompt: str
    tool_name: str
    tool_args: Dict[str, Any]
    offload_path: str
    extract_keys: List[str]
    choice_rule: str


class FilteredTaskBenchRecord(BaseModel):
    record_id: str
    instruction: str
    node_ids: List[str]
    tasks: List[str]
    params_map: Dict[str, Dict[str, Any]]
    extract_map: Dict[str, List[str]]
    adj: Dict[str, List[str]]
    roots: List[str]
    barrier_map: Dict[str, str] = Field(default_factory=dict)
    is_dag: bool = False


class XLAMFilter:
    DATASET_ID = "Salesforce/xlam-function-calling-60k"

    @classmethod
    def apply(cls, row: Dict[str, Any], idx: int = 0) -> Optional[FilteredXLAMRecord]:
        try:
            answers = json.loads(row["answers"]) if isinstance(row.get("answers"), str) else row.get("answers")
        except Exception:
            return None

        if not isinstance(answers, list) or not (2 <= len(answers) <= 4):
            return None

        valid_calls = [a for a in answers if isinstance(a, dict) and a.get("name")]
        if len(valid_calls) != len(answers):
            return None

        func_names = [str(a["name"]).strip() for a in valid_calls]
        if len(set(func_names)) < 2:
            return None

        has_complex_param = any(
            isinstance(a.get("arguments"), dict)
            and len(a["arguments"]) >= 2
            and any(isinstance(v, (int, float, bool, list, dict)) for v in a["arguments"].values())
            for a in valid_calls
        )
        if not has_complex_param:
            return None

        has_read = any(n.lower().startswith(READ_PREFIXES) for n in func_names)
        has_write = any(n.lower().startswith(WRITE_PREFIXES) for n in func_names)

        normalized_calls = []
        for a in valid_calls:
            raw_name = str(a["name"]).strip()
            normalized_calls.append({
                "raw_name": raw_name,
                "clean_name": sanitize_node_id(raw_name),
                "arguments": dict(a.get("arguments") or {}),
                "is_write": raw_name.lower().startswith(WRITE_PREFIXES),
            })

        return FilteredXLAMRecord(
            record_id=str(row.get("id", f"xlam_{idx}")),
            query=str(row.get("query", "")),
            calls=normalized_calls,
            is_hitl_pair=(has_read and has_write),
        )


class HermesFilter:
    DATASET_ID = "NousResearch/hermes-function-calling-v1"
    TOOL_CALL_RE = re.compile(r"<tool_call>\s*(\{.*?\})\s*</tool_call>", re.DOTALL)
    TOOL_RESP_RE = re.compile(r"<tool_response>\s*(\{.*?\})\s*</tool_response>", re.DOTALL)

    @classmethod
    def apply(cls, row: Dict[str, Any], idx: int = 0) -> Optional[FilteredHermesRecord]:
        convs = row.get("conversations") or []
        human_q, tool_call_obj, tool_resp_content = None, None, None

        for turn in convs:
            role, val = turn.get("from"), turn.get("value", "")
            if role == "human" and not human_q:
                human_q = val
            elif role == "gpt" and "<tool_call>" in val and not tool_call_obj:
                m = cls.TOOL_CALL_RE.search(val)
                if m:
                    try:
                        tool_call_obj = json.loads(m.group(1))
                    except Exception:
                        pass
            elif role == "tool" and "<tool_response>" in val and not tool_resp_content:
                m = cls.TOOL_RESP_RE.search(val)
                if m:
                    try:
                        raw_resp = json.loads(m.group(1))
                        tool_resp_content = raw_resp.get("content", raw_resp) if isinstance(raw_resp, dict) else None
                    except Exception:
                        pass

        if not isinstance(tool_resp_content, dict) or len(tool_resp_content) < 3 or not isinstance(tool_call_obj, dict):
            return None

        offload_key: Optional[str] = None
        extract_keys: List[str] = []
        choice_rule: Optional[str] = None

        for k, v in tool_resp_content.items():
            if isinstance(v, (list, dict)) and offload_key is None:
                offload_key = f"$.{k}"
            elif isinstance(v, (int, float)) and not isinstance(v, bool):
                extract_keys.append(f"$.{k}")
                if not choice_rule:
                    choice_rule = f"$.{k} >= {v}"
            elif isinstance(v, str) and k.lower() in SCALAR_STATUS_KEYS:
                extract_keys.append(f"$.{k}")
                if not choice_rule:
                    choice_rule = f"$.{k} != 'error'"

        if not choice_rule:
            return None

        raw_args = tool_call_obj.get("arguments")
        if isinstance(raw_args, str):
            try:
                raw_args = json.loads(raw_args)
            except Exception:
                raw_args = {}

        return FilteredHermesRecord(
            record_id=str(row.get("id", f"hermes_{idx}")),
            prompt=str(human_q or ""),
            tool_name=sanitize_node_id(tool_call_obj.get("name", "hermes_tool")),
            tool_args=raw_args if isinstance(raw_args, dict) else {},
            offload_path=offload_key or "$.raw_payload",
            extract_keys=extract_keys,
            choice_rule=choice_rule,
        )


class TaskBenchFilter:
    DATASET_ID = "microsoft/Taskbench"
    CONFIGS = ("huggingface", "multimedia", "dailylifeapis")
    REF_RE = re.compile(r"<(?:node-(\d+)|output_of_n?(\d+)|output_of_([^>]+))>")

    @classmethod
    def _has_ancestor_path(cls, adj: Dict[str, List[str]], start: str, target: str) -> bool:
        queue = deque([start])
        visited: Set[str] = {start}
        while queue:
            curr = queue.popleft()
            if curr == target:
                return True
            for nxt in adj.get(curr, []):
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)
        return False

    @classmethod
    def apply(cls, row: Dict[str, Any], idx: int = 0) -> Optional[FilteredTaskBenchRecord]:
        if not row.get("human_verified", True):
            return None

        try:
            nodes = json.loads(row["tool_nodes"]) if isinstance(row.get("tool_nodes"), str) else row.get("tool_nodes")
            links = json.loads(row["tool_links"]) if isinstance(row.get("tool_links"), str) else row.get("tool_links")
        except Exception:
            return None

        if not isinstance(nodes, list) or not isinstance(links, list) or not (3 <= len(nodes) <= 8) or not links:
            return None

        task_to_id: Dict[str, str] = {}
        node_ids: List[str] = []
        tasks: List[str] = []
        for i, n in enumerate(nodes):
            t_raw = str(n.get("task", f"task_{i}"))
            clean_t = sanitize_node_id(t_raw)
            nid = sanitize_node_id(f"Node_{i}_{clean_t}")
            node_ids.append(nid)
            tasks.append(clean_t)
            task_to_id[t_raw] = nid
            task_to_id[clean_t.lower()] = nid

        adj: Dict[str, List[str]] = defaultdict(list)
        in_deg: Dict[str, int] = defaultdict(int)
        for link in links:
            src = task_to_id.get(str(link.get("source", "")))
            tgt = task_to_id.get(str(link.get("target", "")))
            if src and tgt and src != tgt and tgt not in adj[src]:
                adj[src].append(tgt)
                in_deg[tgt] += 1

        if not adj:
            return None

        initial_roots = [nid for nid in node_ids if in_deg[nid] == 0]
        if not initial_roots:
            return None

        reachable: Set[str] = set()
        q = deque(initial_roots)
        while q:
            curr = q.popleft()
            if curr not in reachable:
                reachable.add(curr)
                for nxt in adj.get(curr, []):
                    q.append(nxt)

        active_indices = [i for i, nid in enumerate(node_ids) if nid in reachable]
        if len(active_indices) < 3:
            return None

        extract_map: Dict[str, List[str]] = defaultdict(list)
        params_map: Dict[str, Dict[str, Any]] = {}

        for i in active_indices:
            nid = node_ids[i]
            raw_args = nodes[i].get("arguments", [])
            norm_params: Dict[str, Any] = {}

            if isinstance(raw_args, dict):
                raw_items = [{"name": k, "value": v} for k, v in raw_args.items()]
            elif isinstance(raw_args, list):
                raw_items = raw_args
            else:
                raw_items = []

            for arg_idx, item in enumerate(raw_items):
                if isinstance(item, dict) and "name" in item:
                    k, v = str(item["name"]), item.get("value", "")
                else:
                    k, v = f"arg_{arg_idx}", item

                v_str = str(v)
                m = cls.REF_RE.search(v_str)
                if m:
                    num_ref = m.group(1) or m.group(2)
                    task_ref = m.group(3)
                    if num_ref is not None:
                        ref_idx = int(num_ref)
                        if ref_idx >= len(node_ids) or node_ids[ref_idx] not in reachable:
                            return None
                        ref_nid = node_ids[ref_idx]
                    else:
                        clean_ref = sanitize_node_id(task_ref).lower()
                        ref_nid = task_to_id.get(task_ref) or task_to_id.get(clean_ref)
                        if not ref_nid or ref_nid not in reachable:
                            return None
                        ref_idx = node_ids.index(ref_nid)

                    if not cls._has_ancestor_path(adj, ref_nid, nid):
                        return None

                    ref_key = f"$.out_node_{ref_idx}"
                    norm_params[f"{k}.$"] = ref_key
                    if ref_key not in extract_map[ref_nid]:
                        extract_map[ref_nid].append(ref_key)
                else:
                    norm_params[k] = v

            params_map[nid] = norm_params

        active_node_ids = [node_ids[i] for i in active_indices]
        active_tasks = [tasks[i] for i in active_indices]
        clean_adj = {
            nid: [t for t in adj.get(nid, []) if t in reachable]
            for nid in active_node_ids
        }
        is_dag = (
            len(initial_roots) >= 2
            or any(len(targets) >= 2 for targets in clean_adj.values())
            or any(in_deg[nid] >= 2 for nid in active_node_ids)
        )

        barrier_map: Dict[str, str] = {}
        join_nodes = [nid for nid in active_node_ids if in_deg[nid] >= 2]
        for nid in active_node_ids:
            if is_dag:
                matched_barrier = "Evaluate_And_Join"
                for jn in join_nodes:
                    if jn != nid and cls._has_ancestor_path(clean_adj, nid, jn):
                        matched_barrier = jn
                        break
                barrier_map[nid] = matched_barrier

        return FilteredTaskBenchRecord(
            record_id=str(row.get("id", f"tb_{idx}")),
            instruction=str(row.get("instruction", "")),
            node_ids=active_node_ids,
            tasks=active_tasks,
            params_map=params_map,
            extract_map=dict(extract_map),
            adj=clean_adj,
            roots=initial_roots,
            barrier_map=barrier_map,
            is_dag=is_dag,
        )