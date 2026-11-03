# fiber.phase.fsm.dataset.extract
import asyncio
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from datasets import load_dataset
from fiber.phase.fsm.compiler import FSMCompiler
from fiber.phase.fsm.contract import (
    MAX_BLOB_BYTES,
    ProjectionConfig,
    VCRConfig,
)
from fiber.phase.fsm.dataset.filter import (
    FilteredHermesRecord,
    FilteredTaskBenchRecord,
    FilteredXLAMRecord,
    HermesFilter,
    TaskBenchFilter,
    XLAMFilter,
)
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("dataset.extract", phase="DATASET_ETL")


# =====================================================================
# 1. 데이터셋별 ASL 빌더 및 2KB 컴파일 검증기
# =====================================================================
class XLAMConverter:
    @staticmethod
    def to_asl(rec: FilteredXLAMRecord, idx: int = 0) -> Dict[str, Any]:
        branch_ids: List[str] = []
        states: Dict[str, Any] = {}

        for i, call in enumerate(rec.calls):
            t_name = call["clean_name"]
            node_id = f"Branch_{i}_{t_name}"
            branch_ids.append(node_id)

            params = dict(call["arguments"])
            params["__fiber_extensions"] = {
                "VCR": VCRConfig(capture_mode="rpc_payload", coalescing_window_ms=0).model_dump(),
                "Projection": ProjectionConfig(
                    offload_path="$.raw_payload",
                    extract_keys=["$._score", "$.status"],
                ).model_dump(exclude_none=True),
            }

            states[node_id] = {
                "Type": "Task",
                "Resource": f"mcp://{t_name}",
                "Parameters": params,
                "Next": "Evaluate_And_Join",
                "Catch": "Evaluate_And_Join",
            }

        return {
            "Version": "1.0",
            "StartAt": "Plan_Intent_LLM",
            "States": {
                "Plan_Intent_LLM": {
                    "Type": "Task",
                    "Resource": "llm://gpt-4o",
                    "Parameters": {
                        "prompt": rec.query,
                        "__fiber_extensions": {"allow_parallel": True},
                    },
                    "Next": branch_ids,
                },
                **states,
                "Evaluate_And_Join": {
                    "Type": "Choice",
                    "Choices": [{"if": "$._score >= 0.95", "Next": "Final_Act"}],
                    "Default": "Final_Act",
                },
                "Final_Act": {"Type": "Succeed"},
            },
        }


class HermesConverter:
    @staticmethod
    def to_asl(rec: FilteredHermesRecord, idx: int = 0) -> Dict[str, Any]:
        proj = ProjectionConfig(
            offload_path=rec.offload_path,
            extract_keys=rec.extract_keys,
        ).model_dump(exclude_none=True)

        return {
            "Version": "1.0",
            "StartAt": "Plan_LLM",
            "States": {
                "Plan_LLM": {
                    "Type": "Task",
                    "Resource": "llm://gpt-4o",
                    "Parameters": {"prompt": rec.prompt},
                    "Next": "Tool_Exec",
                },
                "Tool_Exec": {
                    "Type": "Task",
                    "Resource": f"mcp://{rec.tool_name}",
                    "Parameters": {
                        **rec.tool_args,
                        "__fiber_extensions": {
                            "Projection": proj,
                            "VCR": VCRConfig(capture_mode="rpc_payload").model_dump(),
                        },
                    },
                    "Next": "Eval_Router",
                },
                "Eval_Router": {
                    "Type": "Choice",
                    "Choices": [{"if": rec.choice_rule, "Next": "Reflect_LLM"}],
                    "Default": "Final_Act",
                },
                "Reflect_LLM": {
                    "Type": "Task",
                    "Resource": "reflect://gpt-4o-mini",
                    "Parameters": {"prompt": "Synthesize final response from projected context."},
                    "Next": "Final_Act",
                },
                "Final_Act": {"Type": "Succeed"},
            },
        }


class TaskBenchConverter:
    @staticmethod
    def to_asl(rec: FilteredTaskBenchRecord, idx: int = 0) -> Dict[str, Any]:
        states: Dict[str, Any] = {
            "Plan_Intent_LLM": {
                "Type": "Task",
                "Resource": "llm://gpt-4o",
                "Parameters": {
                    "prompt": rec.instruction,
                    **(
                        {"__fiber_extensions": {"allow_parallel": True}}
                        if len(rec.roots) >= 2
                        else {}
                    ),
                },
                "Next": rec.roots[0] if len(rec.roots) == 1 else rec.roots,
            }
        }

        for i, nid in enumerate(rec.node_ids):
            next_targets = rec.adj.get(nid, [])
            params = dict(rec.params_map.get(nid, {}))
            ext: Dict[str, Any] = {}

            if nid in rec.extract_map:
                ext["Projection"] = ProjectionConfig(
                    offload_path="$.raw_payload",
                    extract_keys=rec.extract_map[nid],
                ).model_dump(exclude_none=True)

            if len(next_targets) >= 2:
                ext["allow_parallel"] = True

            if ext:
                params["__fiber_extensions"] = ext

            state_def: Dict[str, Any] = {
                "Type": "Task",
                "Resource": f"mcp://{rec.tasks[i]}",
                "Parameters": params,
                "Next": (
                    next_targets[0]
                    if len(next_targets) == 1
                    else (next_targets if next_targets else "Evaluate_And_Join")
                ),
            }
            if rec.is_dag:
                state_def["Catch"] = rec.barrier_map.get(nid, "Evaluate_And_Join")

            states[nid] = state_def

        states["Evaluate_And_Join"] = {
            "Type": "Choice",
            "Choices": [{"if": "$._score >= 0.95", "Next": "Final_Act"}],
            "Default": "Final_Act",
        }
        states["Final_Act"] = {"Type": "Succeed"}

        return {
            "Version": "1.0",
            "StartAt": "Plan_Intent_LLM",
            "States": states,
        }


def verify_and_compile(compiler: FSMCompiler, asl: Dict[str, Any], trace_id: str) -> Optional[Dict[str, Any]]:
    try:
        bundle = compiler.compile_from_asl(asl_schema=asl, trace_id=trace_id)
        if bundle.total_root_bytes <= MAX_BLOB_BYTES:
            return {
                "root_hash": bundle.root_hash,
                "total_root_bytes": bundle.total_root_bytes,
            }
    except Exception:
        return None
    return None


# =====================================================================
# 2. 데이터셋 레지스트리 및 추출·패키징 핸들러
# =====================================================================
REGISTRY: Dict[str, Tuple[Any, Any, str, Tuple[Optional[str], ...], str]] = {
    "hermes": (
        HermesFilter,
        HermesConverter,
        HermesFilter.DATASET_ID,
        ("func_calling", "glaive_func_calling"),
        "train",
    ),
    "xlam": (
        XLAMFilter,
        XLAMConverter,
        XLAMFilter.DATASET_ID,
        (None,),
        "train",
    ),
    "taskbench": (
        TaskBenchFilter,
        TaskBenchConverter,
        TaskBenchFilter.DATASET_ID,
        TaskBenchFilter.CONFIGS,
        "test",
    ),
}


def _sync_extract(
    source: str,
    tag: str,
    quota: int = 10,
    output_dir: str = "./golden_3k",
    dag_only: Optional[bool] = None,
) -> Dict[str, Any]:
    f_cls, c_cls, ds_id, configs, split_name = REGISTRY[source]
    compiler = FSMCompiler(byte_limit=MAX_BLOB_BYTES, use_wasm_linker=False)
    staging = Path(output_dir) / ".staging"
    staging.mkdir(parents=True, exist_ok=True)

    accepted: List[Dict[str, Any]] = []

    for cfg in configs:
        stream = (
            load_dataset(ds_id, cfg, split=split_name, streaming=True)
            if cfg
            else load_dataset(ds_id, split=split_name, streaming=True)
        )
        for idx, row in enumerate(stream):
            rec = f_cls.apply(row, idx)
            if not rec or (dag_only is not None and getattr(rec, "is_dag", None) != dag_only):
                continue

            asl = c_cls.to_asl(rec, idx)
            rec_id = f"{tag}_{rec.record_id}"
            meta = verify_and_compile(compiler, asl, rec_id)
            if not meta:
                continue

            accepted.append({
                "id": rec_id,
                "source": source if not cfg else f"{source}/{cfg}",
                **meta,
                "asl": asl,
            })
            if len(accepted) >= int(quota):
                break
        if len(accepted) >= int(quota):
            break

    log.info(f"[{tag}] Extracted {len(accepted)}/{quota} records.")

    shard = staging / f"shard_{tag}.jsonl"
    shard.write_text(
        "\n".join(json.dumps(x, ensure_ascii=False) for x in accepted) + "\n", "utf-8"
    )

    return {
        "_score": 0.98 if accepted else 0.0,
        f"{tag}_count": len(accepted),
        "shard": {"path": str(shard), "count": len(accepted)},
    }


async def extract_dataset(
    source: str,
    tag: str,
    quota: int = 10,
    output_dir: str = "./golden_3k",
    dag_only: Optional[bool] = None,
) -> Dict[str, Any]:
    return await asyncio.to_thread(
        _sync_extract,
        source=source,
        tag=tag,
        quota=quota,
        output_dir=output_dir,
        dag_only=dag_only,
    )


def package_tiers(output_dir: str = "./golden_3k") -> Dict[str, Any]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    shards: Dict[str, List[Dict[str, Any]]] = {}
    shard_counts: Dict[str, int] = {}

    for tag in ("tb_dag", "tb_chain", "xlam", "hermes"):
        p = out / ".staging" / f"shard_{tag}.jsonl"
        items = (
            [json.loads(line) for line in p.read_text("utf-8").splitlines() if line.strip()]
            if p.is_file()
            else []
        )
        shards[tag] = items
        shard_counts[f"{tag}_count"] = len(items)

    pool: List[Dict[str, Any]] = []
    max_len = max((len(v) for v in shards.values()), default=0)
    for idx in range(max_len):
        for tag in ("tb_dag", "tb_chain", "xlam", "hermes"):
            if idx < len(shards[tag]):
                pool.append(shards[tag][idx])

    n = len(pool)
    c1, c2 = max(1, int(n * 100 / 3000)), max(2, int(n * 600 / 3000))
    tiers = [
        ("tier1_cookbook_100.jsonl", pool[:c1]),
        ("tier2_core_regression_500.jsonl", pool[c1:c2]),
        ("tier3_benchmark_2400.jsonl", pool[c2:]),
    ]
    for name, sl in tiers:
        (out / name).write_text(
            "\n".join(json.dumps(x, ensure_ascii=False) for x in sl) + "\n", "utf-8"
        )

    log.info(f"[Package] Packaged total={n} records | Shards={shard_counts}")
    return {
        "_score": 1.0 if n > 0 else 0.0,
        "status": "packaged",
        "total": n,
        **shard_counts,
    }