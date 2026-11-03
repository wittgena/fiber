# fiber.dev.tester.asl.etl
import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from fiber.phase.fsm.bridge.polyglot import PolyglotExecutionBridge
from fiber.phase.fsm.compiler import FSMCompiler
from fiber.phase.fsm.contract import (
    END_TOKEN,
    MAX_BLOB_BYTES,
    NodeType,
    parse_resource_uri,
)
from fiber.phase.fsm.dataset.extract import extract_dataset, package_tiers
from fiber.phase.fsm.dataset.template import (
    extract_topology_lock,
    rebind_task_params,
)
from fiber.phase.fsm.parser.arg import load_asl, parse_args
from fiber.phase.fsm.runtime import BranchCursor
from fiber.phase.fsm.traverser import GraphTraverser
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

logging.getLogger("httpx").setLevel(logging.WARNING)
log = get_emitter("tester.asl.etl", phase="DATASET_ETL")


def prepare_mapping_context(
    record: Dict[str, Any],
    target_domain: Optional[str] = None,
) -> Dict[str, Any]:
    rec_id = str(record.get("id", "unknown_id"))
    base_asl = record.get("asl") or {}
    topo_lock = extract_topology_lock(base_asl)

    if not target_domain or str(target_domain).startswith("$."):
        target_domain = "preserve_or_refine"

    cognitive_nodes: Dict[str, Any] = {}
    tool_nodes: Dict[str, Any] = {}

    for state_id, s_def in (base_asl.get("States") or {}).items():
        if s_def.get("Type") != "Task":
            continue

        params = s_def.get("Parameters") or {}
        resource_uri = s_def.get("Resource", "")
        parsed_uri = parse_resource_uri(resource_uri)
        lock_info = topo_lock["States"].get(state_id, {})

        static_params = {
            k: v
            for k, v in params.items()
            if not str(k).endswith(".$") and k != "__fiber_extensions"
        }

        if parsed_uri.node_type in (NodeType.PROJECTOR, NodeType.REFLECT, NodeType.TENSION):
            cognitive_nodes[state_id] = {
                "node_type": parsed_uri.node_type.value,
                "current_prompt": static_params.get("prompt", ""),
                "next": lock_info.get("Next"),
            }
        else:
            tool_nodes[state_id] = {
                "current_resource": resource_uri,
                "current_static_params": static_params,
                "locked_dynamic_params": lock_info.get("dynamic_params", {}),
                "locked_extract_keys": lock_info.get("extract_keys", []),
                "next": lock_info.get("Next"),
            }

    payload = {
        "record_id": rec_id,
        "target_domain": target_domain,
        "start_at": topo_lock.get("StartAt"),
        "cognitive_nodes": cognitive_nodes,
        "tool_nodes": tool_nodes,
    }
    return {
        "record_id": rec_id,
        "topology_lock": topo_lock,
        "llm_prompt": json.dumps(payload, ensure_ascii=False),
    }


def bind_and_verify(
    record: Dict[str, Any],
    node_updates: Optional[Dict[str, Dict[str, Any]]] = None,
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    rec_id = str(record.get("id", "unknown_id"))
    base_asl = record.get("asl") or {}

    if not isinstance(node_updates, dict) or not node_updates:
        log.warning(f"[Mapper:{rec_id}] Empty or invalid node_updates.")
        return {"_score": 0.0, "mapped_record": {}}

    compiler = FSMCompiler(byte_limit=MAX_BLOB_BYTES, use_wasm_linker=False)
    mapped_asl, bundle = rebind_task_params(
        base_asl=base_asl,
        node_updates=node_updates,
        compiler=compiler,
        trace_id=rec_id,
    )

    if bundle is None:
        log.warning(f"[Mapper:{rec_id}] Rebind failed topology equivalence or <= 2KB compile check.")
        return {"_score": 0.0, "mapped_record": {}}

    mapped_record = {
        "id": rec_id,
        "source": record.get("source", "mapped"),
        "root_hash": bundle.root_hash,
        "total_root_bytes": bundle.total_root_bytes,
        "asl": mapped_asl,
    }

    if output_path and str(output_path).strip() and not str(output_path).startswith("$."):
        out_file = Path(str(output_path))
        out_file.parent.mkdir(parents=True, exist_ok=True)
        with out_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(mapped_record, ensure_ascii=False) + "\n")

    log.info(
        f"[Mapper:{rec_id}] Verified mapped ASL | root_hash={bundle.root_hash} | bytes={bundle.total_root_bytes}B"
    )
    return {
        "_score": 1.0,
        "root_hash": bundle.root_hash,
        "total_root_bytes": bundle.total_root_bytes,
        "mapped_record": mapped_record,
    }


def _etl_arbiter(cursors: List[BranchCursor]) -> int:
    return max(
        range(len(cursors)),
        key=lambda i: (
            "_fallback_error" not in cursors[i].meta_ctx,
            float(cursors[i].meta_ctx.get("_score", 0.0)),
            cursors[i].fuel,
        ),
    )


async def _run_mapper_batch(
    traverser: GraphTraverser,
    asl: Dict[str, Any],
    args: Any,
) -> None:
    if "raw_record" in args.meta_dict:
        records = [args.meta_dict["raw_record"]]
    else:
        in_path = Path(args.input_file)
        records = [
            json.loads(line)
            for line in in_path.read_text("utf-8").splitlines()
            if line.strip()
        ]
        if args.limit > 0:
            records = records[: args.limit]

    verified = 0
    for idx, rec in enumerate(records):
        meta = {
            "raw_record": rec,
            "target_domain": args.domain,
            "model": args.model,
            "output_path": args.output_file,
            **args.meta_dict,
        }
        res = await traverser.run(
            asl_schema=asl,
            base_stream_id=f"{args.stream_id}_{idx}",
            initial_fuel=args.fuel,
            initial_meta=meta,
        )
        if (
            res.final_cursor.curr_node == END_TOKEN
            and float(res.final_cursor.meta_ctx.get("_score", 0.0)) >= 0.95
        ):
            verified += 1

    print(f"[Done:Mapper] Verified: {verified}/{len(records)} | Output: {args.output_file}")


async def run(args: Any) -> None:
    asl, _ = load_asl(args.asl, "etl")

    async with GraphTraverser(
        arbiter=_etl_arbiter,
        tenant_id=args.tenant,
        mcp_gateway_url=args.gateway_url,
    ) as traverser:
        bridge = PolyglotExecutionBridge(
            runtime=traverser.runtime,
            tenant_id=args.tenant,
            local_handlers={
                "extract_dataset": extract_dataset,
                "package_tiers": package_tiers,
                "prepare_mapping_context": prepare_mapping_context,
                "bind_and_verify": bind_and_verify,
            },
        )
        traverser.node_executor = bridge
        try:
            if asl.get("StartAt") == "Prepare_Lock":
                await _run_mapper_batch(traverser, asl, args)
            else:
                meta = {
                    "output_dir": args.output_dir,
                    "q_hermes": max(1, int(800 * args.scale)),
                    "q_xlam": max(1, int(1000 * args.scale)),
                    "q_tb_dag": max(1, int(900 * args.scale)),
                    "q_tb_chain": max(1, int(300 * args.scale)),
                    **args.meta_dict,
                }
                res = await traverser.run(
                    asl_schema=asl,
                    base_stream_id=args.stream_id,
                    initial_fuel=args.fuel,
                    initial_meta=meta,
                )
                assert res.final_cursor.curr_node == END_TOKEN
                print(f"[Done:ETL] Trace: {' -> '.join(res.visited_history)} | Meta: {res.final_cursor.meta_ctx}")
        finally:
            await bridge.aclose()


def main() -> None:
    args = parse_args("python -m dev.tester.asl.etl", is_downloader=True, is_mapper=True)
    staging = Path(args.output_dir) / ".staging"
    PhaseReactor.ignite(
        main_coro_func=lambda: run(args),
        teardown_hook=(lambda: asyncio.to_thread(shutil.rmtree, staging, True)) if args.clean_staging else None,
        shutdown_timeout=args.shutdown_timeout,
    )

if __name__ == "__main__":
    main()