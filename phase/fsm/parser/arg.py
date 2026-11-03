# fiber.phase.fsm.parser.arg
import argparse
import copy
import json
import sys
from typing import Any, Dict, Optional, Tuple, Union

from fiber.phase.fsm.registry.asl import default_asl_registry


def str_to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if str(v).strip().lower() in ("1", "true", "t", "yes", "y", "mock"):
        return True
    if str(v).strip().lower() in ("0", "false", "f", "no", "n", "live"):
        return False
    raise argparse.ArgumentTypeError(f"Boolean expected, got '{v}'")


def load_asl(
    asl_arg: Optional[str],
    default_asl: Optional[Union[Dict[str, Any], str]] = None,
) -> Tuple[Dict[str, Any], str]:
    target = asl_arg.strip() if asl_arg and asl_arg.strip() else None

    if target == "-":
        return json.loads(sys.stdin.read()), "stdin"
    if target and target.startswith("{"):
        return json.loads(target), "inline_json"

    lookup_key = target if target is not None else default_asl
    if isinstance(lookup_key, str):
        found = default_asl_registry.get(lookup_key)
        if found is not None:
            return found
        raise FileNotFoundError(
            f"ASL '{lookup_key}' not found in search_dirs={default_asl_registry.search_dirs} "
            f"(available={default_asl_registry.list_available()})"
        )

    if isinstance(default_asl, dict):
        return copy.deepcopy(default_asl), "DEFAULT_ASL"

    raise ValueError("No valid ASL key, file path, or fallback schema provided.")


def parse_args(
    prog: str = "fsm",
    is_downloader: bool = False,
    is_mapper: bool = False,
) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog=prog)
    p.add_argument("-a", "--asl", type=str, default=None)
    p.add_argument("--asl-dir", type=str, default=None)
    p.add_argument("--mock", type=str_to_bool, nargs="?", const=True, default=True)
    p.add_argument("--live", action="store_true")
    p.add_argument("--single", action="store_true")
    p.add_argument("-f", "--fuel", type=int, default=1000)
    p.add_argument("-s", "--stream-id", type=str, default="fsm_run_01")
    p.add_argument("-m", "--meta", type=str, default=None)
    p.add_argument("--tenant", type=str, default="default_tenant")
    p.add_argument("--gateway-url", type=str, default="http://127.0.0.1:8356/v1/mcp-gateway")
    p.add_argument("--allow-exec", action="store_true")
    p.add_argument("--approve-hitl", action="store_true")
    p.add_argument("--resume", type=str, default=None)
    p.add_argument("--shutdown-timeout", type=float, default=10.0)

    if is_downloader:
        p.add_argument("--scale", type=float, default=0.01)
        p.add_argument("-o", "--output-dir", type=str, default="./golden_3k")
        p.add_argument("--gate-packaging", action="store_true")
        p.add_argument("--clean-staging", action="store_true")

    if is_mapper:
        p.add_argument("-i", "--input-file", type=str, default="./golden_3k/tier1_cookbook_100.jsonl")
        p.add_argument("-o", "--output-file", type=str, default="./golden_3k/mapped_tier1.jsonl")
        p.add_argument("--model", type=str, default="auto")
        p.add_argument("--domain", type=str, default="preserve_or_refine")
        p.add_argument("--limit", type=int, default=0)

    args = p.parse_args()
    if args.asl_dir:
        default_asl_registry.add_dir(args.asl_dir)

    args.use_mock = False if args.live else bool(args.mock)
    args.meta_dict = json.loads(args.meta) if args.meta else {}
    args.resume_list = [x.strip() for x in args.resume.split(",") if x.strip()] if args.resume else None
    if is_downloader and args.live and args.scale == 0.01:
        args.scale = 1.0
    return args