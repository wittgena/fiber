# phase.flow.align.plane
## @lineage: phase.align.plane
## @lineage: flow.align.plane
import os
import sys
import argparse
from pathlib import Path

from phase.flow.imports.aligner import RelativeImportAligner

from arch.contract.registry.unified import contract
from kernel.phase.runtime.executor.cli import CliTaskAdapter, parse_local, dispatch_cli
from kernel.bind.resolver import find_current_self
from watcher.plane.emitter import get_emitter

log = get_emitter("align.plane", phase="SYSTEM")

SELF_ROOT = find_current_self()

def key_by_directory(r: dict) -> str:
    return str(Path(r["path"]).parent)

GROUP_KEYS = { "by_directory": key_by_directory }

class RelativeImportCliRunner:
    """RelativeImportAligner의 실행을 래핑하고, System Emitter Reporting을 담당하는 Runner"""
    def __init__(self, root_dir: Path, project_root: Path):
        self.aligner = RelativeImportAligner(root_dir=str(root_dir), project_root=str(project_root))

    def run(self, axis: str, group_keys: dict, apply: bool):
        # 현재 실행 모드를 명시적으로 출력하여 혼선 방지
        mode = "APPLY" if apply else "DRY-RUN"
        log.signal(f"Initiating Relative Import Alignment [{mode}]: Target Path = '{self.aligner.root_dir}'")
        
        result = self.aligner.run(
            axis=axis,
            group_keys=group_keys,
            scan_kwargs={},
            fix_kwargs={"apply": apply}
        )

        log.flush()
        log.info("=" * 50)
        log.info("Alignment Summary:")
        log.info(f"Total Files: {result['summary']['total_files']}")
        log.info(f"Matched (No change needed): {result['summary']['matched']}")
        log.info(f"Mismatched (To be updated): {result['summary']['mismatched']}")
        log.info("=" * 50)

        for cluster in result["clusters"]:
            log.info(f"\nDirectory: {cluster['path']}")
            for item in cluster["items"]:
                log.info(f"  - File: {Path(item['path']).name} [Status: {item['status']}]")
                # dry-run 모드일 때만 diff(변경 예정 사항) 출력
                if not apply:
                    log.info(item.get("diff", ""))
                    log.info("-" * 40)
                    
        return result


def entry_task(args):
    """Era-based Alignment Orchestrator 표준 엔트리포인트 (Anchor)"""
    parser = argparse.ArgumentParser(description="Safely refactor relative Python import paths to absolute.")
    parser.add_argument("--path", required=True, help="Target specific directory or file path to process")
    parser.add_argument("--dry-run", action="store_true", help="Do not apply changes, just show what would be updated")
    parsed_args = parser.parse_args(args)
    
    target_root = Path(parsed_args.path).resolve()
    if not target_root.exists():
        log.error(f"[error] 지정한 경로를 찾을 수 없습니다: {target_root}")
        sys.exit(1)

    runner = RelativeImportCliRunner(root_dir=target_root, project_root=SELF_ROOT)
    run_kwargs = {
        "axis": "by_directory",
        "group_keys": GROUP_KEYS,
        "apply": not parsed_args.dry_run
    }
    return CliTaskAdapter(runner.run, **run_kwargs)

@contract.cli(
    name="plane.align",
    args=["--path", "--dry-run"],
    tags=["anchor", "mutation", "imports"],
    recept=[],
    entry="entry_task"
)
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("plane.align", entry_task, __file__)

if __name__ == "__main__":
    main()