# phase.flow.align.imports
import os
import sys
import argparse
from pathlib import Path

from fiber.phase.flow.imports.aligner import PrefixImportAligner

from xphi.arch.contract.registry.unified import contract
from xphi.kernel.bind.resolver import find_current_self
from xphi.kernel.phase.runtime.executor.cli import CliTaskAdapter, parse_local, dispatch_cli
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("align.imports", phase="SYSTEM")

try:
    SELF_ROOT = find_current_self()
except Exception as e:
    log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)

def key_by_directory(r: dict) -> str:
    return str(Path(r["path"]).parent)

GROUP_KEYS = { "by_directory": key_by_directory }

class ImportCliRunner:
    """ImportAligner의 실행을 래핑하고, System Emitter Reporting을 담당하는 Runner"""
    def __init__(self, root_dir: Path, old: str, new: str):
        self.old = old
        self.new = new
        self.aligner = PrefixImportAligner(root_dir=str(root_dir), old_prefix=old, new_prefix=new)

    def run(self, axis: str, group_keys: dict, apply: bool):
        # 현재 실행 모드를 명시적으로 출력하여 혼선 방지
        mode = "APPLY" if apply else "DRY-RUN"
        log.signal(f"Initiating Import Alignment [{mode}]: '{self.old}' -> '{self.new}'")
        
        result = self.aligner.run(
            axis=axis,
            group_keys=group_keys,
            scan_kwargs={},
            fix_kwargs={"apply": apply}
        )

        ## [3] 모든 출력을 print가 아닌 Emitter(log)로 전환
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
    """Era-based Alignment Orchestrator 표준 엔트리포인트"""
    parser = argparse.ArgumentParser(description="Safely refactor Python import paths using LibCST and BaseAligner.")
    parser.add_argument("--repo", required=True, help="Target Repo (relative to SELF_ROOT)")
    parser.add_argument("--old", required=True, help="Old module prefix")
    parser.add_argument("--new", required=True, help="New module prefix")
    parser.add_argument("--dry-run", action="store_true", help="Do not apply changes, just show what would be updated")
    parsed_args = parser.parse_args(args)
    
    target_root = SELF_ROOT / parsed_args.repo
    runner = ImportCliRunner(
        root_dir=target_root,
        old=parsed_args.old,
        new=parsed_args.new
    )
    
    run_kwargs = {
        "axis": "by_directory",
        "group_keys": GROUP_KEYS,
        "apply": not parsed_args.dry_run
    }
    return CliTaskAdapter(runner.run, **run_kwargs)


@contract.cli(
    name="align.imports",
    args=["--repo", "--old", "--new", "--dry-run"],
    tags=["align", "mutation", "core"],
    recept=[],
    entry="entry_task"
)
def main():
    """시스템 글로벌 라우터 인터페이스"""
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("align.imports", entry_task, __file__)

if __name__ == "__main__":
    main()