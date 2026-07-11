# anchor.align.commit
import argparse
import sys
import subprocess
from typing import List, Callable
from pathlib import Path

from arch.contract.registry.unified import contract

from phase.bind.resolver import find_current_self
from phase.runtime.cli.executor import execute_cli_task, CliTaskAdapter, parse_local, dispatch_cli
from phase.gov.node.anchor import ActorNode, EpochManager
from phase.ator.protocol.commit import anchor_git_commit
from phase.gov.node.discovery import NodeDiscovery

from watcher.plane.emitter import get_emitter

log = get_emitter("align.commit", mode="SLIM")

def is_git_repo(path: Path) -> bool:
    """@role: Physical Git Detector (물리적 구현체 판별)"""
    return (path / ".git").is_dir()

def git_commit_runner(path: Path, message: str, apply: bool) -> str:
    """@role: Physical State Finalizer - 실제 Git 저장소의 상태를 확정하고 결과 해시를 반환"""
    if not apply:
        return "dry-run-id"
        
    try:
        status = subprocess.run(
            ["git", "status", "--porcelain"], 
            cwd=path, capture_output=True, text=True
        ).stdout.strip()
        
        if not status:
            res = subprocess.run(
                ["git", "rev-parse", "HEAD"], 
                cwd=path, capture_output=True, text=True
            )
            return res.stdout.strip()

        subprocess.run(["git", "add", "-A"], cwd=path, check=True)
        subprocess.run(["git", "commit", "-m", message], cwd=path, check=True)
        
        res = subprocess.run(
            ["git", "rev-parse", "HEAD"], 
            cwd=path, capture_output=True, text=True
        )
        return res.stdout.strip()
        
    except Exception as e:
        log.error(f"Git execution failed at {path}: {e}")
        return "0000000"

class Commiter:
    """Coordinate physical runners with logical nodes based on injected protocols"""
    
    def __init__(self, apply: bool, runner: Callable, protocol: Callable, **protocol_kwargs):
        self.apply = apply
        self.runner = runner
        self.protocol = protocol
        self.protocol_kwargs = protocol_kwargs
        
        try:
            self.root = find_current_self()
        except Exception as e:
            log.crit(f"failed to resolve self root: {e}")
            sys.exit(1)

    def run(self):
        log.info(f"## execution mode: {'APPLY' if self.apply else 'DRY-RUN'}")
        
        # 1. 탐색기 조립: 논리적 탐색기에 물리적 조건(Git) 주입
        discovery = NodeDiscovery(self.root, is_node_fn=is_git_repo)
        found_paths: List[Path] = discovery.scan()

        if not found_paths:
            log.warning("no physical nodes discovered")
            return

        # 2. 노드 인스턴스화: 탐색된 경로에 물리적 실행기(runner) 주입
        nodes: List[ActorNode] = [
            ActorNode(name=path.name, path=str(path), runner=self.runner) 
            for path in found_paths if path.resolve() != self.root.resolve()
        ]
        
        # 3. 앵커 및 프로토콜 조율
        anchor = EpochManager(name="self", path=str(self.root), runner=self.runner)
        log.info(f"initiating protocol for {len(nodes)} nodes under anchor: {anchor.name}")
        self.protocol(repos=nodes, anchor=anchor, apply=self.apply, **self.protocol_kwargs)

def entry_task(args):
    parser = argparse.ArgumentParser(description="Era-based Alignment Orchestrator")
    parser.add_argument("-m", "--message", required=True, help="Commit message")
    parser.add_argument("--apply", action="store_true", help="Actually execute state closure")
    parsed_args = parser.parse_args(args)

    commiter = Commiter(
        apply=parsed_args.apply,
        runner=git_commit_runner,
        protocol=anchor_git_commit,
        message=parsed_args.message
    )
    return CliTaskAdapter(commiter.run)

@contract.cli(name="align.commit", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("align.commit", entry_task, __file__)

if __name__ == "__main__":
    main()