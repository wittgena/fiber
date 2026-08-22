# phase.align.commit
## @lineage: phase.flow.align.commit
## @lineage: flow.align.commit
import argparse
import sys
import subprocess
import asyncio
from typing import List, Callable
from pathlib import Path

from arch.contract.registry.unified import contract
from arch.topos.node.discovery import NodeDiscovery
from kernel.bind.resolver import find_current_self
from kernel.dphi.broker import DphiBroker
from kernel.phase.commit import anchor_commit, Attractor, EpochManager
from kernel.phase.runtime.flow.executor import dispatch_flow_cli
from watcher.plane.emitter import get_emitter

log = get_emitter("align.commit", mode="SLIM")

def is_git_repo(path: Path) -> bool:
    """@role: Physical Git Detector"""
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

class NodeDiscovery:
    def __init__(self, root_path: Path, is_node_fn: Callable[[Path], bool]):
        self.root = root_path
        self.is_node = is_node_fn
        log.info(f"[NodeDiscovery] root_path: {self.root}")

    def scan(self, depth: int = 2) -> List[Path]:
        found_nodes: List[Path] = []
        log.info(f"scan start: {self.root} (Max Depth: {depth})")

        for entry in self.root.iterdir():
            if not (entry.is_dir() or entry.is_symlink()): 
                continue
            if self.is_node(entry):
                found_nodes.append(entry)
            if depth > 1:
                for sub in entry.iterdir():
                    if (sub.is_dir() or sub.is_symlink()) and self.is_node(sub):
                        found_nodes.append(sub)

        log.info(f"total nodes discovered: {len(found_nodes)}")
        return found_nodes

class FlowEvent:
    """FlowExecutor 스트림에 맞춰 상태를 반환하기 위한 경량 이벤트 객체"""
    def __init__(self, phase: str, boundary: str):
        self.phase = phase
        self.boundary = boundary

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

    async def execute_flow(self):
        yield FlowEvent("INIT", f"Execution mode: {'APPLY' if self.apply else 'DRY-RUN'}")
        
        discovery = NodeDiscovery(self.root, is_node_fn=is_git_repo)
        found_paths: List[Path] = discovery.scan()

        if not found_paths:
            yield FlowEvent("WARN", "No physical nodes discovered")
            return

        yield FlowEvent("DISCOVERY", f"Total nodes discovered: {len(found_paths)}")
        nodes: List[Attractor] = [
            Attractor(name=path.name, path=str(path), runner=self.runner) 
            for path in found_paths if path.resolve() != self.root.resolve()
        ]
        anchor = EpochManager(name="self", path=str(self.root), runner=self.runner)
        broker = DphiBroker()
        
        yield FlowEvent("PROTOCOL", f"Initiating async protocol for {len(nodes)} nodes under anchor: {anchor.name}")
        await self.protocol(repos=nodes, anchor=anchor, broker=broker, apply=self.apply, **self.protocol_kwargs)
        yield FlowEvent("COMPLETE", "State closure and protocol finalized.")

def entry_task(args):
    parser = argparse.ArgumentParser(description="Era-based Alignment Orchestrator")
    parser.add_argument("-m", "--message", required=True, help="Commit message")
    parser.add_argument("--apply", action="store_true", help="Actually execute state closure")
    parsed_args = parser.parse_args(args)

    commiter = Commiter(
        apply=parsed_args.apply,
        runner=git_commit_runner,
        protocol=anchor_commit,
        message=parsed_args.message
    )
    return commiter

@contract.cli(name="align.commit", recept=[])
def main():
    dispatch_flow_cli("align.commit", entry_task, __file__)

if __name__ == "__main__":
    main()