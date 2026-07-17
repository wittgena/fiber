# bound.watcher.audit.node.scanner
## @lineage: arch.topos.node.scanner
## @lineage: phase.gov.repo.scanner
"""
@topos: global self-topology collapse operator
@flow: Φ_total → Φ′_self → Φₓ(anchor)
@role: anchor.align.commit performs single-snapshot anchoring across all repos under self
"""
import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from watcher.plane.emitter import get_emitter

log = get_emitter("repo.scanner", mode="SLIM")

class NodeCommit:
    """@topos.role: local Φ fragment (sub-topology unit)"""
    def __init__(self, path: Path):
        self.path = path

    @property
    def name(self) -> str:
        return self.path.name

    def get_status(self) -> str:
        """@topos.op: detect local ∂Φ (boundary delta)"""
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=self.path, capture_output=True, text=True
        )
        return result.stdout.strip()

    def commit(self, message: str, apply: bool = False):
        """@topos.op: local anchoring attempt (Φ fragment → Φₓ)"""
        status = self.get_status()
        if not status:
            return False

        change_count = len(status.splitlines())
        log.info(f"[{self.name}] detected changes: {change_count}")

        if not apply:
            log.info(f" └─ [DRY-RUN] commit skipped")
            return False

        try:
            subprocess.run(["git", "add", "-A"], cwd=self.path, check=True)
            subprocess.run(["git", "commit", "-m", message], cwd=self.path, check=True)
            log.signal(f" └─ [DONE] commit completed")
            return True
        except subprocess.CalledProcessError as e:
            log.error(f" └─ [FAILED] commit failed: {e}")
            return False

class NodeScanner:
    """@topos.role: Φ constructor (global topology discovery)"""
    def __init__(self, root_path: Path):
        self.root = root_path
        log.info(f"[RepoScanner] root_path: {self.root}")

    def _is_git(self, path: Path) -> bool:
        return (path / ".git").is_dir()

    def scan(self, depth: int = 2) -> List[NodeCommit]:
        repos = []
        log.info(f"scan start: {self.root} (Max Depth: {depth})")
        
        for entry in self.root.iterdir():
            if not (entry.is_dir() or entry.is_symlink()): continue
            
            if self._is_git(entry):
                repos.append(NodeCommit(entry))
            
            if depth > 1:
                for sub in entry.iterdir():
                    if (sub.is_dir() or sub.is_symlink()) and self._is_git(sub):
                        repos.append(NodeCommit(sub))
        
        log.info(f"total repositories found: {len(repos)}")
        for repo in repos:
            log.info(f"repo.path: {repo.path}")
        return repos
