# phase.flow.imports.aligner
## @lineage: nexus.flow.imports.aligner
## @lineage: meta.flow.imports.aligner
## @lineage: meta.flow.trans.imports.aligner
## @lineage: meta.trans.imports.aligner
## @lineage: meta.model.trans.imports.aligner
## @lineage: arch.model.trans.imports.aligner
## @lineage: arch.contract.trans.imports.aligner
## @lineage: arch.gov.trans.imports.aligner
## @lineage: arch.topos.trans.imports.aligner
## @lineage: arch.topos.node.trans.imports.aligner
## @lineage: arch.topos.bound.trans.imports.aligner
## @lineage: arch.bound.trans.imports.aligner
import os
import difflib
import libcst as cst
from typing import List, Tuple
from pathlib import Path
from xphi.arch.model.phase.aligner import PhaseAligner, AlignRecord
from fiber.phase.flow.imports.transformer import ImportTransformer, RelativeImportTransformer
from xphi.watcher.plane.emitter import get_emitter, flow_scope

def generate_diff(original: str, modified: str, filename: str) -> str:
    """원본 코드와 수정된 코드 간의 Unified Diff를 생성합니다."""
    return "\n".join(
        difflib.unified_diff(
            original.splitlines(),
            modified.splitlines(),
            fromfile=f"{filename} (original)",
            tofile=f"{filename} (modified)",
            lineterm=""
        )
    )

class PrefixImportAligner(PhaseAligner):
    """절대 경로 간의 Prefix 치환(old -> new)을 담당하는 클래스"""
    def __init__(self, root_dir: str, old_prefix: str, new_prefix: str):
        super().__init__(root_dir, emitter_name="aligner.imports.prefix")
        self.old_prefix = old_prefix
        self.new_prefix = new_prefix

    def scan(self, **kwargs) -> Tuple[List[AlignRecord], int, int]:
        mismatches = []
        matched_count = 0
        mismatched_count = 0

        with flow_scope(phase="SCAN"):
            for root, _, files in os.walk(self.root_dir):
                for file in files:
                    if not file.endswith(".py"):
                        continue
                    
                    path = Path(root) / file
                    try:
                        original = path.read_text(encoding="utf-8", errors="ignore")
                        tree = cst.parse_module(original)
                        modified_tree = tree.visit(
                            ImportTransformer(self.old_prefix, self.new_prefix)
                        )
                        modified = modified_tree.code

                        if original != modified:
                            diff = generate_diff(original, modified, str(path))
                            mismatches.append({
                                "path": str(path),
                                "original": original,
                                "modified": modified,
                                "diff": diff,
                                "status": "pending"
                            })
                            mismatched_count += 1
                            self.emitter.info(f"Mismatch detected in {file}")
                        else:
                            matched_count += 1
                    except Exception as e:
                        self.emitter.error(f"Failed scanning {path}: {e}")
                        continue

        return mismatches, matched_count, mismatched_count


class RelativeImportAligner(PhaseAligner):
    """상대 경로(., ..)를 절대 경로로 풀어주는 변환을 담당하는 클래스"""
    def __init__(self, root_dir: str, project_root: str = None):
        super().__init__(root_dir, emitter_name="relative.imports.aligner")
        self.project_root = Path(project_root).resolve() if project_root else Path.cwd()

    def scan(self, **kwargs) -> Tuple[List[AlignRecord], int, int]:
        mismatches = []
        matched_count = 0
        mismatched_count = 0
        root_path = Path(self.root_dir).resolve()

        with flow_scope(phase="SCAN"):
            for current_root, _, files in os.walk(root_path):
                for file in files:
                    if not file.endswith(".py"):
                        continue
                        
                    file_path = Path(current_root) / file
                    try:
                        rel_path = file_path.relative_to(self.project_root)
                    except ValueError:
                        rel_path = file_path.relative_to(root_path)

                    parent_dir = rel_path.parent
                    current_package = ".".join(parent_dir.parts) if str(parent_dir) != "." else ""
                    
                    try:
                        original = file_path.read_text(encoding="utf-8", errors="ignore")
                        tree = cst.parse_module(original)
                        
                        modified_tree = tree.visit(RelativeImportTransformer(current_package))
                        modified = modified_tree.code

                        if original != modified:
                            diff = generate_diff(original, modified, str(file_path))
                            mismatches.append({
                                "path": str(file_path),
                                "original": original,
                                "modified": modified,
                                "diff": diff,
                                "status": "pending"
                            })
                            mismatched_count += 1
                            self.emitter.info(f"Relative import mapped to absolute in {file}")
                        else:
                            matched_count += 1
                            
                    except Exception as e:
                        self.emitter.error(f"Failed scanning {file_path}: {e}")
                        continue

        return mismatches, matched_count, mismatched_count