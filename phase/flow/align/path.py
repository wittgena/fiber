# phase.align.path
## @lineage: phase.flow.align.path
import os
import sys
import argparse
import re
from typing import List, Dict, Any
from pathlib import Path

from arch.contract.registry.unified import contract
from arch.model.phase.aligner import PhaseAligner, AlignRecord
from kernel.bind.resolver import find_current_self
from kernel.phase.runtime.executor.cli import execute_cli_task, CliTaskAdapter, dispatch_cli, parse_local
from watcher.plane.emitter import get_emitter

log = get_emitter("align.path")

try:
    SELF_ROOT = find_current_self()
except Exception as e:
    log.error(f"[error] 기준면(.self)을 찾을 수 없음: {e}")
    sys.exit(1)

TARGET_EXTENSIONS = {".md", ".py", ".kt", ".rs"}
EXCLUDED_DIRS = {"__pycache__", "target"}

FILE_FORMATS = {
    ".md": {"title": "# ",  "lineage": "@lineage:"},
    ".py": {"title": "# ",  "lineage": "## @lineage:"},
    ".kt": {"title": "// ", "lineage": "// @lineage:"},
    ".rs": {"title": "// ", "lineage": "// @lineage:"},
}

## @grouping.axes
def key_path(item: AlignRecord) -> str:
    namespace = item["namespace"]
    parts = namespace.split(".")
    return namespace if len(parts) <= 1 else ".".join(parts[:-1])

def key_status(item: AlignRecord) -> str:
    return item.get("status", "unknown")

def key_type(item: AlignRecord) -> str:
    return "missing" if item.get("missing") else "mismatch"

GROUP_KEYS = {
    "path": key_path,
    "status": key_status,
    "type": key_type,
}

class PathAligner(PhaseAligner):
    def check_alphabet_collision(repos: List[Path]):
        """[위상 규칙 3] 각 Repo 내 1차 폴더들의 시작 알파벳 중복 체크"""
        for repo in repos:
            first_chars = [d.name[0].upper() for d in repo.iterdir() if d.is_dir()]
            collisions = {c for c in first_chars if first_chars.count(c) > 1}
            if collisions:
                log.error(f"[topos.collision] {repo.name} has overlapping prefixes: {collisions}")

    def scan(self, only_dirs: List[str] | None = None):
        root_dir = Path(self.root_dir)
        mismatches: List[AlignRecord] = []
        matched_count = 0
        mismatched_count = 0

        log.info(f"[scan] start (root={root_dir})")
        try:
            repo_dirs = [
                d
                for d in root_dir.iterdir()
                if d.is_dir()
                and not d.name.startswith(".")
                and d.name not in EXCLUDED_DIRS
                and (only_dirs is None or d.name in only_dirs)
            ]
        except FileNotFoundError:
            log.error(f"[error] 루트 디렉토리 '{root_dir}'를 찾을 수 없습니다.")
            return [], 0, 0

        for repo_dir in repo_dirs:
            log.info(f"[scan] repo={repo_dir.name}")
            for file_path in repo_dir.rglob("*"):
                if (
                    not file_path.is_file()
                    or file_path.suffix not in TARGET_EXTENSIONS
                    or file_path.name.startswith(".")
                    or any(p in EXCLUDED_DIRS for p in file_path.parts)
                ):
                    continue

                try:
                    with file_path.open("r", encoding="utf-8") as f:
                        lines = f.readlines()

                    first_line = lines[0].strip() if lines else ""
                    
                    ## 확장자에 따른 타이틀 접두사 획득 (기본값 md)
                    ext = file_path.suffix
                    fmt = FILE_FORMATS.get(ext, FILE_FORMATS[".md"])
                    title_prefix = fmt["title"]

                    relative_path = file_path.relative_to(repo_dir)
                    expected = (
                        relative_path.with_suffix("")
                        .as_posix()
                        .replace("/", ".")
                    )
                    namespace = (
                        file_path.relative_to(root_dir)
                        .with_suffix("")
                        .as_posix()
                        .replace("/", ".")
                    )

                    ## 파일이 해당 매질의 주석 기호로 시작하지 않음
                    if not first_line.startswith(title_prefix):
                        log.warning(f"[missing] {namespace} (expected={expected})")
                        mismatched_count += 1
                        mismatches.append({"file_path": str(file_path), "namespace": namespace, "expected": expected, "actual": "", "missing": True})
                        continue
                    
                    actual_candidate = first_line[len(title_prefix):].strip()
                    
                    ## [위상 검증] 주석 기호 뒤의 내용이 유효한 네임스페이스(공백 없는 영문/숫자/점/기호)인지 확인
                    ## 일반 주석이나 코딩 선언문은 튕겨내어 본문(missing)으로 취급함
                    if not actual_candidate or not re.match(r'^[\w\.\-]+$', actual_candidate):
                        log.warning(f"[missing] {namespace} (First line is a general comment, expected={expected})")
                        mismatched_count += 1
                        mismatches.append({"file_path": str(file_path), "namespace": namespace, "expected": expected, "actual": "", "missing": True})
                        continue
                        
                    actual = actual_candidate
                    
                    ## 중복 타이틀 검증 (바로 다음 줄이 완전히 동일한 형태의 네임스페이스인 경우에만 중복으로 판별)
                    has_redundant_titles = False
                    if len(lines) > 1 and lines[1].strip().startswith(title_prefix):
                        next_actual = lines[1].strip()[len(title_prefix):].strip()
                        if next_actual in (expected, actual):
                            has_redundant_titles = True

                    if actual != expected or has_redundant_titles:
                        if actual != expected:
                            log.warning(f"[mismatch] {namespace} (actual={actual}, expected={expected})")
                        else:
                            log.warning(f"[redundant] {namespace} (correct title but duplicated below)")
                            
                        mismatched_count += 1
                        mismatches.append({"file_path": str(file_path), "namespace": namespace, "expected": expected, "actual": actual, "missing": False})
                    else:
                        matched_count += 1

                except Exception as e:
                    log.error(f"[error] {file_path}: {e}")

        log.info(f"[scan] done matched={matched_count}, mismatched={mismatched_count}")
        return mismatches, matched_count, mismatched_count

    def align(self, mismatches: List[AlignRecord], prefixes: List[str], append: bool = False):
        log.info(f"[align] start (total={len(mismatches)})")

        results = []
        for item in mismatches:
            namespace = item["namespace"]
            fixable = any(namespace.startswith(p) for p in prefixes)
            status = "mismatch"
            
            if fixable:
                try:
                    path = Path(item["file_path"])
                    
                    # 언어별 헤더 및 계보 양식 획득
                    ext = path.suffix
                    fmt = FILE_FORMATS.get(ext, FILE_FORMATS[".md"])
                    title_prefix = fmt["title"]
                    lineage_prefix = fmt["lineage"]
                    
                    with path.open("r+", encoding="utf-8") as f:
                        lines = f.readlines()
                        expected_path = item["expected"]
                        expected_title = f"{title_prefix}{expected_path}\n"
                        actual_title = item.get("actual", "")
                        
                        if append:
                            lineage_line = f"{lineage_prefix} {actual_title}\n"
                            if actual_title and actual_title != expected_path:
                                lines.insert(0, lineage_line)
                            lines.insert(0, expected_title)
                        else:
                            cleaned_lines = []
                            existing_lineages = []
                            
                            # 기존 타이틀이 유효한 네임스페이스였을 경우 계보로 예비 편입
                            if actual_title and actual_title != expected_path:
                                existing_lineages.append(actual_title)

                            in_header = True
                            is_first_line = True
                            
                            for line in lines:
                                if in_header:
                                    stripped = line.strip()
                                    
                                    # 1. Lineage 추출 (언어 불문 공통 키워드로 스캔)
                                    if "@lineage:" in stripped:
                                        val = stripped.split("@lineage:")[-1].strip()
                                        if val and val != expected_path and val not in existing_lineages:
                                            existing_lineages.append(val)
                                            
                                    # 2. 첫 줄 삭제 (유효한 기존 타이틀(Mismatch)로 판별되었을 경우에만)
                                    elif is_first_line and not item.get("missing"):
                                        pass
                                        
                                    # 3. 바로 아래 연달아 존재하는 중복 타이틀 제거
                                    elif not item.get("missing") and stripped.startswith(title_prefix) and stripped[len(title_prefix):].strip() in (expected_path, actual_title):
                                        pass
                                        
                                    # 4. 그 외 일반 주석이나 코드가 나타나면 헤더장(Field) 종료 및 본문 보존
                                    else:
                                        in_header = False
                                        cleaned_lines.append(line)
                                else:
                                    cleaned_lines.append(line)
                                
                                is_first_line = False

                            # 5. 위상 재결합: 재질별 접두사를 사용하여 최종 병합
                            final_lines = [expected_title]
                            for lin in existing_lineages:
                                final_lines.append(f"{lineage_prefix} {lin}\n")
                                
                            lines = final_lines + cleaned_lines

                        f.seek(0)
                        f.writelines(lines)
                        f.truncate()

                    status = "fixed"
                except Exception as e:
                    status = "fail"
                    item["error"] = str(e)

            item["status"] = status
            results.append(item)

        log.info(f"[align] done")
        return results

def normalize_prefix(prefix: str) -> str:
    return prefix.strip().replace("/", ".").lstrip(".")

def entry_task(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", action="append")
    parser.add_argument("--group-by", default="path", choices=["path", "status", "type"])
    parser.add_argument("--append", action="store_true", help="기존 타이틀을 대체하지 않고 최상단에 새로 추가합니다.") 
    args = parser.parse_args(args)
    
    prefixes = [normalize_prefix(p) for p in (args.repo or [])]
    only_repo = args.repo or None

    aligner = PathAligner(SELF_ROOT)
    run_kwargs = {
        "axis": args.group_by,
        "group_keys": GROUP_KEYS,
        "scan_kwargs": {"only_dirs": only_repo},
        "fix_kwargs": {"prefixes": prefixes, "append": args.append},
    }
    return CliTaskAdapter(aligner.run, **run_kwargs)

@contract.cli(name="align.path", recept=[])
def main():
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("align.path", entry_task, __file__)

if __name__ == "__main__":
    main()