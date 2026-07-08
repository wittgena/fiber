# anchor.cli.adapter.trans.llama
import os
import sys
import shutil
import tempfile
import argparse
import json
import threading
import subprocess
import urllib.request
from urllib.error import URLError, HTTPError
from pathlib import Path

import anchor.inter as inter_path
import anchor.inter.bound as llama_bound
import xphi.loop as xphi_loop
import xphi.loop.flow as xphi_flow

from arch.contract.registry.unified import contract, registry
from phase.bind.resolver import find_current_self, get_invoker
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

SELF_ROOT = find_current_self()

ACCOUNT = "ext-phase"
REPO = "inter-llama"
GITHUB_API_BASE = f"https://api.github.com/repos/{ACCOUNT}/{REPO}/contents"
GITHUB_REPO_URL = f"https://github.com/{ACCOUNT}/{REPO}.git"

TARGET_REPO = "brane"
DEST_PATH = inter_path.__name__
DEFAULT_TAG =  "v0.14.22"

LLAMA_BOUND_PATH = llama_bound.__name__
LOOP_PATH = xphi_loop.__name__
FLOW_PATH = xphi_flow.__name__

IMPORT_ALIGN_MAP = [
    ("llama_index.core.readers", f"{FLOW_PATH}.reader"),
    ("llama_index.core.embeddings", f"{FLOW_PATH}.embedding"),
    ("llama_index.core.llms", f"{FLOW_PATH}.llm"),
    ("llama_index.core.program", f"{LOOP_PATH}.prog"),
    ("llama_index.core.instrumentation", f"{LOOP_PATH}.inst"),
    ("llama_index.core.callbacks", f"{LOOP_PATH}.callback" ),
    ("llama_index.core", LLAMA_BOUND_PATH),
]

class GitHubExtractor:
    """GitHub API 및 Git Sparse-Checkout을 활용한 소스코드 추출기"""
    def __init__(self, target_subpath: str, dest_dir: Path, tag: str):
        self.target_subpath = target_subpath
        self.dest_dir = dest_dir
        self.tag = tag

    def _run_command_sync(self, cmd: list, cwd=None, check=True):
        log.signal(f"[RUN] {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=cwd, check=check, text=True, capture_output=True)
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                log.info(f"  | {line}")
        return result

    def _download_via_api(self, api_url: str, current_dest: Path) -> int:
        req = urllib.request.Request(api_url, headers={'User-Agent': 'Theoria-Mutation-Agent'})
        with urllib.request.urlopen(req) as response:
            items = json.loads(response.read().decode())

        if not isinstance(items, list):
            items = [items] 

        download_count = 0
        for item in items:
            if item["type"] == "file":
                if item["name"] in ["__init__.py", "README.md"]:
                    continue
                
                dest_file = current_dest / item["name"]
                log.info(f"  -> Downloading: {item['name']}")
                
                dl_req = urllib.request.Request(item["download_url"], headers={'User-Agent': 'Theoria-Mutation-Agent'})
                with urllib.request.urlopen(dl_req) as dl_resp, open(dest_file, 'wb') as f:
                    f.write(dl_resp.read())
                download_count += 1
            elif item["type"] == "dir":
                new_dest = current_dest / item["name"]
                new_dest.mkdir(parents=True, exist_ok=True)
                download_count += self._download_via_api(item["url"], new_dest)
                
        return download_count

    def _fallback_git_sparse_checkout(self):
        log.info(f"[*] Executing Git Sparse-Checkout bypass strategy (Branch/Tag: {self.tag})...")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            self._run_command_sync([
                "git", "clone", "--depth", "1", 
                "--filter=blob:none", "--sparse", 
                "--branch", self.tag, 
                GITHUB_REPO_URL, temp_dir
            ])
            self._run_command_sync(["git", "sparse-checkout", "set", self.target_subpath], cwd=temp_dir)

            source_dir = temp_path / self.target_subpath
            if not source_dir.exists():
                raise FileNotFoundError(f"[CRITICAL] Path not found in GitHub repository: {self.target_subpath}")

            shutil.copytree(source_dir, self.dest_dir, dirs_exist_ok=True)
            
            for init_file in self.dest_dir.rglob("__init__.py"):
                init_file.unlink()
            log.info("[+] Git Fallback extraction and cleanup completed.")

    def fetch(self):
        log.info(f"[*] Synthesized source path: {self.target_subpath}")
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        api_target_url = f"{GITHUB_API_BASE}/{self.target_subpath}?ref={self.tag}"

        try:
            log.info("[*] Attempting file-level extraction via GitHub API...")
            count = self._download_via_api(api_target_url, self.dest_dir)
            if count > 0:
                log.info(f"[+] Success: Directly downloaded {count} files.")
                return 
            else:
                log.warning("[-] No files downloaded. Switching to Git Fallback.")
        except Exception as e:
            log.warning(f"[-] Unexpected error during API download ({e}). Switching to Git Fallback.")

        self._fallback_git_sparse_checkout()

class IsolatedProcessRunner:
    """독립된 서브프로세스 실행 및 로그 스트리밍을 담당하는 유틸리티 클래스"""
    @staticmethod
    def execute_subtask(command_name: str, args: list):
        log.signal(f"[Sub-Task] Resolving contract for isolated execution: {command_name}")
        task_info_list = registry.registered_cli_tasks.get(command_name)
        if not task_info_list:
            raise RuntimeError(f"Cannot find contract '{command_name}' in registry.")

        module_fqn = task_info_list[0].get("module_fqn")
        cmd = [sys.executable, "-m", module_fqn] + args
        log.info(f"  -> Spawning Subprocess: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(SELF_ROOT),
            text=True,
            bufsize=1
        )

        def stream_reader(stream, prefix=""):
            for line in stream:
                text = line.strip()
                if text:
                    log.info(f"  {prefix}| {text}")

        t_out = threading.Thread(target=stream_reader, args=(process.stdout, ""))
        t_err = threading.Thread(target=stream_reader, args=(process.stderr, "[ERR]"))
        t_out.start()
        t_err.start()

        t_out.join()
        t_err.join()
        process.wait()
        
        if process.returncode != 0:
            raise RuntimeError(f"Sub-task '{command_name}' failed with return code {process.returncode}")
        
        return True

class LlamaTransductor:
    """LlamaIndex 통합 모듈 추출 및 의존성 변환을 지휘하는 메인 파이프라인"""
    
    def __init__(self, category: str, name: str, tag: str = DEFAULT_TAG):
        self.category = category
        self.name = name
        self.tag = tag
        
        category_dir = category.replace("_", "-")
        name_dir = name.replace("_", "-")
        category_pkg = category.replace("-", "_")
        name_pkg = name.replace("-", "_")
        
        self.target_subpath = (
            f"llama-index-integrations/{category_pkg}/"
            f"llama-index-{category_dir}-{name_dir}/"
            f"llama_index/{category_pkg}/{name_pkg}"
        )

        new_path_parts = DEST_PATH.split(".")
        self.dest_dir = SELF_ROOT / TARGET_REPO / Path(*new_path_parts) / category_pkg / name_pkg
        self.extractor = GitHubExtractor(self.target_subpath, self.dest_dir, self.tag)

    def mutate_dependencies(self):
        """다중 의존성 치환 및 경로 정렬 (격리된 실행 환경)"""
        log.info("\n## @mutate: Isolated Subprocess Execution")
        log.signal("[TASK] Executing 'align.imports' sequentially...")
        
        for old_pkg, new_pkg in IMPORT_ALIGN_MAP:
            log.info(f"  -> Aligning import: '{old_pkg}' to '{new_pkg}'")
            IsolatedProcessRunner.execute_subtask(
                command_name="align.imports", 
                args=["--local", "--old", old_pkg, "--new", new_pkg, "--repo", TARGET_REPO]
            )
        
        log.signal("[TASK] Executing 'align.path' sequentially...")
        IsolatedProcessRunner.execute_subtask(
            command_name="align.path", 
            args=["--local", "--repo", TARGET_REPO]
        )
        log.info("\n[SUCCESS] All isolated mutation processes completed.")

    def run(self):
        """가독성을 극대화한 메인 동기화 파이프라인"""
        if not (SELF_ROOT / TARGET_REPO).is_dir():
            log.error(f"[ERROR] Cannot find repository '{TARGET_REPO}'. Make sure it is at the top of '.self/'.")
            sys.exit(1)
            
        try:
            log.info(f"## @extract: '{self.category}/{self.name}' [Tag: {self.tag}]")
            
            self.extractor.fetch()
            self.mutate_dependencies()
            
        except Exception as e:
            log.error(f"\n[CRITICAL ERROR] Pipeline halted: {e}")
            raise

def entry_task(args):
    parser = argparse.ArgumentParser(description="Brane LlamaIndex Integration Transductor")
    parser.add_argument("--category", required=True, help="Integration category (e.g., readers, llms)")
    parser.add_argument("--name", required=True, help="Integration name (e.g., database, openai)")
    parser.add_argument("--tag", default=DEFAULT_TAG, help=f"Target GitHub tag or branch (default: {DEFAULT_TAG})")
    
    parsed_args = parser.parse_args(args)
    runner = LlamaTransductor(category=parsed_args.category, name=parsed_args.name, tag=parsed_args.tag)
    return CliTaskAdapter(runner.run)

@contract.cli(
    name=MODULE_NAMESPACE,
    args=["--category", "--name", "--tag"],
    tags=["llama", "trans"],
    entry="entry_task" 
)
def main(args=None):
    if args is not None:
        return entry_task(args)
        
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli(MODULE_NAMESPACE, entry_task, __file__)

if __name__ == "__main__":
    main()