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

from anchor.registry.resolver.ext import ExtResolver
from arch.contract.registry.unified import contract, registry
from phase.bind.resolver import find_current_self, get_invoker
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

SELF_ROOT = find_current_self()

TARGET_REPO = "brane"
DEST_PATH = inter_path.__name__

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
    """GitHub API and Git Sparse-Checkout based source code extractor"""
    
    def __init__(self, category: str, name: str, dest_dir: Path, tag: str):
        self.category = category
        self.name = name
        self.dest_dir = dest_dir
        self.tag = tag
        
        # Delegate URL and path resolutions to ExtResolver
        self.target_subpath = ExtResolver.resolve_source_subpath(category, name)
        self.api_base_url = ExtResolver.resolve_github_api_contents(category, name)
        self.git_repo_url = ExtResolver.resolve_github_repo_url()

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
                self.git_repo_url, temp_dir
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
        api_target_url = f"{self.api_base_url}?ref={self.tag}"

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
    """Utility class for isolated subprocess execution and log streaming"""
    
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
    """Main pipeline orchestrating the extraction and dependency mutation of integration modules"""
    
    def __init__(self, category: str, name: str, tag: str = ExtResolver.DEFAULT_TAG):
        self.category = category
        self.name = name
        self.tag = tag
        
        # Calculate local destination path following Python package naming conventions
        category_pkg = category.replace("-", "_")
        name_pkg = name.replace("-", "_")
        
        new_path_parts = DEST_PATH.split(".")
        self.dest_dir = SELF_ROOT / TARGET_REPO / Path(*new_path_parts) / category_pkg / name_pkg
        
        # Initialize extractor using ExtResolver logic seamlessly
        self.extractor = GitHubExtractor(category, name, self.dest_dir, self.tag)

    def mutate_dependencies(self):
        """Sequential dependency substitution and path alignment within an isolated environment"""
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
        """Main synchronized execution pipeline"""
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
    parser = argparse.ArgumentParser(description="Brane Integration Extraction Transductor")
    parser.add_argument("--category", required=True, help="Integration category (e.g., readers, llms)")
    parser.add_argument("--name", required=True, help="Integration name (e.g., database, openai)")
    parser.add_argument("--tag", default=ExtResolver.DEFAULT_TAG, help=f"Target GitHub tag or branch (default: {ExtResolver.DEFAULT_TAG})")
    
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