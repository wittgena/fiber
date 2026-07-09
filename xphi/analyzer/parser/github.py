# xphi.analyzer.parser.github
import os
import sys
import shutil
import tempfile
import json
import subprocess
import urllib.request
from pathlib import Path

from anchor.registry.resolver.ext import ExtResolver
from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class GitHubExtractor:
    """GitHub API and Git Sparse-Checkout based source code extractor"""
    def __init__(self, category: str, name: str, dest_dir: Path, tag: str):
        self.category = category
        self.name = name
        self.dest_dir = dest_dir
        self.tag = tag
        self.target_subpath = str(ExtResolver.get("source", category=category, name=name))
        self.api_base_url = str(ExtResolver.get("api_content", category=category, name=name))
        self.git_repo_url = str(ExtResolver.get("repo"))

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