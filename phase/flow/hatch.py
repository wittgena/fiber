# phase.flow.hatch
import os
import shutil
import subprocess
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        workspace_root = Path(self.root).parent
        local_xphi = workspace_root / "xphi"
        target_modules = ["arch", "kernel", "watcher"]
        
        if "force_include" not in build_data:
            build_data["force_include"] = {}

        if local_xphi.exists():
            print("[Build Hook] Local 'xphi' workspace detected.")
            for mod in target_modules:
                src = str(local_xphi / mod)
                dst = mod
                build_data["force_include"][src] = dst
        else:
            print("[Build Hook] Fetching 'xphi' from GitHub...")
            cache_dir = Path(self.root) / "phase" / "cache" / "xphi"
            
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
                
            try:
                subprocess.run(
                    ["git", "clone", "--depth", "1", "https://github.com/wittgena/xphi.git", str(cache_dir)],
                    check=True,
                    capture_output=True
                )
                print("[Build Hook] Successfully fetched 'xphi'.")
                for mod in target_modules:
                    src = str(cache_dir / mod)
                    dst = mod
                    if Path(src).exists():
                        build_data["force_include"][src] = dst
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
                print(f"[Error] Failed to clone 'xphi': {error_msg}")
                raise