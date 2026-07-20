# anchor.bind.hatch
import os
import shutil
import subprocess
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        """Hatchling 빌드 파이프라인 개입 훅"""
        workspace_root = Path(self.root).parent
        xphi = workspace_root / "xphi"
        target_modules = ["arch", "phase", "watcher"]
        if "force_include" not in build_data:
            build_data["force_include"] = {}

        if xphi.exists():
            print("[Brane Build Hook] Local 'theoria' workspace detected.")
            for mod in target_modules:
                src = str(xphi / mod)
                dst = f"brane/{mod}"
                build_data["force_include"][src] = dst
                
        else:
            print("[Brane Build Hook] Fetching 'theoria' from GitHub...")
            cache_dir = Path(self.root) / "anchor" / "bind" / "cache" / "xphi"
            
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
                
            try:
                subprocess.run(
                    ["git", "clone", "--depth", "1", "https://github.com/wittgena/xphi.git", str(cache_dir)],
                    check=True,
                    capture_output=True
                )
                print("[Brane Build Hook] Successfully fetched 'xphi'.")
                for mod in target_modules:
                    src = str(cache_dir / mod)
                    dst = f"brane/{mod}"
                    if Path(src).exists():
                        build_data["force_include"][src] = dst
                        
            except subprocess.CalledProcessError as e:
                print(f"[Error] Failed to clone 'theoria': {e.stderr.decode()}")
                raise