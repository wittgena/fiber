# fiber.phase.hatch
import tempfile
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

        ## [CASE 1] 로컬 개발 시 (버전 무관하게 실시간 변경사항 동기화)
        if local_xphi.exists():
            print("[JIT Assembly] Sibling 'xphi' workspace detected. Using bleeding-edge local source.")
            self._inject_modules(local_xphi, target_modules, build_data)
                
        ## [CASE 2] 외부 빌드 / 배포 시 (Fiber 버전과 xphi 버전을 1:1로 락(Lock) 매칭)
        else:
            # tag 포맷이 'v1.1.0' 이라 가정 (버전 정책에 따라 'version' 그대로 사용 가능)
            target_tag = version if version.startswith("v") else f"v{version}"
            print(f"[JIT Assembly] Fetching 'xphi' (Tag: {target_tag}) to match Fiber version {version}...")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                try:
                    # Fiber의 버전과 완벽히 동일한 xphi의 태그를 가져와 융합
                    subprocess.run(
                        ["git", "clone", "--branch", target_tag, "--depth", "1", 
                         "https://github.com/wittgena/xphi.git", temp_dir],
                        check=True, capture_output=True
                    )
                    self._inject_modules(Path(temp_dir), target_modules, build_data)
                        
                except subprocess.CalledProcessError as e:
                    print(f"[FATAL] Failed to fetch xphi tag {target_tag}. Ensure repos are version-synced.")
                    raise

    def _inject_modules(self, source_root: Path, target_modules: list, build_data: dict):
        for mod in target_modules:
            src = str(source_root / mod)
            dst = f"xphi/{mod}"
            if Path(src).exists():
                build_data["force_include"][src] = dst