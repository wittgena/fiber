# fiber.phase.flow.hatch
## @lineage: phase.flow.hatch
import os
import shutil
import subprocess
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        workspace_root = Path(self.root).parent
        local_xphi = workspace_root / "xphi"
        
        # 통합할 xphi 측 타겟 모듈들
        target_modules = ["arch", "kernel", "watcher"]
        
        if "force_include" not in build_data:
            build_data["force_include"] = {}

        # [CASE 1] 로컬 (self) 환경: 상위 디렉토리에 xphi가 존재하는 경우
        if local_xphi.exists():
            print("[Build Hook] Local 'xphi' workspace detected.")
            for mod in target_modules:
                src = str(local_xphi / mod)
                # 최종 패키지 내부에 xphi/arch, xphi/kernel 형태로 주입
                dst = f"xphi/{mod}"
                build_data["force_include"][src] = dst
                
        # [CASE 2] 사용자 환경: 패키지 설치 시 xphi가 없어 GitHub에서 Fetch
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
                    # 다운로드한 코드 역시 xphi/ 네임스페이스 하위로 매핑
                    dst = f"xphi/{mod}"
                    if Path(src).exists():
                        build_data["force_include"][src] = dst
                        
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
                print(f"[Error] Failed to clone 'xphi': {error_msg}")
                raise