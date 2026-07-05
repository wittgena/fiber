# anchor.bind.hatch
# anchor/bind/build.py
import os
import shutil
import subprocess
from pathlib import Path
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        """Hatchling 빌드 파이프라인 개입 훅"""
        # self/ 루트 탐색 로직 (현재 위치: self/brane/anchor/bind/build.py)
        workspace_root = Path(self.root).parent
        local_theoria = workspace_root / "theoria"
        target_modules = ["arch", "phase", "watcher"]
        
        # 동적 파일 주입을 위한 force_include 딕셔너리 초기화
        if "force_include" not in build_data:
            build_data["force_include"] = {}

        # [CASE 1] 로컬 (self/theoria) 환경
        if local_theoria.exists():
            print("[Brane Build Hook] Local 'theoria' workspace detected.")
            for mod in target_modules:
                src = str(local_theoria / mod)
                dst = f"brane/{mod}"
                # 로컬의 theoria/arch를 패키지의 brane/arch로 직접 주입
                build_data["force_include"][src] = dst
                
        # [CASE 2] 사용자 환경 (site-packages 설치 시)
        else:
            print("[Brane Build Hook] Fetching 'theoria' from GitHub...")
            # _가 들어가지 않는 깔끔한 내부 캐시 폴더 사용
            cache_dir = Path(self.root) / "anchor" / "bind" / "cache" / "theoria"
            
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
                
            try:
                subprocess.run(
                    ["git", "clone", "--depth", "1", "https://github.com/wittgena/theoria.git", str(cache_dir)],
                    check=True,
                    capture_output=True
                )
                print("[Brane Build Hook] Successfully fetched 'theoria'.")
                
                # 다운로드한 코드들을 패키지의 brane/ 네임스페이스로 직접 주입
                for mod in target_modules:
                    src = str(cache_dir / mod)
                    dst = f"brane/{mod}"
                    if Path(src).exists():
                        build_data["force_include"][src] = dst
                        
            except subprocess.CalledProcessError as e:
                print(f"[Error] Failed to clone 'theoria': {e.stderr.decode()}")
                raise