# fiber.phase.hatch
import os
from pathlib import Path
from hatchling.metadata.plugin.interface import MetadataHookInterface

class CustomMetadataHook(MetadataHookInterface):
    def update(self, metadata: dict):
        ## 1. hatch-vcs가 감지한 현재 fiber의 상태
        raw_version = metadata.get("version", "0.0.0")
        
        ## 2. 상태 인지: 정식 릴리즈인가, 개발 중(Dirty/Dev)인가?
        is_clean_release = not ("dev" in raw_version or "+" in raw_version or "a" in raw_version)
        
        ## 3. 기본 타겟 결정 (자연스러운 흐름)
        if is_clean_release:
            default_target = f"v{raw_version}" # 깔끔한 릴리즈면 해당 태그를 따라감
        else:
            default_target = "main" # 개발 중(태그 없음)이면 최신(main) 브랜치를 바라봄
            
        ## 사용자가 특정 태그/브랜치를 명시하면 이를 최우선으로 적용함
        remote_ref = os.environ.get("FIBER_XPHI_REMOTE_REF", default_target)
        local_ref = os.environ.get("FIBER_XPHI_LOCAL_REF")
        
        deps = self.config.get("base_dependencies", [])
        workspace_root = Path(self.root).parent
        local_xphi = workspace_root / "xphi"
        
        force_remote = os.environ.get("FIBER_BUILD_DIST") == "1"

        ## [CASE A] 원격 강제 시 (FIBER_BUILD_DIST=1)
        if force_remote:
            print(f"[Phase: Metadata] STRICT Remote lock: xphi @ {remote_ref}")
            deps.append(f"xphi @ git+https://github.com/wittgena/xphi.git@{remote_ref}")
            
        ## [CASE B] 로컬 소스 존재 + 명시적으로 로컬 Git의 특정 커밋/태그 고정 요청 시
        elif local_xphi.exists() and local_ref:
            print(f"[Phase: Metadata] LOCAL Git lock: xphi @ {local_ref}")
            deps.append(f"xphi @ git+file://{local_xphi.resolve().as_posix()}@{local_ref}")
            
        ## [CASE C] 로컬 소스 존재 (명시적 고정 없음) -> 가장 자연스러운 현재 폴더(최신/더티) 쌩얼 상태 사용
        elif local_xphi.exists():
            print(f"[Phase: Metadata] LOCAL Source binding: xphi (Dirty state)")
            deps.append(f"xphi @ file://{local_xphi.resolve().as_posix()}")
            
        ## [CASE D] 로컬 소스 없음 (ex. Git에서 직접 설치) -> 원격 Fallback
        else:
            print(f"[Phase: Metadata] Remote Fallback: xphi @ {remote_ref}")
            deps.append(f"xphi @ git+https://github.com/wittgena/xphi.git@{remote_ref}")
            
        metadata["dependencies"] = deps