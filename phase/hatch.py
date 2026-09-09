# fiber.phase.hatch
import os
from pathlib import Path
from hatchling.metadata.plugin.interface import MetadataHookInterface

class CustomMetadataHook(MetadataHookInterface):
    def update(self, metadata: dict):
        version = metadata.get("version", "1.1.2")
        target_tag = version if version.startswith("v") else f"v{version}"
        
        deps = self.config.get("base_dependencies", [])
        
        workspace_root = Path(self.root).parent
        local_xphi = workspace_root / "xphi"
        
        force_remote = os.environ.get("FIBER_BUILD_DIST") == "1"

        if local_xphi.exists() and not force_remote:
            print(f"[Phase: Metadata] Sibling 'xphi' detected. Using local source binding for DEV mode.")
            ## 표준 PEP 508 file:// URI 포맷 주입
            deps.append(f"xphi @ file://{local_xphi.resolve().as_posix()}")
            
        ## [CASE 2] 외부 설치(USER) 또는 배포용 빌드 모드
        ## xphi를 site-packages에 병렬로 설치하되, 버전이 완벽히 일치하는 태그를 강제 동기화
        else:
            print(f"[Phase: Metadata] Injecting STRICT topology lock: xphi (Tag: {target_tag})")
            deps.append(f"xphi @ git+https://github.com/wittgena/xphi.git@{target_tag}")
        
        ## 4. 동적으로 완성된 의존성 리스트를 메타데이터에 반영
        metadata["dependencies"] = deps