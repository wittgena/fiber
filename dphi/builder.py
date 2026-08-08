# dphi.builder
## @lineage: epoch.time.dphi.builder
import os
import shutil
import json
import time
from pathlib import Path

from kernel.bind.resolver import resolve_path
from watcher.tracer.bound import BaseTracer

THEORIA_ROOT = resolve_path("theoria")
TIME_ROOT = resolve_path("time")

# --- [1] Dphi WASM 경로 설정 ---
WASM_DIR = THEORIA_ROOT / "dphi"
WASM_TARGET_DIR = WASM_DIR / "target" / "wasm32-unknown-unknown" / "release"
WASM_BUILD_FILE = WASM_TARGET_DIR / "dphi.wasm"
DEST_WASM_FILE = TIME_ROOT / "dphi.wasm"

# --- [2] DVM WASM 경로 설정 추가 ---
DVM_DIR = THEORIA_ROOT / "dvm"
DVM_TARGET_DIR = DVM_DIR / "target" / "wasm32-unknown-unknown" / "release"
DVM_BUILD_FILE = DVM_TARGET_DIR / "dvm.wasm"
DVM_DEST_FILE = TIME_ROOT / "dvm.wasm"

REGISTRY_FILE = TIME_ROOT / "registry.json"

class WasmBuilder(BaseTracer):
    """WASM 컴파일 (Dphi & REVM) 및 Rust-Driven JSON 스키마 자동 추출 페이즈"""
    def __init__(self, timeout: int = 120):
        super().__init__(tracer_name="wasm.builder", timeout=timeout)
        self.build_error = ""
        
        cargo_path = str(Path.home() / ".cargo" / "bin")
        if cargo_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{cargo_path}:{os.environ.get('PATH', '')}"

    async def generate_schema_from_rust(self) -> bool:
        self.log.info("[Builder] Extracting JSON Schema using Standard Binary...")
        os.makedirs(TIME_ROOT, exist_ok=True)
        
        code, out, err = await self.boundary.run_command(
            ["cargo", "run", "--bin", "schema", "--quiet"], 
            cwd=str(WASM_DIR), capture=True
        )
        
        if code != 0:
            self.build_error = f"Schema Binary Failed (Exit code: {code})"
            self.log.error(f"[ERROR] {self.build_error}")
            self.log.error(f"--- [Cargo STDERR] ---\n{err.strip() if err else 'No STDERR'}")
            return False

        try:
            schemas = json.loads(out.strip())
            
            methods_schema = schemas.get("Method", {})
            methods_list = methods_schema.get("enum", [])
            
            reg_data = {
                "generated_at": time.time(),
                "methods": methods_list,
                "schema_version": "Draft-07"
            }
            
            with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
                json.dump(reg_data, f, separators=(',', ':'))
                
            schema_file = TIME_ROOT / "schema.json"
            with open(schema_file, "w", encoding="utf-8") as f:
                json.dump(schemas, f, separators=(',', ':'))
                
            self.log.info(f"[Builder] Standard Schema Extraction Complete ({len(methods_list)} methods).")
            return True
            
        except json.JSONDecodeError as e:
            self.log.error(f"[ERROR] Failed to decode STDOUT as JSON: {e}")
            self.log.error(f"--- [RAW STDOUT] ---\n{out.strip()[:1000]}")
            return False
        except Exception as e:
            self.log.error(f"[ERROR] Unexpected error during schema processing: {e}")
            return False

    async def _compile_wasm_project(self, project_dir: Path, name: str) -> bool:
        """개별 Rust 프로젝트를 WASM으로 컴파일하는 헬퍼 메서드"""
        if not project_dir.exists():
            self.log.error(f"[Builder] Project directory not found: {project_dir}")
            return False

        self.log.info(f"[Builder] Compiling {name} to wasm32-unknown-unknown...")
        code, out, err = await self.boundary.run_command(
            ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
            cwd=str(project_dir), capture=True
        )
        
        if code != 0:
            self.build_error = err
            self.log.error(f"[Builder] {name} Compilation Failed:\n{err[:500]}...")
            return False
            
        return True

    async def build_and_deploy(self) -> bool:
        # 1. WASM 타겟 환경 준비 (최초 1회만 실행되어도 무방함)
        await self.boundary.run_command(
            ["rustup", "target", "add", "wasm32-unknown-unknown"], 
            cwd=str(THEORIA_ROOT) 
        )
        
        # 2. Dphi 프로젝트 컴파일
        if not await self._compile_wasm_project(WASM_DIR, "dphi"):
            return False

        # 3. REVM 프로젝트 컴파일
        if not await self._compile_wasm_project(DVM_DIR, "revm"):
            return False

        # 4. 아티팩트 복사 및 배포
        os.makedirs(TIME_ROOT, exist_ok=True)
        
        try:
            shutil.copy2(WASM_BUILD_FILE, DEST_WASM_FILE)
            self.log.info(f"[Builder] Copied artifact -> {DEST_WASM_FILE.name}")
            
            shutil.copy2(DVM_BUILD_FILE, DVM_DEST_FILE)
            self.log.info(f"[Builder] Copied artifact -> {DVM_DEST_FILE.name}")
        except FileNotFoundError as e:
            self.log.error(f"[Builder] Deployment Failed. Artifact not found: {e}")
            return False

        return True

    async def execute(self) -> None:
        self.log.info("\n--- [START] Compiling WASM Artifacts (Dphi & REVM) ---")
        if not await self.generate_schema_from_rust():
            self.rupture_confirmed = True
            return
            
        if not await self.build_and_deploy():
            self.rupture_confirmed = True