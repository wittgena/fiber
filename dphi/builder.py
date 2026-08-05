# dphi.builder
## @lineage: watcher.wasm.builder
import os
import shutil
import json
import time
from pathlib import Path

from kernel.bind.resolver import resolve_path
from watcher.tracer.bound import BaseTracer

THEORIA_ROOT = resolve_path("theoria")
TIME_ROOT = resolve_path("time")
WASM_DIR = THEORIA_ROOT / "dphi"
WASM_TARGET_DIR = WASM_DIR / "target" / "wasm32-unknown-unknown" / "release"
WASM_BUILD_FILE = WASM_TARGET_DIR / "dphi.wasm"
DEST_WASM_FILE = TIME_ROOT / "dphi.wasm"
REGISTRY_FILE = TIME_ROOT / "registry.json"

class WasmBuilder(BaseTracer):
    """WASM 컴파일 및 Rust-Driven JSON 스키마 자동 추출 페이즈"""
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
            
            # [변경됨] indent=4 제거, separators=(',', ':') 추가로 한 줄로 압축
            with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
                json.dump(reg_data, f, separators=(',', ':'))
                
            schema_file = TIME_ROOT / "schema.json"
            # [변경됨] indent=4 제거, separators=(',', ':') 추가로 한 줄로 압축
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

    async def build_and_deploy(self) -> bool:
        await self.boundary.run_command(
            ["rustup", "target", "add", "wasm32-unknown-unknown"], 
            cwd=str(WASM_DIR)
        )
        
        code, out, err = await self.boundary.run_command(
            ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
            cwd=str(WASM_DIR), capture=True
        )
        
        if code != 0:
            self.build_error = err
            self.log.error(f"[Builder] Compilation Failed:\n{err[:500]}...")
            return False

        os.makedirs(TIME_ROOT, exist_ok=True)
        shutil.copy2(WASM_BUILD_FILE, DEST_WASM_FILE)
        self.log.info(f"[Builder] Copied artifact -> {DEST_WASM_FILE.name}")
        return True

    async def execute(self) -> None:
        self.log.info("\n--- [START] Compiling WASM Artifact & Schemas ---")
        if not await self.generate_schema_from_rust():
            self.rupture_confirmed = True
            return
            
        if not await self.build_and_deploy():
            self.rupture_confirmed = True