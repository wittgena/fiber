# dphi.wasm.builder
import os
import shutil
import json
import time
from pathlib import Path

from kernel.bind.resolver import resolve_path
from watcher.tracer.bound import BaseTracer

THEORIA_ROOT = resolve_path("theoria")
TIME_ROOT = resolve_path("time")
REGISTRY_FILE = TIME_ROOT / "registry.json"

# --- 타겟 WASM 프로젝트 목록 정의 ---
# 각 프로젝트별로 필요한 환경 변수(env)를 독립적으로 설정할 수 있습니다.
WASM_PROJECTS = [
    {"name": "dphi"},
    {"name": "dvm"},
    {
        "name": "rpy", 
        "env": {"RUSTFLAGS": '--cfg getrandom_backend="custom"'}
    }
]


class WasmBuilder(BaseTracer):
    """WASM 컴파일 (Dphi, DVM, RPY) 및 Rust-Driven JSON 스키마 자동 추출 페이즈"""
    def __init__(self, timeout: int = 120):
        super().__init__(tracer_name="wasm.builder", timeout=timeout)
        self.build_error = ""
        
        cargo_path = str(Path.home() / ".cargo" / "bin")
        if cargo_path not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{cargo_path}:{os.environ.get('PATH', '')}"

    async def generate_schema_from_rust(self) -> bool:
        self.log.info("[Builder] Extracting JSON Schema using Standard Binary...")
        os.makedirs(TIME_ROOT, exist_ok=True)
        
        # Schema 추출은 dphi 프로젝트의 bin을 사용
        dphi_dir = THEORIA_ROOT / "dphi"
        
        code, out, err = await self.boundary.run_command(
            ["cargo", "run", "--bin", "schema", "--quiet"], 
            cwd=str(dphi_dir), capture=True
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
            self.log.error(f"--- [RAW STDOUT] ---\n{out.strip() if out else 'No STDOUT'}")
            return False
        except Exception as e:
            self.log.error(f"[ERROR] Unexpected error during schema processing: {e}")
            return False

    async def _compile_wasm_project(self, project_dir: Path, name: str, custom_env: dict = None) -> bool:
        """개별 Rust 프로젝트를 WASM으로 컴파일하는 헬퍼 메서드"""
        if not project_dir.exists():
            self.log.error(f"[Builder] Project directory not found: {project_dir}")
            return False

        # 기존 전역 환경변수 백업
        original_env = os.environ.copy()
        
        try:
            # 커스텀 환경변수가 있다면 현재 프로세스의 os.environ에 임시 병합
            if custom_env:
                os.environ.update(custom_env)
                self.log.info(f"[Builder] Applying custom env for {name}: {custom_env}")

            self.log.info(f"[Builder] Compiling {name} to wasm32-unknown-unknown...")
            
            # env 인자 없이 수정된 전역 환경변수 상태로 실행
            code, out, err = await self.boundary.run_command(
                ["cargo", "build", "--target", "wasm32-unknown-unknown", "--release"], 
                cwd=str(project_dir), 
                capture=True
            )
            
            if code != 0:
                self.build_error = err
                self.log.error(f"[Builder] {name} Compilation Failed:\n{err.strip() if err else 'No error message'}")
                return False
                
            return True
            
        finally:
            # 명령어 실행 후 성공/실패 여부와 관계없이 원래 환경변수로 완벽히 복구
            os.environ.clear()
            os.environ.update(original_env)

    async def build_and_deploy(self) -> bool:
        # 1. WASM 타겟 환경 준비
        await self.boundary.run_command(
            ["rustup", "target", "add", "wasm32-unknown-unknown"], 
            cwd=str(THEORIA_ROOT) 
        )
        
        os.makedirs(TIME_ROOT, exist_ok=True)

        # 2. 프로젝트 리스트 순회하며 컴파일 및 복사
        for proj in WASM_PROJECTS:
            proj_name = proj["name"]
            proj_env = proj.get("env", None)
            proj_dir = THEORIA_ROOT / proj_name
            
            # 컴파일 진행 (custom_env 전달)
            if not await self._compile_wasm_project(proj_dir, proj_name, proj_env):
                return False

            # 아티팩트 경로 설정
            build_file = proj_dir / "target" / "wasm32-unknown-unknown" / "release" / f"{proj_name}.wasm"
            dest_file = TIME_ROOT / f"{proj_name}.wasm"

            # 복사 및 배포
            try:
                shutil.copy2(build_file, dest_file)
                self.log.info(f"[Builder] Copied artifact -> {dest_file.name}")
            except FileNotFoundError as e:
                self.log.error(f"[Builder] Deployment Failed for {proj_name}. Artifact not found: {e}")
                return False

        return True

    async def execute(self) -> None:
        project_names = [p["name"] for p in WASM_PROJECTS]
        self.log.info(f"\n--- [START] Compiling WASM Artifacts ({', '.join(project_names).upper()}) ---")
        
        if not await self.generate_schema_from_rust():
            self.rupture_confirmed = True
            return
            
        if not await self.build_and_deploy():
            self.rupture_confirmed = True


if __name__ == "__main__":
    import asyncio
    
    async def main():
        builder = WasmBuilder()
        print("Starting WasmBuilder...")
        await builder.execute()
        
        if getattr(builder, 'rupture_confirmed', False):
            print("[Main] Build failed. Check the logs for details.")
        else:
            print("[Main] Build and deployment completed successfully!")

    asyncio.run(main())