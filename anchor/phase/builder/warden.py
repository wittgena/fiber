# anchor.phase.builder.warden
## @lineage: anchor.phase.builder
import argparse
import subprocess
import os
import sys
import socket
import time
from enum import Enum
from pathlib import Path
from urllib.parse import urlparse
import anyio
import httpx

from anchor.phase.ingress.pypi.client import PypiMCPClient
from xphi.xor.mock.ext.vuln import ignite, _fetch_agent_intelligence

from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
REPO = "brane"

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class BuildMode(Enum):
    SECURE_LEGACY = "secure"
    MIGRATE_TOML = "toml"
    MIGRATE_UVLOCK = "uvlock"

class ProjectManager:
    """@desc: Handles project dependency file discovery and format conversion"""
    def __init__(self, target_dir: Path):
        self.target_dir = target_dir

    def resolve_and_prepare_source(self, mode: BuildMode) -> Path:
        toml_path = self.target_dir / "pyproject.toml"
        req_path = self.target_dir / "requirements.in"
        fallback_txt = self.target_dir / "requirements.txt"

        target_req = req_path if req_path.exists() else (fallback_txt if fallback_txt.exists() else None)

        if toml_path.exists():
            log.info("[*] Dependency source resolved: 'pyproject.toml'")
            return toml_path
        
        if not target_req:
            log.critical(f"🚨 No valid dependency configuration found in {self.target_dir}")
            sys.exit(1)

        if mode == BuildMode.SECURE_LEGACY:
            log.info(f"[*] Preserving legacy source: '{target_req.name}'")
            return target_req

        log.info(f"[*] Migration mode active. Converting '{target_req.name}' to 'pyproject.toml'")
        return self._convert_to_toml(target_req, toml_path)

    def _convert_to_toml(self, req_path: Path, toml_path: Path) -> Path:
        dependencies = []
        with open(req_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    dependencies.append(f'"{line}"')
        
        project_name = self.target_dir.name.lower().replace("_", "-")
        toml_content = f"""[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "{project_name}"
version = "0.1.0"
dependencies = [{', '.join(dependencies)}]
"""
        with open(toml_path, "w") as f:
            f.write(toml_content)
        
        log.info(f"   ✅ Successfully generated pyproject.toml")
        return toml_path


class SecurityWarden:
    """@desc: Manages security policies, automated authentication, and vulnerability quarantines"""
    def __init__(self, registry_url: str):
        self.registry_url = registry_url
        self.intelligence = _fetch_agent_intelligence()

    async def _fetch_dynamic_token(self) -> str:
        """
        @desc: Dynamically fetches a valid OAuth token from the Broker using the shared SDK.
               Caches the token in CredentialStore to avoid redundant network calls.
        """
        try:
            from xphi.xor.secure.auth.credentials import CredentialStore, ClientCredentialsOAuthProvider
            store = CredentialStore()
            mcp_storage = store.as_mcp_storage("brane-builder")
            
            # 1. Initialize the headless automated client provider
            provider = ClientCredentialsOAuthProvider(
                server_url="http://localhost:9000", # Broker URL
                storage=mcp_storage,
                client_id="brane-internal-client",
                client_secret="super-secret"
            )
            
            # 2. Load context from storage
            await provider._initialize()
            
            # 3. Check validity or perform token exchange
            if not provider.context.is_token_valid():
                log.info("🔐 No valid token found in Vault. Requesting new token from Broker...")
                req = await provider._perform_authorization()
                
                async with httpx.AsyncClient() as client:
                    resp = await client.send(req)
                    await provider._handle_token_response(resp)
            else:
                log.info("🔐 Using existing valid token from local Vault.")
                
            # 4. Return the valid access token
            tokens = await mcp_storage.get_tokens()
            return tokens.access_token
            
        except Exception as e:
            log.warning(f"Failed to fetch dynamic token via Broker: {e}. Falling back to default.")
            return "temp_oidc_token_12345"

    def inject_auth(self, env: dict) -> dict:
        allowed_hosts = self.intelligence.get("warden_policies", {}).get("allowed_hosts", [])
        registry_domain = urlparse(self.registry_url).hostname
        
        # Localhost bypass for testing, otherwise strictly check AuditWarden policies
        if registry_domain not in allowed_hosts and registry_domain not in ["localhost", "127.0.0.1"]:
            log.critical(f"🚨 Target registry ({registry_domain}) is not whitelisted in AuditWarden!")
            sys.exit(1)
        
        # Run async token fetching logic in a blocking manner (since uv/pip is sync)
        dynamic_token = anyio.run(self._fetch_dynamic_token)
        
        parsed = urlparse(self.registry_url)
        # Pip/uv uses Basic Auth format for private indexes: __token__:<token>@host
        injected_netloc = f"__token__:{dynamic_token}@{parsed.netloc}"
        
        env["UV_EXTRA_INDEX_URL"] = parsed._replace(netloc=injected_netloc).geturl()
        return env

    def audit_lockfile(self, lock_file: Path):
        log.info(f"🛡️ Scanning {lock_file.name} against security policies...")
        quarantine_targets = [
            t.get("legacy_path") for t in self.intelligence.get("quarantine_targets", [])
        ]
        
        if not lock_file.exists():
            log.error(f"❌ Lockfile ({lock_file.name}) generation failed silently.")
            sys.exit(1)

        with open(lock_file, "r") as f:
            lock_content = f.read()
            for bad_pkg in quarantine_targets:
                if bad_pkg and bad_pkg in lock_content:
                    log.critical(f"🚨 Security Violation: Quarantined package ('{bad_pkg}') detected.")
                    sys.exit(1)
                    
        log.info("   ✅ Passed security policy and vulnerability scan.")


class BuildEngine:
    """@desc: Executes the underlying package manager (uv)"""
    def __init__(self, auth_env: dict, target_dir: Path):
        self.env = auth_env
        self.target_dir = target_dir

    def _run(self, cmd: list):
        str_cmd = [str(arg) for arg in cmd]
        try:
            subprocess.run(str_cmd, check=True, text=True, capture_output=True, env=self.env, cwd=self.target_dir)
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Command execution failed: {' '.join(str_cmd)}\n{e.stderr}", file=sys.stderr)
            sys.exit(1)

    def generate_lockfile(self, in_file: Path, lock_file: Path, mode: BuildMode):
        if mode == BuildMode.MIGRATE_UVLOCK:
            log.info(f"🔒 [UV Mode] Generating universal lockfile (uv.lock)...")
            self._run(["uv", "lock"])
        else:
            log.info(f"🔒 [Legacy Mode] Generating hash lockfile ({lock_file.name}) based on {in_file.name}...")
            self._run(["uv", "pip", "compile", in_file, "--generate-hashes", "--prerelease=allow", "--output-file", lock_file])

    def install_packages(self, lock_file: Path, mode: BuildMode):
        log.info("📦 Installing packages with verified hash integrity...")
        if mode == BuildMode.MIGRATE_UVLOCK:
            self._run(["uv", "sync", "--no-dev"])
        else:
            self._run(["uv", "pip", "install", "-r", lock_file, "--require-hashes", "--no-deps"])
        log.info("   🎉 Secure installation complete.")

class BuilderWorkflow:
    """@desc: Orchestrates the build pipeline sequentially"""
    def __init__(self, target_dir: Path, registry_url: str, mode: BuildMode, mcp_client: PypiMCPClient = None):
        self.mode = mode
        self.target_dir = target_dir
        self.lock_file = target_dir / ("uv.lock" if mode == BuildMode.MIGRATE_UVLOCK else "requirements.lock")
        
        self.project_mgr = ProjectManager(target_dir)
        self.warden = SecurityWarden(registry_url)
        self.mcp_client = mcp_client
        self.auth_env = os.environ.copy()

    def execute(self):
        try:
            self.auth_env = self.warden.inject_auth(self.auth_env)
            in_file = self.project_mgr.resolve_and_prepare_source(self.mode)

            if self.mcp_client:
                self.mcp_client.run_diagnostics()

            engine = BuildEngine(self.auth_env, self.target_dir)
            engine.generate_lockfile(in_file, self.lock_file, self.mode)
            
            self.warden.audit_lockfile(self.lock_file)
            engine.install_packages(self.lock_file, self.mode)
        except SystemExit:
            raise
        except Exception as e:
            log.critical(f"❌ Workflow failed: {e}")
            sys.exit(1)


def _ensure_proxy_active(registry_url: str) -> PypiMCPClient:
    """@desc: Boots the MCP Hybrid Proxy via stdio if the port is not already bound."""
    parsed = urlparse(registry_url)
    if parsed.hostname not in ["localhost", "127.0.0.1"]:
        return None
        
    port = parsed.port or 8080
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex((parsed.hostname, port)) == 0:
            log.info(f"⚡ [Proxy] Port {port} is already active. Assuming server is externally managed.")
            return None

    log.info(f"🚀 [Proxy] Auto-booting internal MCP Proxy on port {port}...")
    try:
        import xphi.xor.secure.server.pypi as proxy_server
        client = PypiMCPClient(sys.executable, proxy_server.__name__)
    except Exception as e:
        log.error(f"Failed to instantiate proxy client: {e}")
        return None
    
    max_retries = 50
    for _ in range(max_retries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((parsed.hostname, port)) == 0:
                log.info("   ✅ [Proxy] Mock HTTP/MCP registry successfully booted.")
                return client
        time.sleep(0.1)

    log.critical("🚨 [Proxy] Failed to boot mock registry within the timeout period.")
    sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Secure Package Builder & Migrator")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["secure", "toml", "uvlock"], 
        default="secure",
        help="Build mode: 'secure' (legacy), 'toml' (migrate to toml), 'uvlock' (migrate to uv workspace)"
    )
    args = parser.parse_args()
    
    build_mode = BuildMode(args.mode)
    TEST_PROXY_URL = "http://127.0.0.1:8000/pypi/simple"
    
    log.info(f"[SecureBuilder] Starting in '{build_mode.name}' mode...")
    ignite()
    
    mcp_client = _ensure_proxy_active(TEST_PROXY_URL)
    workflow = BuilderWorkflow(
        target_dir=SELF_ROOT / REPO,
        registry_url=TEST_PROXY_URL,
        mode=build_mode,
        mcp_client=mcp_client
    )
    workflow.execute()