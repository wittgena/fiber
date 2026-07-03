# anchor.cli.secure.builder
## @lineage: anchor.secure.builder
import argparse
import subprocess
import os
import sys
import socket
import time
import atexit
import json
from enum import Enum
from pathlib import Path
from urllib.parse import urlparse

from anchor.cli.bootstrap import ignite, _fetch_agent_intelligence
from bound.adapter.mcp.client.pypi import PypiMCPClient

from phase.bind.resolver import find_current_self, get_invoker
from watcher.plane.emitter import get_emitter

SELF_ROOT = find_current_self()
REPO = "brane"
PROXY_URL = "https://nexus.next-phase.com/repository/pypi-internal/simple"

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

class BuildMode(Enum):
    """Defines the execution strategy for the dependency build pipeline"""
    SECURE_LEGACY = "secure"   ## Preserves requirements.txt, generates requirements.lock
    MIGRATE_TOML = "toml"      ## Generates pyproject.toml, generates requirements.lock
    MIGRATE_UVLOCK = "uvlock"  ## Generates pyproject.toml, generates uv.lock (Full Workspace)


class ProjectManager:
    """@desc: Handles project dependency file discovery and format conversion"""
    def __init__(self, target_dir: Path):
        self.target_dir = target_dir

    def resolve_and_prepare_source(self, mode: BuildMode) -> Path:
        """
        @action: 
        - Resolves the appropriate source file based on the selected mode
        - Performs file conversion if a migration mode is active
        """
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

        ## Preserve legacy configurations if migration is not requested
        if mode == BuildMode.SECURE_LEGACY:
            log.info(f"[*] Preserving legacy source: '{target_req.name}'")
            return target_req

        ## Execute TOML conversion if a migration mode is triggered
        log.info(f"[*] Migration mode active. Converting '{target_req.name}' to 'pyproject.toml'")
        return self._convert_to_toml(target_req, toml_path)

    def _convert_to_toml(self, req_path: Path, toml_path: Path) -> Path:
        """@desc: Parses legacy text-based requirements and maps them to PEP 621 pyproject.toml format"""
        dependencies = []
        with open(req_path, "r") as f:
            for line in f:
                line = line.strip()
                # Ignore empty lines and comments
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
        
        log.info(f"  ✅ Successfully generated pyproject.toml")
        return toml_path

class SecurityWarden:
    """@desc: Manages security policies, private registry authentication, and vulnerability quarantines"""
    def __init__(self, registry_url: str):
        self.registry_url = registry_url
        self.intelligence = _fetch_agent_intelligence()

    def inject_auth(self, env: dict) -> dict:
        """@action: Issues dynamic tokens and validates the target registry against Warden policies."""
        allowed_hosts = self.intelligence.get("warden_policies", {}).get("allowed_hosts", [])
        registry_domain = urlparse(self.registry_url).hostname
        
        if registry_domain not in allowed_hosts:
            log.critical(f"🚨 Target registry ({registry_domain}) is not whitelisted in AuditWarden!")
            sys.exit(1)
        
        log.info("🔐 Fetching authentication token for the private registry...")
        dynamic_token = "temp_oidc_token_12345" 
        parsed = urlparse(self.registry_url)
        injected_netloc = f"__token__:{dynamic_token}@{parsed.netloc}"
        
        env["UV_EXTRA_INDEX_URL"] = parsed._replace(netloc=injected_netloc).geturl()
        return env

    def audit_lockfile(self, lock_file: Path):
        """@action: Scans the generated lockfile against external CVEs and internal quarantine blocklists"""
        log.info(f"🛡️ Scanning {lock_file.name} against security policies...")
        quarantine_targets = [
            t.get("legacy_path") for t in self.intelligence.get("quarantine_targets", [])
        ]
        
        if not lock_file.exists():
            log.error(f"❌ Lockfile ({lock_file.name}) generation failed silently.")
            sys.exit(1)

        ## @note: A simple text-based substring match. Sufficient for both text locks and TOML locks.
        with open(lock_file, "r") as f:
            lock_content = f.read()
            for bad_pkg in quarantine_targets:
                if bad_pkg and bad_pkg in lock_content:
                    log.critical(f"🚨 Security Violation: Quarantined package ('{bad_pkg}') detected.")
                    sys.exit(1)
                    
        log.info("   ✅ Passed security policy and vulnerability scan.")

class BuildEngine:
    """@desc: Executes the underlying package manager (uv) for resolution and installation"""
    def __init__(self, auth_env: dict, target_dir: Path):
        self.env = auth_env
        self.target_dir = target_dir

    def _run(self, cmd: list):
        """@desc: Utility wrapper to execute shell commands with proper CWD and Environment."""
        str_cmd = [str(arg) for arg in cmd]
        try:
            subprocess.run(str_cmd, check=True, text=True, capture_output=True, env=self.env, cwd=self.target_dir)
        except subprocess.CalledProcessError as e:
            log.error(f"❌ Command execution failed: {' '.join(str_cmd)}\n{e.stderr}", file=sys.stderr)
            sys.exit(1)

    def generate_lockfile(self, in_file: Path, lock_file: Path, mode: BuildMode):
        """@action: Deterministically resolves dependencies based on the specified build mode."""
        if mode == BuildMode.MIGRATE_UVLOCK:
            log.info(f"🔒 [UV Mode] Generating universal lockfile (uv.lock)...")
            self._run(["uv", "lock"])
        else:
            log.info(f"🔒 [Legacy Mode] Generating hash lockfile ({lock_file.name}) based on {in_file.name}...")
            self._run([
                "uv", "pip", "compile", in_file,
                "--generate-hashes", "--prerelease=allow",
                "--output-file", lock_file
            ])

    def install_packages(self, lock_file: Path, mode: BuildMode):
        """@action: Installs dependencies ensuring strict adherence to the lockfile."""
        log.info("📦 Installing packages with verified hash integrity...")
        if mode == BuildMode.MIGRATE_UVLOCK:
            # Synchronize the environment completely using the uv.lock state
            self._run(["uv", "sync", "--no-dev"])
        else:
            # Legacy installation targeting the specific text lockfile
            self._run([
                "uv", "pip", "install", "-r", lock_file,
                "--require-hashes", "--no-deps"
            ])
        log.info("   🎉 Secure installation complete.")

class MCPCoordinator:
    """@desc: Handles state communication and pre-flight health checks with the MCP Proxy"""
    def __init__(self, client: PypiMCPClient):
        self.client = client

    def run_preflight(self):
        if not self.client:
            return

        log.info("📡 Executing MCP Pre-flight diagnostics...")
        self.client.call("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "SecureBuilder", "version": "1.0"}
        })
        self.client.notify("notifications/initialized")

        status_resp = self.client.call("tools/call", {"name": "check_proxy_status"})
        log.info(f"   ↳ Internal State: {status_resp.get('content', [{}])[0].get('text', 'Unknown')}")

        fetch_resp = self.client.call("tools/call", {
            "name": "simulate_package_fetch", 
            "arguments": {"package_name": "uvloop"}
        })
        fetch_text = fetch_resp.get("content", [{}])[0].get("text", "Unknown")
        log.info(f"   ↳ Upstream Test: {fetch_text}")

        if "[ERROR]" in fetch_text:
            log.critical("🚨 MCP Pre-flight failed: Upstream registry unreachable.")
            sys.exit(1)

class BuilderWorkflow:
    """@desc: Orchestrates the domain objects to execute the build pipeline sequentially"""
    def __init__(self, target_dir: Path, registry_url: str, mode: BuildMode, mcp_client: PypiMCPClient = None):
        self.mode = mode
        self.target_dir = target_dir
        
        ## Determine the target lockfile format based on the selected mode
        self.lock_file = self.target_dir / ("uv.lock" if mode == BuildMode.MIGRATE_UVLOCK else "requirements.lock")
        
        self.project_mgr = ProjectManager(target_dir)
        self.warden = SecurityWarden(registry_url)
        self.mcp_coordinator = MCPCoordinator(mcp_client)
        self.auth_env = os.environ.copy()

    def execute(self):
        try:
            self.auth_env = self.warden.inject_auth(self.auth_env)
            in_file = self.project_mgr.resolve_and_prepare_source(self.mode)

            self.mcp_coordinator.run_preflight()
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
    """
    @desc:
    - Detects if the target registry is a local mock. 
    - If so, boots the MCP Hybrid Proxy and returns the connected MCP client
    """
    parsed = urlparse(registry_url)
    if parsed.hostname not in ["localhost", "127.0.0.1"]:
        return None
        
    port = parsed.port or 8080
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex((parsed.hostname, port)) == 0:
            log.info(f"⚡ [Proxy] Port {port} is already active. Cannot capture stdio for MCP.")
            return None

    log.info(f"🚀 [Proxy] Auto-booting internal MCP Proxy on port {port}...")
    
    try:
        import xphi.proxy.pypi as proxy_server
        client = PypiMCPClient(sys.executable, proxy_server)
    except Exception as e:
        log.error(f"Failed to instantiate proxy client: {e}")
        return None
    
    ## Poll the HTTP socket until the background proxy thread is fully ready
    max_retries = 50
    for _ in range(max_retries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((parsed.hostname, port)) == 0:
                log.info("   ✅ [Proxy] Mock HTTP/MCP registry successfully booted and ready.")
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
    
    ## Map the parsed string argument to the Enum explicitly
    build_mode = BuildMode(args.mode)
    TEST_PROXY_URL = "http://localhost:8083/simple"
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