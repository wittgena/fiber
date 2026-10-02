# fiber.dev.ex.space.challenge.plane
import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Any, Dict

from fiber.infra.plane.compose import ComposeOrchestrator, ComposeContext
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

FIBER_ROOT = resolve_path("fiber")
log = get_emitter("challenge.plane")

class ProvisionScene:
    """
    Provisions the CTF challenge runtime and maintains it in the background.
    Bypasses test analytics to focus solely on booting a single target environment.
    """
    def __init__(self, broker: Any = None, context: ComposeContext = None):
        self.broker = broker
        self.context = context
        self.adapter = context.adapter if context else None
        self.log = get_emitter("provision.scene")

    async def run_all(self):
        self.log.info("\n=== [START] Provisioning Fiber CTF Challenge Runtime ===")
        
        if not self.adapter:
            self.log.error("  └─ Compose Runtime Availability: Failed 🔴")
            return

        # Stripped test logic (fiber e2e) and added an infinite sleep to maintain the container state.
        challenge_bash_script = """
set -e
trap 'EXIT_CODE=\(?; if [\)EXIT_CODE -ne 0 ]; then echo -e "\\n🔥 [FATAL] KERNEL CRASHED! 🔥\\n"; exit $EXIT_CODE; fi' EXIT

echo "[CTF-SYNC] 1. Bootstrap Topology Boundary..."
python -m xphi.kernel.space.bind.around

echo "[CTF-SYNC] 1.5. Injecting Mock Node Identity..."
mkdir -p ~/.ssh
cat << 'EOF' > ~/.ssh/id_ed25519
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACC3e9qbo208kLZIK53Q9rso+2oqHzQGP5bqpKkQer1exgAAAKirLcH+qy3B
/gAAAAtzc2gtZWQyNTUxOQAAACC3e9qbo208kLZIK53Q9rso+2oqHzQGP5bqpKkQer1exg
AAAEBvdGKojS3foXcxfI4iAcxsuYLXS7w/X7K7VKaJt7yNd7d72pujbTyQtkgrndD2uyj7
aiofNAY/luqkqRB6vV7GAAAAJHdpdHRnZW5hQHdpdHRnZW5hcy1NYWNCb29rLVByby5sb2
NhbAE=
-----END OPENSSH PRIVATE KEY-----
EOF
chmod 600 ~/.ssh/id_ed25519

echo "[CTF-SYNC] 2. Booting Kernel (Background)..."
# Standard output routing (no file dump) ensures logs remain accessible via 'docker logs' for attackers.
python -u -m xphi.kernel.ops.boot & 

echo "[CTF-SYNC] 3. Waiting for Kernel Healthcheck..."
timeout 30 bash -c 'while ! curl -s http://127.0.0.1:8000/v1/exchange/keys > /dev/null; do sleep 1; done' || { echo -e "\\n🔥 KERNEL BOOT FAILED! 🔥\\n"; exit 1; }

echo -e "\\n========================================================"
echo "🎯 [FIBER CTF] Target Node is LIVE and ready for attacks!"
echo "📡 Kernel API Gateway: http://127.0.0.1:8000"
echo "🗂️ Proof of Concept (Flag) Target: /artifact_mount/FLAG.txt"
echo "========================================================\\n"

# Infinite sleep to persist the runtime for challenges.
exec sleep infinity
"""
        try:
            # Applying detach=True to execute the job asynchronously in the background.
            success = await self.adapter.apply_job(
                job_name="runtime-boot", 
                env={
                    "XPHI_ENV": "ci",
                    "REDIS_URL": "redis://redis:6379/0",
                    "FIBER_E2E_STEPS": challenge_bash_script.strip()
                },
                detach=True 
            )
            if not success:
                raise RuntimeError("runtime provision failed.")
                
            self.log.info("  └─ CTF Runtime is LIVE in the background 🟢")
        except Exception as e:
            self.log.error(f"  [SCENARIO HALTED] Provisioning Failed: {e}")

class ComposeFlow:
    """CLI Control Plane for orchestrating CTF Environments."""
    def __init__(self, action: str = "start", mode: str = "dev", keep_workspace: bool = True, rebuild: bool = False):
        self.action = action
        self.mode = mode
        self.keep_workspace = keep_workspace
        self.rebuild = rebuild

    async def execute(self):
        self.log = log
        self.log.info(f"\n[CTF] Initializing Orchestrator in [{self.mode.upper()}] mode")
        
        original_cwd = Path.cwd()
        if FIBER_ROOT:
            os.chdir(FIBER_ROOT)

        try:
            controller = ComposeOrchestrator(
                mode=self.mode,
                suites={"provision": ProvisionScene},
                rebuild=self.rebuild,
                auto_teardown=False 
            )
            controller.keep_workspace = self.keep_workspace
            
            if self.action == "start":
                success, err_msg = await controller.execute(broker=None)
                if success:
                    self.log.info("\n" + "="*75)
                    self.log.info(f"🎯 FIBER CTF READY 🎯".center(75))
                    self.log.info("="*75)
                    self.log.info(f"🟢 Container is running in the background.")
                    self.log.info(f"📂 Mounted Evidence Artifact Directory: {controller.artifact_dir}")
                    self.log.info(f"🛑 Termination Command: python -m fiber.dev.ex.challenge.plane stop")
                    self.log.info("="*75 + "\n")
                else:
                    self.log.critical(f"🔴 CTF Environment Provisioning Failed: {err_msg}")
                    sys.exit(1)
                    
            elif self.action == "stop":
                self.log.info("\n[CTF] Executing forced teardown of the challenge environment per user request.")
                await controller.adapter.teardown()
                self.log.info(f"  └─ Environment terminated. Verify artifacts in: {controller.artifact_dir}")
                
        finally:
            os.chdir(original_cwd)

def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber CTF Orchestrator via DOCKER COMPOSE")
    
    parser.add_argument("action", choices=["start", "stop"], default="start", nargs="?", help="Start or stop the challenge environment.")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev")
    parser.add_argument("--no-keep", action="store_false", dest="keep_workspace", help="Destroy host mounts on termination (Not Recommended)")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    
    args, _ = parser.parse_known_args(args_list)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal execution logging is ENABLED.")

    app = ComposeFlow(action=args.action, mode=args.mode, keep_workspace=args.keep_workspace, rebuild=args.rebuild)
    PhaseReactor.ignite(main_coro_func=app.execute)

if __name__ == "__main__":
    main()