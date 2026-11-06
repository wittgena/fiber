# fiber.phase.ops.flare.deploy
import os
import sys
import shutil
import asyncio
import argparse
from typing import Optional

from xphi.arch.contract.config.env import config as env_config
from xphi.node.rpc.schema.receipt import EdgeHeader
from xphi.node.runtime.wasm.broker import WasmBroker
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.plane.flare.tunnel import FlareTunnelFactory
from xphi.watcher.plane.flare.controller import FlareController

log = get_emitter("ops.flare.deploy")

GATEWAY_WORKER_NAME = "xphi-gateway-node"
PYTHON_WORKER_NAME = "xphi-python-node"


async def _exec_wrangler(*args: str, cwd: Optional[str] = None) -> tuple[int, str]:
    """Executes an `npx wrangler` command and returns (returncode, combined_output)."""
    proc = await asyncio.create_subprocess_exec(
        "npx", "wrangler", *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout_bytes, _ = await proc.communicate()
    output = stdout_bytes.decode("utf-8", errors="replace").strip()
    return proc.returncode or 0, output


async def check_wrangler_auth() -> bool:
    """Verifies that `npx wrangler whoami` succeeds or CLOUDFLARE_API_TOKEN is set."""
    if os.getenv("CLOUDFLARE_API_TOKEN"):
        log.info("[AUTH] CLOUDFLARE_API_TOKEN detected in environment.")
        return True

    log.info("[AUTH] Checking local Wrangler authentication (`npx wrangler whoami`)...")
    code, output = await _exec_wrangler("whoami")

    if code != 0 or "not authenticated" in output.lower():
        log.error(
            "[AUTH FAIL] Wrangler is not logged in.\n"
            "Please run `npx wrangler login` or export `CLOUDFLARE_API_TOKEN` first.\n"
            f"Details:\n{output}"
        )
        return False

    log.info("[AUTH OK] Wrangler session verified.")
    return True


async def run_smoke_probes(target_url: str, timeout: float = 20.0) -> bool:
    """Executes 3-Stage live smoke probes (PHASE ABI, PYTHON Service Binding, Canonical Seal)."""
    env_config.set_override("XPHI_WASM_PLANE", f"flare:{target_url}")
    broker = WasmBroker(tunnel_factory=FlareTunnelFactory, timeout=timeout)

    try:
        # Stage 1: PHASE Wasm (`phase.wasm` -> `verify_parity`)
        log.info("[TEST 1/3] Dispatching PHASE Wasm (`verify_parity`) to Edge...")
        res_phase = await broker.invoke(
            target_func="verify_parity",
            payload={"topos_id_low32": 101010, "phase_id": 999999, "nexus_id": 907049},
            wasm_path="phase.wasm",
        )
        if not res_phase.success or "is_valid" not in str(res_phase.output):
            log.error(f"[FAIL] PHASE Wasm failed: {getattr(res_phase, 'error', res_phase.output)}")
            return False
        log.info(f"[PASS] PHASE Wasm Response: {res_phase.output.strip()}")

        # Stage 2: PYTHON Service Binding (`xphi-gateway-node` -> `xphi-python-node`)
        log.info("[TEST 2/3] Dispatching Sandboxed Python (`Pyodide`) via Service Binding...")
        res_py = await broker.execute(
            code="import sys; print(f'EDGE_PY_OK:{edge_val * 3}')",
            variables={"edge_val": 14},
        )
        edge_hash = getattr(res_py, "edge_hash", None)
        if not res_py.success or "EDGE_PY_OK:42" not in str(res_py.output):
            log.error(f"[FAIL] PYTHON Engine failed: {getattr(res_py, 'error', res_py.output)}")
            return False
        log.info(f"[PASS] PYTHON Engine Output: {res_py.output.strip()}")

        # Stage 3: Verify Canonical Hash Header Seal
        seal_header = EdgeHeader.CANONICAL_HASH.value
        log.info(f"[TEST 3/3] Verifying Edge Boundary Cryptographic Seal (`{seal_header}`)...")
        if not edge_hash or edge_hash == "HASH_GENERATION_FAILED":
            log.error(f"[FAIL] Missing or invalid {seal_header}: {edge_hash}")
            return False
        log.info(f"[PASS] Captured {seal_header}: {edge_hash}")
        return True
    finally:
        await broker.close()


async def action_deploy_or_dev(
    mode: str,
    target_url: Optional[str] = None,
    skip_verify: bool = False,
    keep_workspace: bool = True,
    timeout: int = 180,
) -> int:
    """Provisions Dual Workers (local dev or remote Cloudflare deploy) and runs smoke verification."""
    log.info("=" * 70)
    log.info(f"[START] FLARE EDGE PROVISIONING & VERIFICATION (action={mode.upper()})")
    log.info("=" * 70)

    if target_url:
        normalized = target_url if target_url.startswith("flare:") else f"flare:{target_url}"
        os.environ["XPHI_WASM_PLANE"] = normalized
        env_config.set_override("XPHI_WASM_PLANE", normalized)

    if mode == "deploy":
        explicit_url = os.getenv("XPHI_WASM_PLANE", "")
        if not explicit_url.startswith("flare:https://"):
            if not await check_wrangler_auth():
                return 1

    controller = FlareController(mode=mode, timeout=timeout, keep_workspace=keep_workspace)
    try:
        deployed_url = await controller.start_runtime()
        log.info(f"[BOUND] Active Edge Endpoint: {deployed_url}")

        if not skip_verify:
            passed = await run_smoke_probes(deployed_url)
            if not passed:
                return 1

        log.info("=" * 70)
        log.info("[SUCCESS] Cloudflare Edge Operation Complete!")
        log.info(f" -> Reusable Plane Config : export XPHI_WASM_PLANE=flare:{deployed_url}")
        if keep_workspace:
            log.info(f" -> Synthesized Blueprints: {controller.workspace}")
        log.info("=" * 70)
        return 0

    except Exception as e:
        log.error(f"[ERROR] Edge provisioning aborted: {e}", exc_info=True)
        return 1
    finally:
        await controller.stop_runtime()


async def action_verify(target_url: Optional[str] = None, timeout: float = 20.0) -> int:
    """Runs zero-deploy 3-stage smoke verification against an existing Edge endpoint."""
    raw_plane = target_url or os.getenv("XPHI_WASM_PLANE", "")
    resolved_url = raw_plane[len("flare:"):] if raw_plane.startswith("flare:") else raw_plane

    if not resolved_url.startswith("http"):
        log.error(
            "[VERIFY FAIL] No active Edge URL provided.\n"
            "Pass `--url https://xphi-gateway-node.<subdomain>.workers.dev` "
            "or set `export XPHI_WASM_PLANE=flare:https://...`."
        )
        return 1

    log.info("=" * 70)
    log.info(f"[START] FLARE ZERO-DEPLOY SMOKE VERIFICATION -> {resolved_url}")
    log.info("=" * 70)

    passed = await run_smoke_probes(resolved_url, timeout=timeout)
    if not passed:
        return 1

    log.info("=" * 70)
    log.info(f"[SUCCESS] Live Edge Endpoint Verified: {resolved_url}")
    log.info("=" * 70)
    return 0


async def action_teardown(clean_workspace: bool = True) -> int:
    """Deletes remote Cloudflare Workers in reverse dependency order (Gateway -> Python Engine)."""
    log.info("=" * 70)
    log.info("[START] FLARE EDGE TEARDOWN (Reverse Dependency Order)")
    log.info("=" * 70)

    if not await check_wrangler_auth():
        return 1

    # Upstream Gateway MUST be deleted before Downstream Python Engine to release Service Binding
    for worker_name in (GATEWAY_WORKER_NAME, PYTHON_WORKER_NAME):
        log.info(f"[DELETE] Removing remote Worker `{worker_name}` from Cloudflare...")
        code, output = await _exec_wrangler("delete", "--name", worker_name, "--force")
        if code == 0:
            log.info(f"[DELETED] `{worker_name}` removed successfully.")
        elif "does not exist" in output.lower() or "could not find" in output.lower():
            log.info(f"[SKIP] `{worker_name}` is already absent on Cloudflare.")
        else:
            log.warning(f"[WARN] Non-zero response while deleting `{worker_name}`:\n{output}")

    workspace_path = "/tmp/xphi_edge_workspace"
    if clean_workspace and os.path.exists(workspace_path):
        shutil.rmtree(workspace_path, ignore_errors=True)
        log.info(f"[CLEAN] Removed local synthesized workspace: {workspace_path}")

    log.info("=" * 70)
    log.info("[SUCCESS] Cloudflare Edge Workers & Workspace Teardown Complete!")
    log.info("=" * 70)
    return 0


async def action_status() -> int:
    """Checks Wrangler auth session and lists deployment status for Dual Workers."""
    log.info("=" * 70)
    log.info("[STATUS] FLARE EDGE CONTROL PLANE DIAGNOSTICS")
    log.info("=" * 70)

    if not await check_wrangler_auth():
        return 1

    active_plane = os.getenv("XPHI_WASM_PLANE", "local (default)")
    log.info(f"[ENV] XPHI_WASM_PLANE = {active_plane}")

    for worker_name in (GATEWAY_WORKER_NAME, PYTHON_WORKER_NAME):
        code, output = await _exec_wrangler("deployments", "list", "--name", worker_name)
        if code == 0 and "version id" in output.lower():
            last_lines = "\n".join(output.splitlines()[-6:])
            log.info(f"[LIVE] `{worker_name}` is deployed on Cloudflare:\n{last_lines}")
        else:
            log.info(f"[ABSENT] `{worker_name}` is not currently deployed.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m fiber.phase.ops.flare.deploy",
        description="Fiber Cloudflare Edge V8 Isolate Provisioning, Verification & Teardown CLI",
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="deploy",
        choices=["deploy", "up", "dev", "verify", "teardown", "down", "delete", "status"],
        help="Lifecycle action to execute (default: deploy)",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="Target BYOC workers.dev URL for zero-deploy binding or verification",
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Provision and deploy workers without running the 3-stage smoke probes",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove `/tmp/xphi_edge_workspace` after deploy/dev finishes",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="Provisioning timeout in seconds (default: 180)",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    action = args.action.lower()

    if action in ("deploy", "up"):
        return await action_deploy_or_dev(
            mode="deploy",
            target_url=args.url,
            skip_verify=args.skip_verify,
            keep_workspace=not args.clean,
            timeout=args.timeout,
        )
    if action == "dev":
        return await action_deploy_or_dev(
            mode="dev",
            skip_verify=args.skip_verify,
            keep_workspace=not args.clean,
            timeout=args.timeout,
        )
    if action == "verify":
        return await action_verify(target_url=args.url)
    if action in ("teardown", "down", "delete"):
        return await action_teardown(clean_workspace=True)
    if action == "status":
        return await action_status()

    return 1

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(async_main(args))
    sys.exit(exit_code)

if __name__ == "__main__":
    main()