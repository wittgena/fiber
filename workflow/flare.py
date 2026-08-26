# workflow.flare
## @lineage: dphi.workflow.flare
import sys
import argparse
import importlib
import logging
from dataclasses import dataclass, field
from typing import List, Dict

from xphi.watcher.flare.tester import FlareTester
from xphi.kernel.space.topos.tunnel.flare import FlareTunnelFactory
from xphi.kernel.dphi.broker import DphiBroker
from xphi.kernel.phase.reactor import PhaseReactor
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("workflow.flare")

@dataclass
class FlarePipelineConfig:
    """
    @desc: Registry of test scenes to validate in the edge environment.
           Reuses local Wasm environment scenes without modification, 
           while integrating edge-specific physical boundary tests (flare scene).
    """
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "sandbox": "fiber.dphi.workflow.scene.sandbox:SandboxScene",
        "cert": "fiber.dphi.workflow.scene.cert:CertProofScene",
        "flare": "fiber.dphi.workflow.scene.flare:FlareEdgeScene",   # [ADD] Edge-specific physical validation
        # "anchor": "fiber.dphi.workflow.scene.anchor:AnchorScene" # Uncomment if needed
    })
    
    default_suites: List[str] = field(default_factory=lambda: [
        "sandbox",     # 1. Runtime security & single sandbox isolation (General Wasm semantics)
        "cert",        # 2. Floating-point determinism & Byzantine fault tolerance (Consensus integrity)
        "flare",       # 3. [CRITICAL] Edge physical boundary validation (Must be last due to Kinetic Trap rupture)
    ])


class FlareFlow:
    """
    @role: Cloudflare Edge Pipeline Orchestrator
    @desc: Bypasses WASM build process, dynamically provisioning V8 holograms 
           via FlareTester to execute end-to-end integration tests at the Global Edge.
    """
    def __init__(self, mode: str = "dev", command: str = "test", suites: List[str] = None, config: FlarePipelineConfig = None, keep_workspace: bool = False):
        self.config = config or FlarePipelineConfig()
        self.command = command
        self.mode = mode
        self.keep_workspace = keep_workspace  # Control flag for workspace preservation
        
        # Execute default suites sequentially if 'all' or empty is provided
        if not suites or suites == ["all"]:
            self.suites = self.config.default_suites
        else:
            self.suites = suites
        
        self.log = get_emitter("flare.entry")

    def _resolve_suite_class(self, suite_name_or_path: str):
        module_path_str = self.config.suites_registry.get(suite_name_or_path, suite_name_or_path)
        try:
            if ":" not in module_path_str:
                raise ValueError(f"Invalid suite path format '{module_path_str}'. Expected 'module.path:ClassName'")

            mod_name, cls_name = module_path_str.split(":")
            module = importlib.import_module(mod_name)
            return getattr(module, cls_name)
        except Exception as e:
            self.log.error(f"[CLI] Failed to dynamically load suite '{suite_name_or_path}': {e}")
            sys.exit(1)

    async def test(self):
        self.log.info(f"\n[CLI] Starting Cloudflare Edge Orchestrator in [{self.mode.upper()}] mode for suites: {self.suites}...")
            
        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue
            suite_map[suite_name] = self._resolve_suite_class(suite_name)
            
        if not suite_map:
            self.log.error("❌ [CLI] No valid test suites found to execute.")
            sys.exit(1)

        worker_name = "dphi-edge-sandbox"
        
        # 1. Dynamic mapping of edge endpoint based on physical environment mode
        if self.mode == "dev":
            edge_url = "http://127.0.0.1:8787"
        else:
            # Routing address for the actual deployed worker (.workers.dev domain)
            edge_url = f"https://{worker_name}.workers.dev"

        self.log.info(f"[SYSTEM] Injecting FlareTunnelFactory targeting: {edge_url}")
        
        # 2. Inject Flare-exclusive tunnel & broker (Duck Typing & Dependency Injection)
        await FlareTunnelFactory.get_default(mq_url=edge_url)
        
        broker = DphiBroker(
            tunnel_factory=FlareTunnelFactory,  # DI Pattern applied
            request_stream="wasm:execute:stream:tester_isolated",
            timeout=5.0
        )
        broker.control_channel = "wasm:control:req:tester_isolated"

        # 3. Ignite FlareTester (Orchestrating observation & test networks)
        tester = FlareTester(
            target_name=worker_name,
            mode=self.mode,
            timeout=120,
            suites=suite_map
        )
        
        tester.keep_workspace = self.keep_workspace
        
        # Execute suites
        success, err_msg = await tester.execute(broker=broker)
        
        log.info("\n" + "="*75)
        log.info("🚀 CLOUDFLARE EDGE PIPELINE EXECUTION REPORT 🚀".center(75))
        log.info("="*75)
        
        if success:
            self.log.info(f"🟢 [SUCCESS] All Edge Test Suites ({', '.join(suite_map.keys())}) PASSED.")
            if getattr(tester, 'test_execution_hash', None):
                self.log.info(f"🔗 Execution Canonical Hash (Sealed at Edge): {tester.test_execution_hash}")
            log.info("="*75 + "\n")
        else:
            self.log.critical(f"🔴 [FAILED] Edge Test execution terminated with errors.")
            # Multi-line error message support for automated self-diagnosis
            for line in err_msg.split('\n'):
                self.log.critical(f"📝 {line}")
            
            if hasattr(tester, 'suite_runners') and tester.suite_runners:
                log.info("\n" + "🔥"*37)
                log.info("🚨 DETAILED EDGE FAILURE TRACES 🚨".center(75))
                log.info("🔥"*37)
                
                for suite_name, runner in tester.suite_runners.items():
                    fail_cnt = getattr(runner, 'fail_count', 0)
                    failed_cases = getattr(runner, 'failed_cases', [])
                    
                    if fail_cnt > 0 or failed_cases:
                        log.info(f"\n❌ [SUITE: {suite_name.upper()}] ➔ {fail_cnt} Test(s) Failed on V8 Engine")
                        for idx, fc in enumerate(failed_cases, 1):
                            title = fc.get('title', 'Unknown Test Case')
                            err = fc.get('error', 'No error details provided')
                            log.info(f"  └─ {idx}. {title}")
                            log.info(f"     [Reason] {err}\n")
                
                log.info("="*75 + "\n")
            
            sys.exit(1)

    async def run(self):
        await self.test()


def main():
    parser = argparse.ArgumentParser(description="DPHI Cloudflare Edge Orchestrator")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev", help="Run locally (dev) or on Global Edge (deploy)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run")
    
    # Debug and Diagnostics Flags
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level to capture underlying Edge/Wrangler streams.")
    parser.add_argument("--keep-workspace", action="store_true", help="Prevent teardown of the workspace on failure for post-mortem analysis.")
    
    args = parser.parse_args()
    config = FlarePipelineConfig()
    
    # [FIX] Robustly enforce DEBUG logging down to the specific auditor instances
    if args.debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        
        # Explicitly target the auditor's namespace to ensure stream logs aren't swallowed
        logging.getLogger("auditor.flare.dev").setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal stream logging is ENABLED.")
    
    app = FlareFlow(
        mode=args.mode, 
        command="test", 
        suites=args.suites, 
        config=config,
        keep_workspace=args.keep_workspace
    )
    PhaseReactor.ignite(app.run)

if __name__ == "__main__":
    main()