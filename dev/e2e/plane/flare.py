# fiber.dev.e2e.plane.flare
import sys
import os
import argparse
import importlib
import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict

import fiber.dev.e2e.dphi.scene as scene_module

from xphi.kernel.wasm.broker import DphiBroker
from xphi.state.phase.reactor import PhaseReactor
from xphi.watcher.plane.flare.tunnel import FlareTunnelFactory
from xphi.watcher.plane.flare.controller import FlareController

# [수정됨] flow_scope를 추가로 import 합니다.
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("e2e.plane.flare")
MODULE_PATH = scene_module.__name__

@dataclass
class FlarePipelineConfig:
    suites_registry: Dict[str, str] = field(default_factory=lambda: {
        "flare": f"{MODULE_PATH}.flare:FlareScene",
    })
    
    default_suites: List[str] = field(default_factory=lambda: [
        "flare",
    ])

class FlareFlow:
    def __init__(self, mode: str = "dev", command: str = "test", suites: List[str] = None, config: FlarePipelineConfig = None, keep_workspace: bool = False):
        self.config = config or FlarePipelineConfig()
        self.command = command
        self.mode = mode
        self.keep_workspace = keep_workspace
        
        if not suites or suites == ["all"]:
            self.suites = self.config.default_suites
        else:
            self.suites = suites
        
        self.log = get_emitter("flare.entry")
        self.log.debug(f"[FlareFlow:__init__] Initialized with mode={self.mode}, command={self.command}, suites={self.suites}, keep_workspace={self.keep_workspace}") # [LOG ADDED]

    def _resolve_suite_class(self, suite_name_or_path: str):
        module_path_str = self.config.suites_registry.get(suite_name_or_path, suite_name_or_path)
        self.log.debug(f"[FlareFlow:_resolve_suite_class] Resolving suite '{suite_name_or_path}' -> '{module_path_str}'") # [LOG ADDED]
        try:
            if ":" not in module_path_str:
                raise ValueError(f"Invalid suite format '{module_path_str}'. Expected 'module.path:ClassName'")

            mod_name, cls_name = module_path_str.split(":")
            module = importlib.import_module(mod_name)
            suite_cls = getattr(module, cls_name)
            self.log.debug(f"[FlareFlow:_resolve_suite_class] Successfully loaded class {cls_name} from {mod_name}") # [LOG ADDED]
            return suite_cls
        except Exception as e:
            self.log.error(f"[CLI] Failed to load suite '{suite_name_or_path}': {e}")
            sys.exit(1)

    async def test(self):
        self.log.info(f"\n[PHASE 1] Initializing Cloudflare Edge Orchestrator in [{self.mode.upper()}] mode")
        
        self.log.debug("[FlareFlow:test] Validating and mapping requested suites...") # [LOG ADDED]
        suite_map = {}
        for suite_name in self.suites:
            if suite_name not in self.config.suites_registry:
                self.log.warning(f"[CLI] Unknown suite '{suite_name}', skipping...")
                continue
            suite_map[suite_name] = self._resolve_suite_class(suite_name)
            
        if not suite_map:
            self.log.error("❌ [CLI] No valid test suites found to execute.")
            sys.exit(1)
            
        self.log.debug(f"[FlareFlow:test] Suite map constructed: {list(suite_map.keys())}") # [LOG ADDED]

        worker_name = "dphi-edge-sandbox"
        edge_url = "http://127.0.0.1:8787" if self.mode == "dev" else f"https://{worker_name}.workers.dev"

        self.log.info(f"[PHASE 2] Connecting to Edge Endpoint: {edge_url}")
        
        self.log.debug("[FlareFlow:test] Awaiting FlareTunnelFactory.get_default()...") # [LOG ADDED]
        t0 = time.time() # [LOG ADDED]
        await FlareTunnelFactory.get_default(mq_url=edge_url)
        self.log.debug(f"[FlareFlow:test] FlareTunnelFactory initialized in {time.time() - t0:.3f}s") # [LOG ADDED]
        
        # =========================================================================
        # [핵심 개선] 60초 강제 확장을 제거하여 다이내믹 타임아웃 존중 (Fast-Fail 복원)
        # =========================================================================
        self.log.debug("[FlareFlow:test] Instantiating DphiBroker with timeout=15.0s (Dynamic Timeout Enable)") # [LOG ADDED]
        broker = DphiBroker(
            tunnel_factory=FlareTunnelFactory,
            request_stream="wasm:execute:stream:tester_isolated",
            timeout=15.0  # [FIX] 60.0에서 15.0(기본값)으로 롤백. 이제 5초짜리 테스트는 8초 시점에 깔끔하게 끊어집니다.
        )
        broker.control_channel = "wasm:control:req:tester_isolated"
        self.log.debug(f"[FlareFlow:test] Broker instantiated. Request stream: {broker.request_stream}, Control: {broker.control_channel}") # [LOG ADDED]

        self.log.info("[PHASE 3] Handing over execution to FlareController...")
        
        self.log.debug("[FlareFlow:test] Instantiating FlareController...") # [LOG ADDED]
        controller = FlareController(
            target_name=worker_name,
            mode=self.mode,
            timeout=120,
            suites=suite_map
        )
        controller.keep_workspace = self.keep_workspace
        
        self.log.debug("[FlareFlow:test] Yielding execution to controller.execute() -> Will block until orchestration/tests complete.") # [LOG ADDED]
        t1 = time.time() # [LOG ADDED]
        success, err_msg = await controller.execute(broker=broker)
        self.log.debug(f"[FlareFlow:test] controller.execute() returned in {time.time() - t1:.3f}s. Success={success}") # [LOG ADDED]
        
        log.info("\n" + "="*75)
        log.info("🚀 CLOUDFLARE EDGE PIPELINE EXECUTION REPORT 🚀".center(75))
        log.info("="*75)
        
        if success:
            self.log.info(f"🟢 [SUCCESS] All Edge Test Suites ({', '.join(suite_map.keys())}) PASSED.")
            if getattr(controller, 'test_execution_hash', None):
                self.log.info(f"🔗 Execution Canonical Hash (Sealed at Edge): {controller.test_execution_hash}")
            log.info("="*75 + "\n")
        else:
            self.log.critical(f"🔴 [FAILED] Edge Test execution terminated with errors.")
            for line in err_msg.split('\n'):
                self.log.critical(f"📝 {line}")
            
            if hasattr(controller, 'suite_runners') and controller.suite_runners:
                log.info("\n" + "🔥"*37)
                log.info("🚨 DETAILED EDGE FAILURE TRACES 🚨".center(75))
                log.info("🔥"*37)
                
                for suite_name, runner in controller.suite_runners.items():
                    fail_cnt = getattr(runner, 'fail_count', 0)
                    failed_cases = getattr(runner, 'failed_cases', [])
                    
                    if fail_cnt > 0 or failed_cases:
                        log.info(f"\n❌ [SUITE: {suite_name.upper()}] ➔ {fail_cnt} Test(s) Failed")
                        for idx, fc in enumerate(failed_cases, 1):
                            title = fc.get('title', 'Unknown Test Case')
                            err = fc.get('error', 'No error details provided')
                            log.info(f"  └─ {idx}. {title}")
                            log.info(f"     [Reason] {err}\n")
                log.info("="*75 + "\n")
            sys.exit(1)

    async def run(self):
        self.log.debug("[FlareFlow:run] Entrypoint triggered, awaiting test()...") # [LOG ADDED]
        # [수정됨] 하위 전체 트리에 적용되도록 flow_scope(mode="RAW") 블록으로 감쌉니다.
        # 이로 인해 하위 로거들(터널, 스페이스 러너 등)이 Burst Control을 우회하게 됩니다.
        with flow_scope(mode="RAW"):
            await self.test()

def main(args_list: list[str] = None):
    parser = argparse.ArgumentParser(description="DPHI Cloudflare Edge Orchestrator")
    parser.add_argument("--mode", choices=["dev", "deploy"], default="dev", help="Run locally (dev) or on Global Edge (deploy)")
    parser.add_argument("--suites", nargs="+", default=["all"], help="List of suites to run")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG log level to capture underlying Edge/Wrangler streams.")
    parser.add_argument("--keep-workspace", action="store_true", help="Prevent teardown of the workspace on failure for post-mortem analysis.")
    
    args, _ = parser.parse_known_args(args_list)
    
    # [LOG ADDED] 기본 로거 포맷이 세팅되기 전일 수 있으나 print 형태로 남기거나 세팅 직후 남김
    if args.debug:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        
        logging.getLogger("auditor.flare.dev").setLevel(logging.DEBUG)
        log.info("🐛 [DEBUG MODE] Internal stream logging is ENABLED.")
        log.debug(f"[main] Parsed CLI arguments: mode={args.mode}, suites={args.suites}, keep_workspace={args.keep_workspace}") # [LOG ADDED]

    config = FlarePipelineConfig()
    log.debug("[main] Instantiating FlareFlow pipeline app...") # [LOG ADDED]
    app = FlareFlow(
        mode=args.mode, 
        command="test", 
        suites=args.suites, 
        config=config,
        keep_workspace=args.keep_workspace
    )
    
    log.debug("[main] Igniting PhaseReactor with app.run coroutine...")
    PhaseReactor.ignite(main_coro_func=app.run)

if __name__ == "__main__":
    main()