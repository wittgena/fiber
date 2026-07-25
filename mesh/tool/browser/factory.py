# mesh.tool.browser.factory
## @lineage: gov.tool.browser.factory
## @lineage: eco.gov.tool.browser.factory
## @lineage: atoa.gov.tool.browser.factory
## @lineage: agent.gov.tool.browser.factory
## @lineage: gov.sandbox.engine.tool.browser.factory
## @lineage: agent.engine.tool.browser.factory
import sys
import threading
from typing import Any

from gov.action.executor import ActionExecutor
from gov.action.tool.schema.browser import BrowserAction, BrowserObservation

if sys.platform == "win32":
    from mesh.tool.browser.window import WindowsBrowserExecutor
else:
    from mesh.tool.browser.executor import BrowserExecutor

from watcher.plane.emitter import get_logger

log = get_logger(__name__)

class BrowserExecutorFactory:
    _shared_executor: ActionExecutor[BrowserAction, BrowserObservation] | None = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def get_shared_executor(
        cls, env_observation_dir: str, **executor_config: Any
    ) -> ActionExecutor[BrowserAction, BrowserObservation]:
        
        with cls._lock:
            if cls._shared_executor is not None:
                if executor_config:
                    log.warning(
                        "Browser executor already exists. Config %s will be ignored. "
                        "Subagents will reuse the parent's browser session.",
                        list(executor_config.keys())
                    )
                return cls._shared_executor

            if sys.platform == "win32":
                executor = WindowsBrowserExecutor(
                    full_output_save_dir=env_observation_dir,
                    **executor_config,
                )
            else:
                executor = BrowserExecutor(
                    full_output_save_dir=env_observation_dir,
                    **executor_config,
                )
                
            cls._shared_executor = executor
            return executor