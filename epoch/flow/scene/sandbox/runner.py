# epoch.flow.scene.sandbox.runner
## @lineage: epoch.flow.sandbox.runner
import time
from typing import Callable

from epoch.flow.scene.sandbox.script.test import ScriptDef, CONST
from kernel.phase.runner import SchemeRunner
from watcher.plane.emitter import get_emitter

log = get_emitter("sandbox.runner")

class SandboxRunner(SchemeRunner):
    async def _assert_script(self, script: ScriptDef, context: dict = None, validator: Callable[[str], bool] = None):
        """ScriptDef 기반으로 파이썬 샌드박스를 실행하고 정확성(Output/Error)을 검증합니다."""
        start_time = time.time()
        result = await self.broker.execute(code=script.code, tier=script.tier, context=context)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        if result.success != script.expect_success:
            self._record_fail(elapsed_ms, f"Expected Success={script.expect_success}, Got {result.success} (Output: {output_str})", script.title)
            return
            
        if script.expected_match and script.expected_match not in output_str:
            self._record_fail(elapsed_ms, f"Expected string '{script.expected_match}' not found in output. Output: {output_str}", script.title)
            return
            
        if validator and not validator(output_str):
            self._record_fail(elapsed_ms, f"Validation failed: {output_str}", script.title)
            return
            
        self._record_success(elapsed_ms, output_str)