# fiber.infra.e2e.sandbox
import os
import time
import json
import hashlib
import uuid
import random
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

import httpx
from pydantic import BaseModel, Field

from xphi.kernel.space.sandbox.runner import SchemeRunner
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("infra.e2e.sandbox")

# Sandbox Script Definitions & Isolation Tests
@dataclass(frozen=True)
class ScriptDef:
    title: str
    code: str
    expect_success: bool = True
    expected_match: Optional[str | tuple[str, ...]] = None
    tier: str = "SYSTEM"

class TestScripts:
    LEGACY_NORMAL = ScriptDef(
        title="Integrity: Light Compute (Simple Math)",
        code="print(sum([x**2 for x in range(1000)]))",
        expect_success=True,
        expected_match="332833500"
    )
    
    COMPUTE_HEAVY = ScriptDef(
        title="Workload: Heavy CPU Compute (Prime Factorization)",
        code="""
def is_prime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0: return False
    return True
primes = [p for p in range(30000) if is_prime(p)]
print(f'Found {len(primes)} primes')
        """.strip(),
        expect_success=True,
        expected_match="Found 3245 primes"
    )

    DATA_PROCESSING = ScriptDef(
        title="Workload: Memory & Data Processing (JSON Array)",
        code="""
import json
data = [{'id': i, 'val': i * 2.5, 'active': i % 2 == 0} for i in range(20000)]
serialized = json.dumps(data)
parsed = json.loads(serialized)
print(f'Processed {len(parsed)} records')
        """.strip(),
        expect_success=True,
        expected_match="Processed 20000 records"
    )

    TIME_LEAK = ScriptDef(
        title="Determinism: Sandbox Context Time Enforcement",
        code="import time\nprint(f'{time.time()}|{time.perf_counter()}')"
    )
    INJECTION = ScriptDef(
        title="Determinism: Context Injection",
        code="import time, random\nprint(f'{time.time()}|{random.random()}')"
    )
    PRNG_IDEMPOTENT = ScriptDef(
        title="Determinism: PRNG Idempotency",
        code="import random, os\nprint(f'{random.random()}|{os.urandom(4).hex()}')"
    )

    ENV_LEAK = ScriptDef(
        title="Isolation: Selective Gateway & Host Leak Prevention",
        code="""
import os
env = os.environ

## 1. Active Gateway Permeability: Verify that the explicitly whitelisted orchestration marker successfully penetrates the host-to-sandbox bridge
is_gateway_working = env.get('FIBER_ISOLATION_MARKER') == '1'

## 2. Host Environment Segregation: Guarantee strict absence of platform-specific or CI-injected variables to maintain absolute determinism across OS architectures.
blocked_keys = ['GITHUB_ACTIONS', 'COMPUTERNAME', 'XPC_SERVICE_NAME', 'COMMAND_MODE', 'TERM_PROGRAM']
is_host_blocked = all(k not in env for k in blocked_keys)

## 3. State Entropy Constraint: Strictly cap the total quantity of environment variables
is_minimal = len(env) <= 11

## [Assertion] A controlled closed system
isolated = is_gateway_working and is_host_blocked and is_minimal

print(f'Isolated: {isolated} | Dump: {dict(env)}')
        """.strip(),
        expect_success=True,
        expected_match="Isolated: True"
    )

    IO_VIOLATION = ScriptDef(
        title="Isolation: Deny Low-level Filesystem Scan",
        code="with open('/etc/passwd', 'r') as f:\n    print(f.read())",
        expect_success=False,
        expected_match="FileNotFoundError"
    )
    NET_VIOLATION = ScriptDef(
        title="Isolation: Deny Low-level Socket Binding",
        code="import socket\ns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\ns.connect(('8.8.8.8', 53))",
        expect_success=False,
        expected_match="Error" 
    )
    SYS_EXIT_ATTACK = ScriptDef(
        title="Isolation: Host Protection against sys.exit()",
        code="import sys\nsys.exit(1)",
        expect_success=False,
        expected_match="PythonError"
    )
    SUBPROCESS_ATTACK = ScriptDef(
        title="Isolation: Deny Process Spawning (Subprocess)",
        code="import subprocess\nsubprocess.run(['ls', '-la'])",
        expect_success=False,
        expected_match="Error" 
    )
    THREAD_ATTACK = ScriptDef(
        title="Isolation: Deny Multi-threading",
        code="import threading\ndef f(): pass\nt = threading.Thread(target=f)\nt.start()",
        expect_success=False
    )
    
    INFINITE_LOOP_ATTACK = ScriptDef(
        title="Resource: Opcode-based Fuel Exhaustion",
        code="x = 2\nwhile True: x = x ** 2",
        expect_success=False,
        expected_match="timeout", 
        tier="STANDARD"
    )
    HEAP_ALLOCATION_ATTACK = ScriptDef(
        title="Resource: Heap Allocation Guard (OOM / SLA Timeout)",
        code="""
lst = []
while True:
    lst.append(bytearray(10 * 1024 * 1024))
        """.strip(),
        expect_success=False,
        expected_match=("MemoryError", "Sandbox Hard Terminated", "timeout"),
        tier="STANDARD"
    )
    STACK_OVERFLOW_ATTACK = ScriptDef(
        title="Resource: Deep Recursion Guard (Stack Overflow)",
        code="def recurse(n):\n    return recurse(n+1)\nrecurse(1)",
        expect_success=False,
        expected_match="RecursionError"
    )

class SandboxRunner(SchemeRunner):
    async def _assert_script(self, script: ScriptDef, context: dict = None, validator: Callable[[str], bool] = None):
        start_time = time.time()
        _ctx = context or {}
        _ctx["sandbox_tier"] = script.tier
        
        result = await self.broker.execute(code=script.code, tier=script.tier, context=_ctx)
        elapsed_ms = (time.time() - start_time) * 1000
        
        output_str = str(result.output) if result.success else str(result.error)
        
        if result.success != script.expect_success:
            self._record_fail(elapsed_ms, f"Expected Success={script.expect_success}, Got {result.success} (Output: {output_str})", "Execution Output", title=script.title)
            return

        if script.expected_match is not None:
            if isinstance(script.expected_match, tuple):
                if not any(match_str in output_str for match_str in script.expected_match):
                    self._record_fail(elapsed_ms, f"Expected one of {script.expected_match} not found in output. Output: {output_str}", "String Match", title=script.title)
                    return
            else:
                if script.expected_match not in output_str:
                    self._record_fail(elapsed_ms, f"Expected string '{script.expected_match}' not found in output. Output: {output_str}", "String Match", title=script.title)
                    return
            
        if validator:
            try:
                if not validator(output_str):
                    self._record_fail(elapsed_ms, f"Validation failed: {output_str}", "Custom Validator", title=script.title)
                    return
            except Exception as e:
                self._record_fail(elapsed_ms, f"Validation crashed: {e} (Output: {output_str})", "Validator Exception", title=script.title)
                return
            
        self._record_success(elapsed_ms, output_str)