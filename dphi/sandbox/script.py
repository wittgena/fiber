# dphi.sandbox.script
import time
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List
from watcher.plane.emitter import get_emitter

@dataclass(frozen=True)
class ScriptDef:
    title: str
    code: str
    expect_success: bool = True
    expected_match: Optional[str] = None
    tier: str = "SYSTEM"

class TestScripts:
    # 1. 가벼운 베이스라인 연산 (예상 Exec: 1~5ms)
    LEGACY_NORMAL = ScriptDef(
        title="Integrity: Light Compute (Simple Math)",
        code="print(sum([x**2 for x in range(1000)]))",
        expect_success=True,
        expected_match="332833500"
    )
    
    # 2. 무거운 CPU 연산 부하 (예상 Exec: 50~200ms)
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

    # 3. 메모리 및 직렬화/역직렬화 부하 (예상 Exec: 30~100ms)
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

    # 4. 시간 누수 및 멱등성 검증 (Determinism)
    TIME_LEAK = ScriptDef(
        title="Determinism: Sandbox enforces 0.0s fallback time",
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
    
    # 5. 시스템 탈옥 및 보안 방어선 테스트
    ENV_LEAK = ScriptDef(
        title="Isolation: Prevent Host Environment Variable Leakage",
        code="""
import os
is_virtual = '__LLVM_PROFILE_RT_INIT_ONCE' in os.environ
current_path = os.environ.get('PATH', '')
is_host_path_blocked = len(current_path.split(':')) <= 3 and 'Users' not in current_path
print(f'Isolated: {is_virtual and is_host_path_blocked}')
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
    
    # 6. 자원 고갈 공격 방어선 테스트 (Tier=STANDARD)
    INFINITE_LOOP_ATTACK = ScriptDef(
        title="Resource: Opcode-based Fuel Exhaustion",
        code="x = 2\nwhile True: x = x ** 2",
        expect_success=False,
        expected_match="timeout", 
        tier="STANDARD"
    )
    OOM_ATTACK = ScriptDef(
        title="Resource: Heap Fragmentation OOM Guard",
        code="""
lst = []
while True:
    lst.append('A' * (1024 * 1024))
        """,
        expect_success=False,
        expected_match="MemoryError", 
        tier="STANDARD"
    )
    STACK_OVERFLOW_ATTACK = ScriptDef(
        title="Resource: Deep Recursion Guard (Stack Overflow)",
        code="def recurse(n):\n    return recurse(n+1)\nrecurse(1)",
        expect_success=False,
        expected_match="RecursionError"
    )