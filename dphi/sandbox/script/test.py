# dphi.sandbox.script.test
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List

@dataclass(frozen=True)
class ScriptDef:
    title: str
    code: str
    expect_success: bool = True
    expected_match: Optional[str] = None
    tier: str = "SYSTEM"

class TestScripts:
    LEGACY_NORMAL = ScriptDef(
        title="Integrity: Complex Language Semantics",
        code="print(sum([x**2 for x in range(10)]))",
        expect_success=True,
        expected_match="285"
    )
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
    ENV_LEAK = ScriptDef(
        title="Isolation: Prevent Host Environment Variable Leakage",
        code="""
import os
is_virtual = '__LLVM_PROFILE_RT_INIT_ONCE' in os.environ

# WASM 런타임이 자체적으로 생성한 최소한의 가상 PATH만 존재하는지 확인
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

class TestPayloads:
    """WASM 네이티브 함수에 주입할 표준 페이로드 딕셔너리"""
    PHASE_GEN       = {"topo": 50, "press": -10, "rupture": False}
    INJECTED_STATE  = {"topo": 100, "press": 200, "rupture": True, "injected_anchor": 999999, "injected_tick": 77}
    VALID_PACKET    = {"packet_id": "123", "files": {"model.bin": "hash"}}
    MALFORMED_JSON  = '{"topo": 50, "press": -10, "rupture": '

@dataclass(frozen=True)
class TestConstants:
    """테스트 한계치 및 환경 상수 관리"""
    PAYLOAD_10K: str = "A" * 10_000
    PAYLOAD_50K: str = "A" * 50_000
    PAYLOAD_150K: str = "A" * 150_000
    
    SCALE_STEPS: List[int] = field(default_factory=lambda: [1, 5, 17, 46, 71, 128, 256, 353])
    
    MAX_TIMEOUT: float = 35.0 
    MEM_WARN_LIMIT: float = 85.0
    CPU_WARN_LIMIT: float = 95.0
    
    T_ID: int = 101010
    P_ID: int = 999999
    N_ID: int = 907049
    
    INJECTED_CTX: Dict[str, Any] = field(default_factory=lambda: {
        "timestamp": 1600000000, 
        "seed": "proof_of_compute_seed_777"
    })

CONST = TestConstants()
