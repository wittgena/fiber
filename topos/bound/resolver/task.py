# topos.bound.resolver.task
## @lineage: void.topos.bound.resolver.task
## @lineage: gov.resolver.task
## @lineage: ops.resolver.task
import asyncio
import socket
import json
import sys
import argparse
from enum import Enum
from pathlib import Path
from dataclasses import asdict
from typing import List, Dict, Any, Optional, Union, Tuple, Type

from bound.resolver.model.tier import model_tier_registry
from gov.factory.action import CoreAction

from topos.bound.resolver.spec import SchemeCategory, TransactionDomain, TraceDomain, BRIDGE_SPEC, TRANSACTION_SPEC, TRACER_SPEC
from topos.bound.resolver.bridge import SchemeBlueprint, TransactionBlueprint, TraceBlueprint

from arch.topos.bound.surge.blueprint import SurgeBlueprint
from arch.gov.trans.logic.analyzer import LogicAnalyzer
from arch.contract.schema.graph import GraphSchema

from phase.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

SCHEME_ROOT = resolve_path("scheme")
log = get_emitter("scheme.manager")

# -------------------------------------------------------------------------
# Static Definitions
# -------------------------------------------------------------------------

_RESOLUTION_DICT: Dict[str, Any] = {
    "topology_name": "Semantic Resolution Funnel",
    "focus": "Resolution Hacking & Architectural Signaling",
    "depth_limit": 4,
    "relations_constraint": "sequential",
    
    "system_instructions": """
        You are executing a Resolution Hacking funnel.
        Your objective is to traverse the boundary between low-level structural repair and high-level human consensus.
        1. Investigate anomalies using terminal/file tools.
        2. Apply decoupling and dynamic factory pattern fixes.
        3. CRITICAL: Do not halt after fixing. You MUST invoke the `signal` tool to broadcast Semantic Telemetry.
            Translate your mechanical diffs into architectural value to evangelize the fix to human architects.
        4. Wait for human consensus (merge/accept), then use the `finish` tool to finalize.
    """,
    
    "nodes": [
        {
            "id": "step_1_rupture_analysis",
            "intent": "explore",
            "action": "terminal",
            "description": "[LOW-LEVEL] Scan system logs and stack traces to identify the structural rupture.",
            "expected_outcome": "Identify the root cause (e.g., Duplicate Class Definition in Pydantic registry)."
        },
        {
            "id": "step_2_synthesis_fix",
            "intent": "modify",
            "action": "terminal",
            "description": "[LOW-LEVEL] Use sed or file operations to decouple legacy static imports and apply dynamic factory patterns.",
            "expected_outcome": "Codebase is structurally aligned with the Single Source of Truth."
        },
        {
            "id": "step_3_semantic_telemetry",
            "intent": "evangelize",
            "action": CoreAction.SIGNAL.value,
            "params_template": {
                "channel": "slack_#architecture",
                "audience": "architect",
                "technical_context": "Removed static Action imports and replaced with CoreTool registry evaluation.",
                "semantic_translation": "🚀 *Architecture Update*: Decoupled the core Agent loop from legacy static classes. We now rely 100% on the dynamic Factory registry, enabling infinite tool scaling without Pydantic collisions. Ready for review.",
                "requires_consensus": True
            },
            "description": "[HIGH-LEVEL] Translate the structural fix into a compelling architectural win and request human consensus.",
            "expected_outcome": "Message delivered to human boundary, system paused for user validation."
        },
        {
            "id": "step_4_state_commit",
            "intent": "commit",
            "action": CoreAction.FINISH.value,
            "description": "Upon human consensus, merge the state into 'collapse_log.md' and finalize the trajectory.",
            "expected_outcome": "ConverStatus.FINISHED"
        }
    ]
}

RESOLUTION_BLUEPRINT: SurgeBlueprint = SurgeBlueprint.model_validate(_RESOLUTION_DICT)

class BlueprintType(Enum):
    SCHEME = "scheme"
    TRANSACTION = "transaction"
    TRACER = "tracer"
    RESOLUTION = "resolution"

# -------------------------------------------------------------------------
# Unified Task Resolver (Data + Routing + Execution)
# -------------------------------------------------------------------------

class TaskResolver:
    """
    @desc: 통합 시스템 오케스트레이터
    1. 레지스트리 관리 (Blueprint 로드 및 컴파일)
    2. 환경 분석 (네트워크/프록시/모델 상태 평가)
    3. 라우팅 및 실행 (CLI 커맨드 ➔ DAG/프롬프트 매핑 및 실행)
    """
    ULTIMATE_LOCAL_MODEL = "local-gemma-3"
    
    def __init__(self, run_context: Optional[dict] = None, launcher_cls: Optional[Type[Any]] = None, root_path: str = None, auto_load_default: bool = True):
        # Execution Engine Setup
        self.run_context = run_context or {}
        self.launcher_cls = launcher_cls
        
        # Registry & Paths Setup
        self.code_root = Path(SCHEME_ROOT)
        self.repo_root = Path(root_path).resolve() if root_path else None
        
        self._schemes: Dict[SchemeCategory, SchemeBlueprint] = {}
        self._transactions: Dict[TransactionDomain, TransactionBlueprint] = {}
        self._tracers: Dict[TraceDomain, TraceBlueprint] = {}
        self._resolutions: Dict[str, SurgeBlueprint] = {}
        
        if auto_load_default:
            self._load_defaults()

    # =========================================================================
    # [Phase 1] Data Registry & Blueprint Compilation
    # =========================================================================
    def _load_defaults(self):
        """정적 스펙 정의들을 메모리에 적재합니다."""
        self._schemes = BRIDGE_SPEC.copy()
        self._transactions = TRANSACTION_SPEC.copy()
        self._tracers = TRACER_SPEC.copy()
        self._resolutions = {"resolution_hacking": RESOLUTION_BLUEPRINT}
        log.info("[TaskResolver] Loaded Universal Blueprints (Schemes, Transactions, Tracers, Resolutions).")

    def get_executable_surge(self, category: Union[Enum, str], b_type: BlueprintType) -> Optional[SurgeBlueprint]:
        """대상 Blueprint를 기계가 실행 가능한 형태(SurgeBlueprint DAG)로 컴파일하여 반환합니다."""
        blueprint = None
        if b_type == BlueprintType.SCHEME:
            blueprint = self._schemes.get(category)
        elif b_type == BlueprintType.TRANSACTION:
            blueprint = self._transactions.get(category)
        elif b_type == BlueprintType.TRACER:
            blueprint = self._tracers.get(category)
        elif b_type == BlueprintType.RESOLUTION:
            return self._resolutions.get(category)
            
        return blueprint.compile_to_surge() if blueprint else None

    # getters for pure data inspection
    def get_all_schemes(self) -> Dict[SchemeCategory, SchemeBlueprint]: return self._schemes
    def get_all_transactions(self) -> Dict[TransactionDomain, TransactionBlueprint]: return self._transactions
    def get_all_tracers(self) -> Dict[TraceDomain, TraceBlueprint]: return self._tracers

    # =========================================================================
    # [Phase 2] Environment & Scope Resolution
    # =========================================================================
    @classmethod
    def _check_network_connectivity(cls, host: str = "8.8.8.8", port: int = 53, timeout: float = 2.0) -> bool:
        """물리적 네트워크 경계(온오프라인 상태)를 검증합니다."""
        try:
            socket.setdefaulttimeout(timeout)
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
            return True
        except OSError:
            return False

    @classmethod
    def resolve_environment(cls, requested_model: Optional[str], requested_proxy: bool) -> Tuple[Dict[str, Any], str, bool]:
        """[Static] 시스템 상태에 따라 최적의 (Scope Params, 모델, 프록시 사용 여부)를 도출합니다."""
        resolved_model = requested_model
        use_proxy = requested_proxy
        is_online = cls._check_network_connectivity()

        if not is_online:
            log.warning("🚨 [System Offline] Network connectivity lost. Forcing fallback to Local Engine.")
            resolved_model = cls.ULTIMATE_LOCAL_MODEL
            use_proxy = False
        elif not resolved_model:
            optimal_model = model_tier_registry.get_optimal_model(requires_tools=True)
            if optimal_model:
                resolved_model = f"gemini/{optimal_model}"
                log.info(f"✅ Registry resolved optimal model: {resolved_model}")
            else:
                log.warning(f"⚠️ Registry exhausted. Falling back to local model: {cls.ULTIMATE_LOCAL_MODEL}")
                resolved_model = cls.ULTIMATE_LOCAL_MODEL
                use_proxy = False

        scope_kwargs = {
            "use_proxy": use_proxy,
            "show_logs": True,
            "model": resolved_model  
        }
        return scope_kwargs, resolved_model, use_proxy

    # =========================================================================
    # [Phase 3] Routing & Execution
    # =========================================================================
    async def route_and_execute(self, args: argparse.Namespace):
        """CLI arguments를 분석하여 적절한 실행 경로(단일 프롬프트/대화형/DAG)로 라우팅합니다."""
        if not self.launcher_cls:
            log.error("❌ Execution failed: launcher_cls is not injected into TaskResolver.")
            return

        if args.interactive:
            await self._start_interactive_mode()
            return
            
        if args.prompt:
            await self._execute_prompt(args.prompt)
            return

        # Resolve Blueprint Topology based on flags
        surge_dag = None
        if args.resolution:
            surge_dag = self.get_executable_surge("resolution_hacking", BlueprintType.RESOLUTION)
        elif args.transaction:
            surge_dag = self.get_executable_surge(TransactionDomain(args.transaction), BlueprintType.TRANSACTION)
        else:
            category = SchemeCategory(args.scenario) if args.scenario else SchemeCategory.AGENT
            surge_dag = self.get_executable_surge(category, BlueprintType.SCHEME)

        if surge_dag:
            log.info(f"🚀 Launching Executable Surge DAG: {surge_dag.topology_name}")
            await self._run_surge_blueprint(surge_dag)
        else:
            log.error("Failed to compile or locate the requested topology blueprint.")

    async def _execute_prompt(self, instruction: str):
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_task(instruction=instruction)
        self._print_residue("TRACE", instruction[:30], trace)

    async def _run_surge_blueprint(self, blueprint: SurgeBlueprint):
        app = self.launcher_cls("RootAgentApp", run_context=self.run_context)
        trace = await app.setup_default_nodes().run_scheme(blueprint=blueprint)
        entry_name = getattr(blueprint, "topology_name", "Structured Scheme")
        self._print_residue("SCHEME", entry_name, trace)

    async def _start_interactive_mode(self):
        use_proxy = self.run_context.get("use_proxy", False)
        log.info(f"Interactive CLI Mode started (Proxy Mode: {use_proxy}). Type 'exit' to terminate.")
        
        while True:
            try:
                prompt = await asyncio.to_thread(input, "\n🤖 [Agent Prompt]> ")
                prompt = prompt.strip()
                if not prompt: continue
                if prompt.lower() in ['exit', 'quit']: break
                await self._execute_prompt(prompt)
            except (KeyboardInterrupt, EOFError):
                log.info("\nSession context disrupted. Exiting...")
                break

    def _print_residue(self, trace_type: str, context_name: str, trace: Any):
        log.info("\n" + "="*50)
        log.info(f"FINAL HYBRID RESIDUE ({trace_type}) FOR: '{context_name}'")
        log.info(trace)
        log.info("="*50)

    # =========================================================================
    # [Phase 4] Legacy Operations (Topology Generator)
    # =========================================================================
    def generate_topology_schema(self) -> GraphSchema:
        """레거시 파일 스캐닝 및 구조 분석을 통해 토폴로지 스키마를 생성합니다."""
        if not self.repo_root or not self.repo_root.exists():
            log.error(f"[Error] Directory not found or not provided: {self.repo_root}")
            sys.exit(1)

        log.info(f"[Scanner] Indexing {self.repo_root.name}...")
        module_index = {
            ".".join(p.relative_to(self.repo_root).with_suffix("").parts): p 
            for p in self.repo_root.rglob("*.py")
        }

        log.info("[Linker] Building Topology Phase...")
        analyzer = LogicAnalyzer()
        analyzer.build_structure(module_index, self.repo_root)
        result = analyzer.get_dissolve_schema(module_index, self.repo_root)
        self._write_json(result)
        return result

    def _write_json(self, data: GraphSchema):
        output_dir = self.code_root / "node"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{self.repo_root.name}.link.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(asdict(data), f, indent=2, ensure_ascii=False)
        log.info(f"[Export] Dissolve topology saved to {output_path}")