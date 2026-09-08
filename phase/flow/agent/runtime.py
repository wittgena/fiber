# fiber.phase.flow.agent.runtime
## @lineage: surgent.topos.controller
import os
import json
from typing import Dict, Any, Optional

from pydantic import SecretStr

from fiber.agent.conver.loop.executor import NodeExecutor
from fiber.agent.conver.space.manager import SpaceNode, space_provider

from fiber.llm.driver.model import LLMModel
from fiber.llm.driver.config.agent import AgentConfig
from fiber.phase.plane.scope.observer import ManifoldFolder, FlowTransition
from fiber.dphi.transaction.settlement import ClearingAdapter, TransactionReceipt

from xphi.arch.model.conv.tool import Tool
from xphi.arch.model.surge.blueprint import SurgeBlueprint
from xphi.arch.model.dphi.graph import EntryNode
from xphi.arch.model.sealer import EpochSealer
from xphi.kernel.space.topos.node.gan import Message, GanNode
from xphi.kernel.space.topos.node.event import AgentConfigured
from xphi.kernel.wasm.broker import DphiBroker
from xphi.kernel.wasm.cgroup import Tier
from xphi.watcher.plane.emitter import get_emitter

# ==========================================
# 1. Loggers & Environment Configurations
# ==========================================
log_flow = get_emitter("conv.flow")
log_runtime = get_emitter("runtime.node")

LOCAL_MODEL = os.getenv("LLAMA_MODEL_NAME", "openai/gemma-3-1b-it-Q4_K_M.gguf")
LOCAL_PORT = os.getenv("LLAMA_PORT", "8080")
LOCAL_URL = os.getenv("LLM_BASE_URL", f"http://localhost:{LOCAL_PORT}/v1")

PROXY_URL = os.getenv("SANDBOX_SERVER_URL", "http://localhost:8000")
PROXY_WORKSPACE = os.getenv("SANDBOX_WORKSPACE_REF", "container-edge-123")
PROXY_API_KEY = os.getenv("SANDBOX_API_KEY", "dummy-token")


# ==========================================
# 2. Runtime Node Implementation
# ==========================================
class RuntimeNode(GanNode):
    def __init__(self, name: str, use_proxy: bool = False, target_model: str = None):
        super().__init__(name)
        self.settings = None
        self.use_proxy = use_proxy
        self.target_model = target_model

    def _init_local_llm_payload(self) -> dict:
        model_to_use = self.target_model or LOCAL_MODEL
        log_runtime.info(f"[{self.name}] 🔌 [Local] Binding engine to model: {model_to_use}")
        is_external = "gemini" in model_to_use or "openai/" in model_to_use
        base_url = None if is_external else LOCAL_URL
        api_key_val = None if is_external else "not-needed"

        llm_obj = LLMModel(
            model=model_to_use,
            base_url=base_url,
            api_key=SecretStr(api_key_val) if api_key_val else None,
        )
        return llm_obj.model_dump() if hasattr(llm_obj, "model_dump") else llm_obj.dict()

    def _init_proxy_llm_payload(self) -> dict:
        """@flow: Remote parameter extraction -> Ephemeral routing DTO synthesis"""
        log_runtime.info(f"[{self.name}] 🌐 [Proxy] Structuring remote connection profile: {PROXY_URL}")
        return {
            "model": self.target_model or LOCAL_MODEL,
            "server_url": PROXY_URL,
            "workspace_ref": PROXY_WORKSPACE,
            "session_api_key": PROXY_API_KEY,
            "is_proxy": True
        }

    async def on_boot(self, message: Message):
        """## @phase: Engine Assetization Initialization"""
        log_runtime.info(f"[{self.name}] ⚙️ Initializing engine allocation (Proxy Mode: {self.use_proxy})")

        try:
            llm_payload = None
            if self.use_proxy:
                try:
                    ## @step: Attempt priority remote proxy topology wiring
                    llm_payload = self._init_proxy_llm_payload()
                except Exception as e:
                    ## @step: Local standalone fallback on remote connectivity failure
                    log_runtime.warning(f"[{self.name}] ⚠️ Proxy allocation failed. Collapsing to Local Fallback: {e}")
                    self.use_proxy = False  
            
            if not self.use_proxy or llm_payload is None:
                ## @step: Resolve baseline local model specs
                llm_payload = self._init_local_llm_payload()

            ## @step: Construct deterministic capability descriptors (DTO)
            tools_dto = [
                Tool(name="terminal"), 
                Tool(name="file_editor")
            ]
            
            ## @step: Secure Pydantic boundary validation
            self.settings = AgentConfig(llm=llm_payload, tools=tools_dto)
            
            mode_str = "Proxy" if self.use_proxy else "Local"
            log_runtime.info(f"[{self.name}] ✓ 인프라 자산 구성 완료 ({mode_str} Mode).")
            
            self.post_message(AgentConfigured(settings=self.settings))
        except Exception as e:
            log_runtime.error(f"[{self.name}] ❌ Fatal exception during asset allocation: {e}")
            self.post_message(Message("shutdown", bubble=True))

    async def on_shutdown(self, message: Message):
        """## @phase: Sub-manifold Reclaim"""
        mode = "Proxy" if getattr(self, "use_proxy", False) else "Local"
        log_runtime.info(f"[{self.name}] 💤 Purging configuration engine ({mode}).")
        self._running = False
        self._queue.put_nowait(None)


# ==========================================
# 3. Topology & Flow Controllers
# ==========================================
class RuntimeController:
    @staticmethod
    def assemble_and_mount(topos: GanNode, run_context: dict, broker: Any, topology_spec: dict) -> None:
        use_proxy, target_model = run_context.get("use_proxy", False), run_context.get("target_model")
        log_flow.info(f"[ToposController] Wiring hybrid nodes (Proxy Mode: {use_proxy}, Model: {target_model})")
        
        active_nodes = {}
        
        # 동적 노드 스포닝 (Dynamic Node Spawning) - Multi-agent 확장을 위한 Factory Pattern
        for node_id, spec in topology_spec.items():
            node_type = spec.get("type", "default")
            
            # 하위 호환성 및 새로운 'type' 메타데이터 지원
            if node_id == "ConfigPolicyNode" or node_type == "executor":
                active_nodes[node_id] = NodeExecutor(node_id)
            elif node_id == "ConfigSettingsNode" or node_type == "runtime":
                active_nodes[node_id] = RuntimeNode(node_id, use_proxy=use_proxy, target_model=target_model)
            elif node_id == "DockerWorkspaceNode" or node_type == "workspace":
                active_nodes[node_id] = SpaceNode(node_id, provider=space_provider, use_proxy=use_proxy)
            else:
                log_flow.warning(f"[ToposController] Unknown node type or ID: {node_id}. Skipping initialization.")

        # 폴딩 및 토폴로지 마운트
        ManifoldFolder(broker=broker).fold_manifold(active_nodes=active_nodes, topology_spec=topology_spec)
        for node in active_nodes.values():
            topos.mount(node)
            
        log_flow.info(f"[ToposController] Topology spatial folding complete. Nodes: {list(active_nodes.keys())}")

    @staticmethod
    def dispatch_payload(policy_node: Any, blueprint: Optional[SurgeBlueprint], instruction: str, settings: dict) -> None:
        if blueprint:
            context_msg = Message("set_context")
            context_msg.entry_node = EntryNode(
                entry=blueprint.topology_name, focus=blueprint.focus,
                depth=blueprint.depth_limit, relations=blueprint.relations_constraint.split(',') if blueprint.relations_constraint else []
            )
            policy_node.post_message(context_msg)
            
            run_msg = Message("execute_events")
            run_msg.events = blueprint.nodes
            run_msg.settings = settings
            run_msg.system_instructions = getattr(blueprint, 'system_instructions', None)
            policy_node.post_message(run_msg)
        else:
            run_msg = Message("run_conversation")
            run_msg.instruction = instruction
            run_msg.settings = settings
            policy_node.post_message(run_msg)

    @staticmethod
    async def settle_dominium(
        origin_name: str,
        transition: FlowTransition,
        broker: DphiBroker,
        exchange_adapter: ClearingAdapter,
        cost: float,
        fuel_consumed: int
    ) -> Optional[TransactionReceipt]:
        entangled_state = {
            "parity": {"topos_id": f"task_{transition.id}", "phase_id": 0, "nexus_id": 0},
            "repos": {}
        }
        canonical_payload = EpochSealer.generate_seal_payload(entangled_state, parent_commit_id="genesis")
        res = await broker.invoke("seal_epoch", canonical_payload)
        
        signatures = []
        if res.success:
            try:
                signatures = json.loads(res.output).get("signatures", [])
            except json.JSONDecodeError:
                log_flow.warning(f"[{origin_name}] Unparseable response from seal_epoch.")
        else:
            log_flow.warning(f"[{origin_name}] Epoch sealing friction: {res.error}")

        receipt = exchange_adapter.finalize_settlement(
            entangled_state=entangled_state,
            signatures=signatures, 
            cost_metrics={"fuel_consumed": fuel_consumed, "accumulated_cost": cost},
            tier=Tier.STANDARD.value
        )
        log_flow.info(f"[{origin_name}] 🧾 Transaction Receipt Issued: {receipt.job_id}")
        transition.reach_dominium(resource_address=f"urn:surgent:resource:resolved_task_{transition.id}")
        return receipt

    @staticmethod
    def handle_rupture(origin_name: str, transition: FlowTransition, source: str, error: str):
        log_flow.critical(f"[{origin_name}] 🚨 Fatal topological rupture originating from [{source}]. Error: {error}")
        transition.record(f"System Error: {source} failed -> {error}")
        transition.fracture_topology(lmbda=0.2, tau=1.0, force_collapse=True)