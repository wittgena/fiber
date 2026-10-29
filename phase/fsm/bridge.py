# fiber.phase.fsm.bridge
from typing import Dict, Any, Optional, Tuple, List
import httpx

from fiber.llm.entry import acompletion
from fiber.llm.pipeline.channel.breaker import StreamBreaker
from fiber.llm.pipeline.channel.observer import MeteringObserver
from fiber.infra.ops.finops.watcher import default_finops_watcher, default_tenant_eco
from xphi.arch.model.auth import NodeAuthPayload
from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.compiler import CompactKey, NodeType
from fiber.phase.fsm.runtime import BranchCursor, BranchController

log = get_emitter("fsm.bridge", phase="ACTUATOR_BRIDGE")

class FiberExecutionBridge:
    def __init__(
        self,
        runtime: BranchController,
        tenant_id: str = "default_tenant",
        mcp_gateway_url: str = "http://127.0.0.1:8356/v1/mcp-gateway",
        interceptors: Optional[List[Any]] = None,
        http_timeout: float = 30.0,
    ):
        self.runtime = runtime
        self.tenant_id = tenant_id
        self.mcp_gateway_url = mcp_gateway_url.rstrip("/")
        self.interceptors = interceptors or []
        self._http_client = httpx.AsyncClient(timeout=http_timeout)

    async def aclose(self) -> None:
        await self._http_client.aclose()

    async def __call__(
        self,
        cursor: BranchCursor,
        node_spec: Dict[str, Any],
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        node_type = node_spec.get(CompactKey.TYPE)

        # 1. Pure Control / Terminal Nodes: Zero external I/O, zero fuel cost
        if node_type in (NodeType.ROUTER.value, NodeType.ACT.value):
            return {}, None, 0

        # 2. LLM Inference & Cognitive Nodes -> Fiber Drop-in LLM Pipeline
        if node_type in (
            NodeType.PROJECTOR.value,
            NodeType.REFLECT.value,
            NodeType.TENSION.value,
        ):
            return await self._execute_llm_node(cursor, task_io)

        # 3. External Tool / Sidecar Nodes -> Fiber MCP Gateway
        if node_type in (NodeType.OPERSIG.value, NodeType.PENDING.value):
            return await self._execute_mcp_node(cursor, task_io)

        return {}, None, 1

    # ------------------------------------------------------------------------
    # [Actuator 1] Fiber LLM Pipeline + StreamBreaker + VCR + Projection
    # ------------------------------------------------------------------------
    async def _execute_llm_node(
        self,
        cursor: BranchCursor,
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        call_params = self._resolve_dynamic_params(task_io.get("call_params", {}), cursor.meta_ctx)
        vcr_cfg = task_io.get("vcr") or {}
        projection = task_io.get("projection") or {}

        model = call_params.pop("model", "auto")
        messages = call_params.pop("messages", [{"role": "user", "content": str(cursor.meta_ctx)}])
        stream = bool(call_params.pop("stream", False))

        auth_payload = NodeAuthPayload(
            tenant_id=self.tenant_id,
            fuel_budget=cursor.fuel,
            is_enforced=True,
        )

        response = await acompletion(
            model=model,
            messages=messages,
            stream=stream,
            interceptors=self.interceptors,
            stream_breaker=StreamBreaker(finops_watcher=default_finops_watcher),
            metering_observer=MeteringObserver(tenant_eco=default_tenant_eco, pta_adapter=self.runtime.pta),
            metadata={
                "node_auth": auth_payload,
                "checkpoint_handle": cursor.pointer.to_key(),
                "vcr_scenario": f"{cursor.stream_id}_{cursor.curr_node}_{cursor.tick}",
                **vcr_cfg,
            },
            **call_params,
        )

        raw_dump = await self._normalize_llm_response(response, stream=stream)
        tokens_used = self._extract_path(raw_dump, "usage.total_tokens") or 10

        # Extract lightweight routing variables via Projection.extract_keys
        meta_updates: Dict[str, Any] = {}
        for key_path in projection.get("extract_keys", []):
            val = self._extract_path(raw_dump, key_path)
            if val is not None:
                meta_updates[key_path] = val
                meta_updates[key_path.split(".")[-1]] = val

        # Extract heavy payload target via Projection.offload_path, otherwise offload full response dump
        offload_path = projection.get("offload_path")
        offload_payload = self._extract_path(raw_dump, offload_path) if offload_path else raw_dump
        if not isinstance(offload_payload, dict):
            offload_payload = {"data": offload_payload}

        return meta_updates, offload_payload, max(1, int(tokens_used))

    # ------------------------------------------------------------------------
    # [Actuator 2] Fiber MCP Gateway + Idempotent Checkpoint Handle + HITL
    # ------------------------------------------------------------------------
    async def _execute_mcp_node(
        self,
        cursor: BranchCursor,
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        resource_uri = task_io.get("resource_uri", "")
        target_id = resource_uri.split("/")[-1] if "/" in resource_uri else resource_uri.split("://")[-1]
        resolved_args = self._resolve_dynamic_params(task_io.get("call_params", {}), cursor.meta_ctx)
        projection = task_io.get("projection") or {}
        extras = task_io.get("extras") or {}

        payload = {
            "checkpoint_handle": cursor.pointer.to_key(),
            "arguments": resolved_args,
            "meta": cursor.meta_ctx,
        }

        resp = await self._http_client.post(f"{self.mcp_gateway_url}/{target_id}/invoke", json=payload)

        # HTTP 202 (SEP-2322 Pre-Flight HITL Yield): Park cursor without advancing state
        if resp.status_code == 202:
            hitl_data = resp.json()
            log.info(f"[Bridge:{cursor.stream_id}] MCP Pre-Flight HITL Lock triggered at '{cursor.curr_node}'.")
            return (
                {
                    "_hitl_yield": True,
                    "_hitl_ticket": hitl_data.get("ticket_id", ""),
                },
                hitl_data,
                0,
            )

        resp.raise_for_status()
        tool_result = resp.json()
        if not isinstance(tool_result, dict):
            tool_result = {"data": tool_result}

        # Extract lightweight routing variables via Projection.extract_keys
        meta_updates: Dict[str, Any] = {"intent_type": "tool_completed", "_hitl_yield": False}
        for key_path in projection.get("extract_keys", []):
            val = self._extract_path(tool_result, key_path)
            if val is not None:
                meta_updates[key_path] = val
                meta_updates[key_path.split(".")[-1]] = val

        default_cost = int(extras.get("step_cost", tool_result.get("step_cost", 5)))
        return meta_updates, tool_result, max(1, default_cost)

    # ------------------------------------------------------------------------
    # [Internal Helpers] Stream Coalescing & Dot-Path Parameter Resolution
    # ------------------------------------------------------------------------
    @staticmethod
    def _normalize_llm_response(response: Any, stream: bool) -> Any:
        # Retained as coroutine below
        pass

    @staticmethod
    async def _normalize_llm_response(response: Any, stream: bool) -> Dict[str, Any]:
        if stream and hasattr(response, "__aiter__"):
            chunks: List[str] = []
            async for chunk in response:
                if hasattr(chunk, "choices") and chunk.choices:
                    delta = getattr(chunk.choices[0], "delta", None)
                    if delta and getattr(delta, "content", None):
                        chunks.append(delta.content)
            text = "".join(chunks)
            return {"content": text, "raw_stream_payload": text, "usage": {"total_tokens": max(1, len(chunks))}}

        if hasattr(response, "model_dump"):
            return response.model_dump()
        if isinstance(response, dict):
            return response
        return {"raw": str(response)}

    @classmethod
    def _resolve_dynamic_params(cls, params: Dict[str, Any], meta_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Resolves ASL dynamic parameter references (e.g., `"query.$": "$.event.tool_args"`) from `meta_ctx`."""
        resolved: Dict[str, Any] = {}
        for k, v in params.items():
            if k.endswith(".$") and isinstance(v, str):
                clean_key = k[:-2]
                resolved[clean_key] = cls._extract_path(meta_ctx, v) or meta_ctx.get(v, v)
            else:
                resolved[k] = v
        return resolved

    @staticmethod
    def _extract_path(data: Dict[str, Any], path: Optional[str]) -> Any:
        if not path or not isinstance(data, dict):
            return None
        if path in data:
            return data[path]
        clean = path.lstrip("$.")
        if clean in data:
            return data[clean]

        curr: Any = data
        for part in clean.split("."):
            if isinstance(curr, dict) and part in curr:
                curr = curr[part]
            elif isinstance(curr, list) and part.isdigit() and int(part) < len(curr):
                curr = curr[int(part)]
            else:
                return None
        return curr