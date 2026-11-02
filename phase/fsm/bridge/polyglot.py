# fiber.phase.fsm.bridge.polyglot
import asyncio
import importlib
import inspect
import json
import shlex
from typing import Any, Callable, Dict, List, Optional, Tuple

from xphi.watcher.plane.emitter import get_emitter

from fiber.phase.fsm.bridge.execution import FiberExecutionBridge
from fiber.phase.fsm.contract import (
    CompactKey,
    NodeType,
    ParsedResource,
    apply_projection,
    parse_resource_uri,
    resolve_dynamic_params,
)
from fiber.phase.fsm.runtime import BranchController, BranchCursor

log = get_emitter("fsm.bridge.polyglot", phase="POLYGLOT_BRIDGE")

LocalCallable = Callable[..., Any]


class PolyglotExecutionBridge(FiberExecutionBridge):
    def __init__(
        self,
        runtime: BranchController,
        tenant_id: str = "default_tenant",
        mcp_gateway_url: str = "http://127.0.0.1:8356/v1/mcp-gateway",
        interceptors: Optional[List[Any]] = None,
        http_timeout: float = 30.0,
        local_handlers: Optional[Dict[str, LocalCallable]] = None,
        allow_py_import: bool = True,
        allow_exec_subprocess: bool = False,
        exec_timeout: float = 30.0,
    ):
        super().__init__(
            runtime=runtime,
            tenant_id=tenant_id,
            mcp_gateway_url=mcp_gateway_url,
            interceptors=interceptors,
            http_timeout=http_timeout,
        )
        self._fn_registry: Dict[str, LocalCallable] = {}
        for k, v in (local_handlers or {}).items():
            self.register_fn(k, v)
        self.allow_py_import = allow_py_import
        self.allow_exec_subprocess = allow_exec_subprocess
        self.exec_timeout = exec_timeout

    def register_fn(self, name: str, handler: LocalCallable) -> "PolyglotExecutionBridge":
        """Registers an in-process sync or async callable for `fn://<name>` resources."""
        parsed = parse_resource_uri(name)
        self._fn_registry[parsed.target or parsed.target_id] = handler
        return self

    async def __call__(
        self,
        cursor: BranchCursor,
        node_spec: Dict[str, Any],
        task_io: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        node_type = node_spec.get(CompactKey.TYPE)

        if node_type == NodeType.OPERSIG.value:
            parsed = parse_resource_uri(task_io.get("resource_uri", ""))
            if parsed.is_polyglot_opersig:
                return await self._execute_polyglot_node(cursor, task_io, parsed)

        return await super().__call__(cursor, node_spec, task_io)

    async def _execute_polyglot_node(
        self,
        cursor: BranchCursor,
        task_io: Dict[str, Any],
        parsed: ParsedResource,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], int]:
        resolved_args = resolve_dynamic_params(task_io.get("call_params", {}), cursor.meta_ctx)
        projection = task_io.get("projection")
        extras = task_io.get("extras") or {}
        ckpt_handle = cursor.pointer.to_key()

        if parsed.scheme == "fn":
            raw_result = await self._invoke_registered_fn(parsed, resolved_args, cursor, ckpt_handle)
        elif parsed.scheme == "py":
            raw_result = await self._invoke_dynamic_py(parsed, resolved_args, cursor, ckpt_handle)
        elif parsed.scheme in ("http", "https"):
            raw_result = await self._invoke_direct_http(parsed, resolved_args, cursor, ckpt_handle, extras)
        elif parsed.scheme == "exec":
            raw_result = await self._invoke_subprocess(parsed, resolved_args, cursor, ckpt_handle, extras)
        else:
            return await super()._execute_mcp_node(cursor, task_io)

        return self._finalize_output(cursor, raw_result, projection, extras)

    # ----------------------------------------------------------------------------------
    # 1. In-Process Registered Function (`fn://<handler_name>`)
    # ----------------------------------------------------------------------------------
    async def _invoke_registered_fn(
        self,
        parsed: ParsedResource,
        resolved_args: Dict[str, Any],
        cursor: BranchCursor,
        ckpt_handle: str,
    ) -> Dict[str, Any]:
        handler = self._fn_registry.get(parsed.target) or self._fn_registry.get(parsed.target_id)
        if handler is None:
            raise KeyError(f"[PolyglotBridge:{cursor.stream_id}] Unregistered handler '{parsed.raw_uri}'")
        return await self._call_flexible_signature(handler, resolved_args, cursor.meta_ctx, ckpt_handle)

    # ----------------------------------------------------------------------------------
    # 2. Dynamic Python Module Callable (`py://<package.module>:<callable_name>`)
    # ----------------------------------------------------------------------------------
    async def _invoke_dynamic_py(
        self,
        parsed: ParsedResource,
        resolved_args: Dict[str, Any],
        cursor: BranchCursor,
        ckpt_handle: str,
    ) -> Dict[str, Any]:
        if not self.allow_py_import:
            raise PermissionError(f"[PolyglotBridge:{cursor.stream_id}] Dynamic 'py://' import is disabled.")

        target = parsed.target
        if ":" in target:
            mod_path, attr_name = target.split(":", 1)
        elif "." in target:
            mod_path, attr_name = target.rsplit(".", 1)
        else:
            raise ValueError(f"[PolyglotBridge] Invalid 'py://' URI '{parsed.raw_uri}'. Expected 'py://module:func'.")

        module = importlib.import_module(mod_path)
        handler = getattr(module, attr_name)
        return await self._call_flexible_signature(handler, resolved_args, cursor.meta_ctx, ckpt_handle)

    # ----------------------------------------------------------------------------------
    # 3. Direct REST / Webhook Server (`http://<url>` or `https://<url>`)
    # ----------------------------------------------------------------------------------
    async def _invoke_direct_http(
        self,
        parsed: ParsedResource,
        resolved_args: Dict[str, Any],
        cursor: BranchCursor,
        ckpt_handle: str,
        extras: Dict[str, Any],
    ) -> Dict[str, Any]:
        method = str(extras.get("method", resolved_args.pop("_http_method", "POST"))).upper()
        custom_headers = extras.get("headers") or resolved_args.pop("_http_headers", {})
        headers = {
            "X-Fiber-Checkpoint": ckpt_handle,
            "X-Fiber-Tenant": self.tenant_id,
            "X-Fiber-Stream": cursor.stream_id,
            **custom_headers,
        }

        if method in ("GET", "DELETE"):
            resp = await self._http_client.request(method, parsed.raw_uri, params=resolved_args, headers=headers)
        else:
            resp = await self._http_client.request(method, parsed.raw_uri, json=resolved_args, headers=headers)

        if resp.status_code == 202:
            try:
                hitl_data = resp.json()
            except Exception:
                hitl_data = {"status": 202, "raw": resp.text}
            if not isinstance(hitl_data, dict):
                hitl_data = {"data": hitl_data}
            hitl_data["_hitl_yield"] = True
            hitl_data.setdefault("ticket_id", resp.headers.get("X-Fiber-HITL-Ticket", ""))
            return hitl_data

        resp.raise_for_status()
        try:
            data = resp.json()
            return data if isinstance(data, dict) else {"data": data}
        except Exception:
            return {"data": resp.text, "status_code": resp.status_code}

    # ----------------------------------------------------------------------------------
    # 4. Local Binary / Shell Script (`exec://<command_or_path>`)
    # ----------------------------------------------------------------------------------
    async def _invoke_subprocess(
        self,
        parsed: ParsedResource,
        resolved_args: Dict[str, Any],
        cursor: BranchCursor,
        ckpt_handle: str,
        extras: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self.allow_exec_subprocess:
            raise PermissionError(
                f"[PolyglotBridge:{cursor.stream_id}] Direct 'exec://' subprocess execution is disabled. "
                f"Mount via 'fiber connect' as 'mcp://<target>' or enable allow_exec_subprocess=True."
            )

        argv = shlex.split(parsed.target)
        if not argv:
            raise ValueError(f"[PolyglotBridge] Empty command in '{parsed.raw_uri}'")

        stdin_envelope = json.dumps(
            {
                "checkpoint_handle": ckpt_handle,
                "stream_id": cursor.stream_id,
                "node": cursor.curr_node,
                "tick": cursor.tick,
                "arguments": resolved_args,
                "meta": cursor.meta_ctx,
            }
        ).encode("utf-8")

        timeout = float(extras.get("timeout", self.exec_timeout))
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(input=stdin_envelope), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            raise TimeoutError(f"[PolyglotBridge:{cursor.stream_id}] Subprocess '{argv[0]}' timed out after {timeout}s")

        stdout_text = stdout_b.decode("utf-8", errors="replace").strip()
        stderr_text = stderr_b.decode("utf-8", errors="replace").strip()

        if proc.returncode == 202:
            out = self._try_parse_json(stdout_text)
            out["_hitl_yield"] = True
            return out

        if proc.returncode != 0:
            raise RuntimeError(
                f"[PolyglotBridge:{cursor.stream_id}] Subprocess '{argv[0]}' failed (exit={proc.returncode}): "
                f"{stderr_text or stdout_text}"
            )

        return self._try_parse_json(stdout_text)

    # Helpers: Flexible Callable Invocation & Shared Contract Projection
    @staticmethod
    async def _call_flexible_signature(
        fn: LocalCallable,
        resolved_args: Dict[str, Any],
        meta_ctx: Dict[str, Any],
        ckpt_handle: str,
    ) -> Dict[str, Any]:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        param_names = {
            p.name
            for p in params
            if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        }
        pos_params = [
            p for p in params if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        has_pos_only = any(p.kind == inspect.Parameter.POSITIONAL_ONLY for p in params)

        # 1. If `resolved_args` keys directly match the function's named parameters (e.g., `a`, `b` or `p`), unpack as **kwargs
        if resolved_args and not has_pos_only and (
            set(resolved_args.keys()).issubset(param_names) or has_var_kw
        ):
            res = fn(**resolved_args)
        # 2. If single positional-only arg (e.g. C-builtins or single-arg stdlib) and 1 key in resolved_args, pass the value
        elif len(pos_params) == 1 and len(resolved_args) == 1 and next(iter(resolved_args.keys())) not in ("args", "params", "data"):
            first_param_name = pos_params[0].name
            if first_param_name not in ("args", "resolved_args", "params", "payload", "data"):
                res = fn(next(iter(resolved_args.values())))
            else:
                res = fn(resolved_args)
        # 3. Otherwise, dispatch as FSM-aware handler: (args, meta_ctx, ckpt_handle) / (args, meta_ctx) / (args)
        elif len(pos_params) >= 3:
            res = fn(resolved_args, meta_ctx, ckpt_handle)
        elif len(pos_params) == 2:
            res = fn(resolved_args, meta_ctx)
        else:
            res = fn(resolved_args)

        if inspect.isawaitable(res):
            res = await res

        return res if isinstance(res, dict) else {"data": res}

    @staticmethod
    def _finalize_output(
        cursor: BranchCursor,
        tool_result: Any,
        projection: Optional[Dict[str, Any]],
        extras: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], int]:
        payload_dict = tool_result if isinstance(tool_result, dict) else {"data": tool_result}

        if payload_dict.get("_hitl_yield") is True:
            ticket_id = str(payload_dict.get("_hitl_ticket") or payload_dict.get("ticket_id") or "")
            log.info(f"[PolyglotBridge:{cursor.stream_id}] Pre-Flight HITL Yield triggered at '{cursor.curr_node}'.")
            return (
                {"_hitl_yield": True, "_hitl_ticket": ticket_id},
                payload_dict,
                0,
            )

        # Delegates extraction & offloading directly to `contract.apply_projection`
        meta_updates, offload_payload = apply_projection(
            raw_output=payload_dict,
            projection=projection,
            base_meta={"intent_type": "tool_completed", "_hitl_yield": False},
        )
        default_cost = int(extras.get("step_cost", payload_dict.get("step_cost", 5)))
        return meta_updates, offload_payload, max(1, default_cost)

    @staticmethod
    def _try_parse_json(raw_text: str) -> Dict[str, Any]:
        if not raw_text:
            return {"data": ""}
        try:
            parsed = json.loads(raw_text)
            return parsed if isinstance(parsed, dict) else {"data": parsed}
        except Exception:
            return {"data": raw_text}