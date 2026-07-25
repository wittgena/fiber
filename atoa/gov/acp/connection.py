# atoa.gov.acp.connection
## @lineage: agent.gov.acp.connection
## @lineage: gov.acp.connection
## @lineage: gov.policy.acp.connection
## @lineage: gov.protocol.acp.connection
from __future__ import annotations
import asyncio
import time
from typing import Any

from acp.helpers import text_block
from atoa.gov.acp.conn import ClientSideConnection
from watcher.plane.emitter import get_emitter

from atoa.gov.acp.client import ACPClient
from atoa.gov.acp.support import (
    _USAGE_UPDATE_TIMEOUT,
    _STREAM_READER_LIMIT,
    _select_auth_method,
    _resolve_bypass_mode,
    _build_session_meta, 
    _maybe_set_session_model,
    _filter_jsonrpc_lines
)

log = get_emitter(name="acp.connection", phase="agent_infrastructure")

class ACPConnectionManifold:
    """
    @desc: Isolated Substrate Manifold Object
    @role: Encapsulates asynchronous JSON-RPC protocol state, subprocess lifecycle, and session forks.
           Operates as an injected component, not an inherited mixin.
    """
    def __init__(
        self, 
        executor: Any, 
        client: ACPClient, 
        working_dir: str,
        command: str,
        args: list[str],
        env: dict[str, str],
        acp_model: str | None,
        acp_session_mode: str | None
    ):
        self.executor = executor
        self.client = client
        self.working_dir = working_dir
        self.command = command
        self.args = args
        self.env = env
        self.acp_model = acp_model
        self.acp_session_mode = acp_session_mode

        # Encapsulated Topological State
        self.conn: Any = None
        self.process: Any = None
        self.session_id: str | None = None
        self.agent_name: str = ""
        self.agent_version: str = ""

    def boot(self) -> None:
        ## @desc: Synchronous wrapper mapping to the asynchronous boot sequence
        (
            self.conn, self.process, _, 
            self.session_id, self.agent_name, self.agent_version
        ) = self.executor.run_async(self._async_boot_sequence)

    async def _async_boot_sequence(self) -> tuple[Any, Any, Any, str, str, str]:
        ## @desc: Subprocess generation and protocol handshake vector
        process = await asyncio.create_subprocess_exec(
            self.command, *self.args, 
            stdin=asyncio.subprocess.PIPE, 
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.PIPE,
            env=self.env, limit=_STREAM_READER_LIMIT,
        )
        
        filtered_reader = asyncio.StreamReader(limit=_STREAM_READER_LIMIT)
        asyncio.get_event_loop().create_task(_filter_jsonrpc_lines(process.stdout, filtered_reader))
        conn = ClientSideConnection(self.client, process.stdin, filtered_reader)

        init_response = await conn.initialize(protocol_version=1)
        a_name = init_response.agent_info.name or "" if init_response.agent_info else ""
        a_ver = init_response.agent_info.version or "" if init_response.agent_info else ""

        if auth_methods := init_response.auth_methods or []:
            method_id = _select_auth_method(auth_methods, self.env)
            if method_id:
                auth_kwargs = {"gateway": {"baseUrl": self.env["GEMINI_BASE_URL"]}} if method_id == "gemini-api-key" and "GEMINI_BASE_URL" in self.env else {}
                await conn.authenticate(method_id=method_id, **auth_kwargs)

        session_meta = _build_session_meta(a_name, self.acp_model)
        response = await conn.new_session(cwd=self.working_dir, **session_meta)
        sess_id = response.session_id
        
        await _maybe_set_session_model(conn, a_name, sess_id, self.acp_model)
        await conn.set_session_mode(mode_id=self.acp_session_mode or _resolve_bypass_mode(a_name), session_id=sess_id)
        
        return conn, process, filtered_reader, sess_id, a_name, a_ver

    def fork_and_prompt(self, question: str, record_usage_cb: Any) -> str:
        ## @desc: Wraps the fork sequence and ensures synchronous return
        if not self.conn or not self.session_id:
            raise RuntimeError("Topological failure: Manifold not initialized.")

        with self.client._fork_lock:
            return self.executor.run_async(self._async_fork_and_prompt, question, record_usage_cb)

    async def _async_fork_and_prompt(self, question: str, record_usage_cb: Any) -> str:
        ## @desc: Forks the active session and injects a hypothesis stimulus
        fork_resp = await self.conn.fork_session(cwd=self.working_dir, session_id=self.session_id)
        fork_id = fork_resp.session_id

        self.client._fork_session_id = fork_id
        self.client._fork_accumulated_text.clear()
        
        try:
            fork_t0 = time.monotonic()
            usage_sync = self.client.prepare_usage_sync(fork_id)
            response = await self.conn.prompt([text_block(question)], fork_id)
            
            if self.client.get_turn_usage_update(fork_id) is None:
                try: 
                    await asyncio.wait_for(usage_sync.wait(), timeout=_USAGE_UPDATE_TIMEOUT)
                except TimeoutError: 
                    log.warning(f"[LATENCY] Fork UsageUpdate sync failed within {_USAGE_UPDATE_TIMEOUT}s.")
            
            record_usage_cb(
                response, 
                fork_id, 
                elapsed=time.monotonic() - fork_t0, 
                usage_update=self.client.pop_turn_usage_update(fork_id)
            )
            return "".join(self.client._fork_accumulated_text)
        finally:
            self.client._fork_session_id = None
            self.client._fork_accumulated_text.clear()

    def teardown(self) -> None:
        ## @desc: Safely collapse the physical subprocess and connections
        if self.conn and self.executor:
            try: self.executor.run_async(self.conn.close())
            except Exception: pass
            self.conn = None

        if self.process:
            try: self.process.terminate(); self.process.kill()
            except Exception: pass
            self.process = None