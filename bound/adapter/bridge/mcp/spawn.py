# bound.adapter.bridge.mcp.spawn
## @lineage: bound.channel.mcp.bridge.spawn
## @lineage: bound.agent.mcp.bridge
## @lineage: phase.bind.stdio.bridge
## @lineage: bound.transport.spawn.mcp
## @lineage: bound.transport.mcp
"""
Surgent MCP Stdio Runner Bridge
Provides a secure, isolated mechanism to spawn and connect to external 
(cross-language or standalone) MCP servers via Standard I/O streams.
"""
from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from mcp.client.stdio import stdio_client, StdioServerParameters
from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.spawn")

class BridgeSpawn:
    """
    Factory for spawning standalone MCP servers (Node.js, Go, Python binaries)
    in isolated subprocesses and returning their IO streams as a Transport.
    """

    @classmethod
    @asynccontextmanager
    async def spawn(
        cls, 
        command: str, 
        args: list[str], 
        env_vars: dict[str, str] | None = None,
        inherit_env: bool = False
    ) -> AsyncIterator[Any]:
        """
        Spawns an external binary and yields its Stdio Transport.
        
        Args:
            command: The executable command (e.g., "npx", "uv", "go").
            args: List of arguments for the command.
            env_vars: Specific environment variables to inject.
            inherit_env: If False (Default), isolates the subprocess from the host's 
                         environment variables to prevent token/secret leakage.
        """
        # 1. 보안 격리: 호스트의 민감한 환경변수(API Keys)가 외부 바이너리로 새어나가는 것을 방지
        safe_env = {}
        if inherit_env:
            safe_env.update(os.environ)
        else:
            # 필수 시스템 환경변수만 최소한으로 상속 (PATH 등)
            for key in ["PATH", "USER", "HOME", "UV_INDEX"]:
                if key in os.environ:
                    safe_env[key] = os.environ[key]
        
        if env_vars:
            safe_env.update(env_vars)

        server_params = StdioServerParameters(
            command=command,
            args=args,
            env=safe_env
        )

        log.info(f"Spawning external MCP server: {command} {' '.join(args)}")

        # 2. 파이프(STDIO) 생성 및 yielding
        # 반환된 read_stream, write_stream은 통합 Client가 Transport로 인식하여 처리합니다.
        try:
            async with stdio_client(server_params) as (read_stream, write_stream):
                log.debug(f"Stdio transport established for {command}")
                # Tuple 형태의 Transport 스트림 반환
                yield (read_stream, write_stream)
        except Exception as e:
            log.error(f"Failed to spawn stdio server '{command}': {str(e)}")
            raise

    # --- Agentic Convenience Methods (에이전트 편의성 팩토리) ---

    @classmethod
    def create_node_runner(cls, package_name: str, args: list[str] | None = None) -> AsyncIterator[Any]:
        """Node.js 기반의 외부 MCP 서버를 npx로 실행 (예: @modelcontextprotocol/server-postgres)"""
        cmd_args = ["-y", package_name] + (args or [])
        return cls.spawn(command="npx", args=cmd_args)

    @classmethod
    def create_python_runner(cls, script_path: str, args: list[str] | None = None) -> AsyncIterator[Any]:
        """독립된 가상환경의 Python MCP 서버를 uv로 실행"""
        cmd_args = ["run", script_path] + (args or [])
        return cls.spawn(command="uv", args=cmd_args)