# bound.adapter.bridge.mcp.completor
## @lineage: bound.channel.mcp.bridge.completor
"""
@phase: MCP Autocomplete Bridge
@desc: Provides dynamic context resolution for Agent inputs.
Helps the agent narrow down valid arguments before executing tools or prompts.
"""
from __future__ import annotations

from typing import Any
from mcp.client.client import Client
from mcp_types import PromptReference, ResourceTemplateReference
from watcher.plane.emitter import get_emitter

log = get_emitter("bridge.completor")

class AgentContextCompletor:
    """
    Utility class to fetch valid argument completions from an MCP server,
    acting as a dynamic schema resolver for the Agent's LLM router.
    """

    @staticmethod
    async def get_resource_hints(
        client: Client, 
        uri_template: str, 
        arg_name: str, 
        partial_value: str = "",
        context_args: dict[str, str] | None = None
    ) -> list[str]:
        """
        에이전트가 특정 리소스(예: Github Repo)를 탐색할 때, 유효한 하위 경로를 서버에 질의합니다.
        """
        log.debug(f"Requesting resource hints for '{arg_name}' on {uri_template}")
        result = await client.complete(
            ref=ResourceTemplateReference(type="ref/resource", uri=uri_template),
            argument={"name": arg_name, "value": partial_value},
            context_arguments=context_args,
        )
        hints = result.completion.values if result.completion else []
        log.debug(f"Received hints: {hints}")
        return hints

    @staticmethod
    async def get_prompt_hints(
        client: Client, 
        prompt_name: str, 
        arg_name: str, 
        partial_value: str = ""
    ) -> list[str]:
        """
        에이전트가 서버의 내장 프롬프트를 호출할 때, 유효한 매개변수(예: style, role)를 질의합니다.
        """
        log.debug(f"Requesting prompt hints for '{arg_name}' on prompt '{prompt_name}'")
        result = await client.complete(
            ref=PromptReference(type="ref/prompt", name=prompt_name),
            argument={"name": arg_name, "value": partial_value},
        )
        return result.completion.values if result.completion else []