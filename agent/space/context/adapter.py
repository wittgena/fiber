# agent.space.context.adapter
## @lineage: bound.space.context.adapter
## @lineage: bound.adapter.context.adapter
import json
import os
import sys
import re
from pathlib import Path
from collections.abc import Mapping
from typing import Any
from functools import lru_cache

from pydantic import ValidationError
from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemBytecodeCache,
    Template,
    TemplateNotFound,
)

from fiber.agent.space.action.action import Action, Observation
from fiber.agent.llm.driver.event.message import MessageEvent
from fiber.agent.llm.driver.event.observation import UserRejectObservation
from fiber.agent.space.action.types import ConversationID
from fiber.agent.llm.driver.model import LLMModel
from fiber.agent.space.action.message import Message, TextContent
from fiber.agent.loop.conv.stats import ConversationStats
from fiber.agent.loop.conv.security import SecurityAnalyzerBase
from xphi.arch.model.conv.security.confirm import ConfirmationPolicyBase, NeverConfirm

from fiber.agent.loop.runtime.protocol.context import AgentCommunicationProtocol, ExecutionControlProtocol, SecurityControlProtocol
from fiber.agent.loop.runtime.protocol.conv import ProtoConv, EngineContextProtocol
from fiber.agent.loop.runtime.protocol.conv import ConvStateProtocol
from fiber.agent.loop.conv.command import TransitionStatus, UpdateSecurityPolicy
from fiber.agent.space.context.actor import Actor
from fiber.agent.llm.driver.facade import MessageBuilder, LLMFacade

from xphi.arch.topos.context.status import ConverStatus
from xphi.arch.contract.resolver.secret import SecretValue
from xphi.arch.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class FlexibleFileSystemLoader(BaseLoader):
    def __init__(self, searchpath: str):
        self.searchpath = os.path.abspath(searchpath)

    def get_source(self, environment, template):
        if os.path.isabs(template):
            path = template
        else:
            path = os.path.join(self.searchpath, template)

        if not os.path.exists(path):
            raise TemplateNotFound(template)

        mtime = os.path.getmtime(path)
        with open(path, encoding="utf-8") as f:
            source = f.read()

        def uptodate():
            try:
                return os.path.getmtime(path) == mtime
            except OSError:
                return False

        return source, path, uptodate


def refine(text: str) -> str:
    if sys.platform == "win32":
        text = re.sub(r"\bterminal\b", "execute_powershell", text, flags=re.IGNORECASE)
        text = re.sub(
            r"(?<!execute_)(?<!_)\bbash\b", "powershell", text, flags=re.IGNORECASE
        )
    return text


@lru_cache(maxsize=64)
def _get_env(prompt_dir: str) -> Environment:
    if not prompt_dir:
        raise ValueError("prompt_dir is required")
    cache_folder = os.path.join(os.path.expanduser("~"), ".surgent", "cache", "jinja")
    os.makedirs(cache_folder, exist_ok=True)
    bcc = FileSystemBytecodeCache(directory=cache_folder)
    env = Environment(
        loader=FlexibleFileSystemLoader(prompt_dir),
        bytecode_cache=bcc,
        autoescape=False,
    )
    env.filters["refine"] = refine
    return env


@lru_cache(maxsize=256)
def _get_template(prompt_dir: str, template_name: str) -> Template:
    env = _get_env(prompt_dir)
    try:
        return env.get_template(template_name)
    except Exception:
        raise FileNotFoundError(
            f"Prompt file {os.path.join(prompt_dir, template_name)} not found"
        )


def render_template(prompt_dir: str, template_name: str, **ctx) -> str:
    if os.path.isabs(template_name):
        if not os.path.isfile(template_name):
            raise FileNotFoundError(f"Prompt file {template_name} not found")
        actual_dir = os.path.dirname(template_name)
        actual_filename = os.path.basename(template_name)
        tpl = _get_template(actual_dir, actual_filename)
    else:
        tpl = _get_template(prompt_dir, template_name)
    return refine(tpl.render(**ctx).strip())

class AgentCommunicator:
    def __init__(self, context: ProtoConv):
        self._context = context

    @property
    def id(self) -> ConversationID:
        return self._context.id

    def _dispatch(self, event: Any) -> None:
        """내부 이벤트 라우팅 헬퍼 (Conv의 _on_event 우회 호출)"""
        if hasattr(self._context, "_on_event"):
            self._context._on_event(event)
        else:
            self._context.state.events.append(event)

    def send_message(self, message: str | Message, sender: str | None = None) -> None:
        if isinstance(message, str):
            message = Message(role="user", content=[TextContent(text=message)])

        state = self._context.state
        
        if state.execution_status in [ConverStatus.FINISHED, ConverStatus.STUCK]:
            state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User sent a new message"))

        activated_skill_names: list[str] = []
        extended_content: list[TextContent] = []

        user_msg_event = MessageEvent(
            source="user",
            llm_message=message,
            activated_skills=activated_skill_names,
            extended_content=extended_content,
            sender=sender,
        )
        self._dispatch(user_msg_event)

    async def ask(self, question: str) -> str:
        """
        @desc: 메인 루프를 방해하지 않고 현재 상태를 기반으로 LLM에 비동기 질문을 던집니다.
               (기존 Sidecar의 역할을 내부로 흡수하고 await 누락 버그를 수정함)
        """
        registry = getattr(self._context, "llm_registry", None)
        base_llm = registry.get_default() if registry else LLMModel(model="gpt-4o")
        
        llm_to_use = base_llm.model_copy(update={"usage_id": "ask-agent-llm"}, deep=True)
        available_tools = list(getattr(self._context, "tools", {}).values())

        template_dir = Path(__file__).parent.parent.parent / "context" / "prompts" / "templates"
        
        # 내부 함수(render_template) 호출
        question_text = render_template(str(template_dir), "ask_agent_template.j2", question=question)

        user_message = Message(role="user", content=[TextContent(text=question_text)])
        messages = MessageBuilder.prepare_llm_messages(
            self._context.state.events, additional_messages=[user_message]
        )

        response = await LLMFacade.make_completion(
            llm=llm_to_use, 
            messages=messages, 
            tools=available_tools
        )
        
        message = response.message
        if message.content and len(message.content) > 0:
            for content in message.content:
                if isinstance(content, TextContent):
                    return content.text

        raise Exception("Failed to generate answer via AgentCommunicator.ask")


class ExecutionController:
    """
    @implements: ExecutionControlProtocol
    @desc: 스케줄러, 백그라운드 워커, 메인 루프 제어기가 대화를 실행하고 중지하기 위한 어댑터.
           실제 루프 실행은 순수 실행기로 정제된 Conver 클래스에 위임합니다.
    """
    def __init__(self, context: ProtoConv):
        self._context = context
        self._runner = Actor(context)

    async def run(self) -> None:
        await self._runner.run()

    def pause(self) -> None:
        self._runner.pause()

    async def close(self) -> None:
        if getattr(self._context, "_cleanup_initiated", False):
            return
        self._context._cleanup_initiated = True
        
        # Observability span 종료 시도
        try:
            self._context._end_observability_span()
        except AttributeError:
            pass
            
        conv_id_str = str(self._context.id)
        
        try:
            tunnel = await TunnelFactory.get_default()
            control_channel = f"agent_control:{conv_id_str}"
            payload = {"command": "shutdown", "conversation_id": conv_id_str}
            await tunnel.publish(control_channel, json.dumps(payload))
            log.info(f"Published shutdown signal to remote Agent via {control_channel}")
        except Exception as e:
            log.warning(f"Error publishing close signal to agent: {e}")
            
        # Executor 정리 로직 (Gov 로컬 툴 정리)
        if getattr(self._context, "delete_on_close", True):
            self._runner.close_executors()

    def rerun_actions(self, rerun_log_path: str | Path | None = None) -> bool:
        return self._runner.rerun_actions(rerun_log_path)

    def execute_tool(self, tool_name: str, action: Action) -> Observation:
        return self._runner.execute_tool(tool_name, action)


class SecurityManager:
    """
    @implements: SecurityControlProtocol
    @desc: 보안 미들웨어, 유저 컨펌 시스템, 시크릿 볼트(Vault)가 사용하는 가드레일 제어 어댑터.
           Conv를 거치지 않고 직접 state를 안전하게 조작합니다.
    """
    def __init__(self, context: ProtoConv):
        self._context = context

    @property
    def confirmation_policy_active(self) -> bool:
        return not isinstance(self._context.state.confirmation_policy, NeverConfirm)

    @property
    def is_confirmation_mode_active(self) -> bool:
        return (self._context.state.security_analyzer is not None and self.confirmation_policy_active)

    def set_confirmation_policy(self, policy: ConfirmationPolicyBase) -> None:
        self._context.state.apply(UpdateSecurityPolicy(confirmation_policy=policy))
        log.info(f"Confirmation policy set to: {policy}")

    def set_security_analyzer(self, analyzer: SecurityAnalyzerBase | None) -> None:
        self._context.state.apply(UpdateSecurityPolicy(security_analyzer=analyzer))
        log.info("Security analyzer updated.")

    def reject_pending_actions(self, reason: str = "User rejected the action") -> None:
        from fiber.agent.loop.conv.state import ConversationState
        state = self._context.state
        pending_actions = ConversationState.get_unmatched_actions(state.events)
        
        if state.execution_status == ConverStatus.WAITING_FOR_USER:
            state.apply(TransitionStatus(new_status=ConverStatus.IDLE, reason="User rejected pending action"))

        if not pending_actions:
            log.warning("No pending actions to reject")
            return

        for action_event in pending_actions:
            rejection_event = UserRejectObservation(
                action_id=action_event.id,
                tool_name=action_event.tool_name,
                tool_call_id=action_event.tool_call_id,
                rejection_reason=reason,
            )
            if hasattr(self._context, "_on_event"):
                self._context._on_event(rejection_event)
            else:
                state.events.append(rejection_event)
            log.info(f"Rejected pending action: {action_event} - {reason}")

    def update_secrets(self, secrets: Mapping[str, SecretValue]) -> None:
        secret_registry = self._context.state.secret_registry
        secret_registry.update_secrets(secrets)
        log.info(f"Added {len(secrets)} secrets to conversation")


class EngineContextAdapter:
    def __init__(self, context: ProtoConv):
        self._context = context

    @property
    def state(self) -> ConvStateProtocol:
        return self._context.state

    @property
    def conversation_stats(self) -> ConversationStats:
        return self._context.conversation_stats

    async def switch_profile(self, profile_name: str) -> None:
        """기존 SessionManager의 역할을 직접 흡수하여 Tunnel 발행 수행"""
        tunnel = await TunnelFactory.get_default()
        conv_id_str = str(self._context.id)
        control_channel = f"agent_control:{conv_id_str}"
        command_payload = {
            "command": "switch_profile",
            "profile_name": profile_name,
            "conversation_id": conv_id_str
        }
        
        await tunnel.publish(control_channel, json.dumps(command_payload))
        log.info(f"[EngineContextAdapter] Requested Agent profile switch to: {profile_name} via {control_channel}")

    def ensure_agent_ready(self) -> None:
        pass