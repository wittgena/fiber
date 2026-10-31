# fiber.dev.e2e.gateway.llm.guardrail
## @lineage: fiber.dev.e2e.llm.guardrail
from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path

from fiber.llm.entry import acompletion
from fiber.gateway.llm.state.traverser import StateTraverser
from fiber.dev.base.workflow.builder import (
    E2EBaseWorkflow,
    E2EBaseApplication,
    create_e2e_parser,
    build_e2e_context,
    ReportMsg,
)
from fiber.llm.pipeline.tracer import DebugTracer
from fiber.llm.pipeline.interceptor.cache import EdgeRedisCache
from fiber.llm.pipeline.interceptor.guardrail import (
    ActiveBlockingGuardrail,
    CustomRegexBlockingGuardrail,
    CustomRegexMaskingGuardrail,
    PayloadSanitizationGuardrail,
)
from fiber.llm.compat.secret.registry import RedactRegistryIO
from fiber.llm.compat.secret.redact import (
    BASE_SECRET_RULES,
    SECRET_KEY_PATTERNS,
    configure_redaction,
)
from xphi.state.topos.workflow import WorkflowMessage, step


class BaselineAndCacheMsg(WorkflowMessage): pass
class PreTranslateMaskingMsg(WorkflowMessage): pass
class PostTranslateBlockingMsg(WorkflowMessage): pass
class SecretRegistryInjectionMsg(WorkflowMessage): pass
class ChainedDefenseAndFallbackMsg(WorkflowMessage): pass


class LlmGuardrailWorkflow(E2EBaseWorkflow):
    async def execute(self) -> None:
        self.log.info(f"[{self.name}] Igniting LLM Guardrail & Secret Registry Suite (Model: {self.target_model})")
        self.post_message(BaselineAndCacheMsg())
        await self.run()

    @step
    async def phase_baseline_and_cache(self, msg: BaselineAndCacheMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 1] BASELINE & CACHE: Tracer Hook & Edge Cache Short-Circuit")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "N/A"

        try:
            # 1) Baseline Unary Call with Tracer
            test_tracer = DebugTracer()
            response = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Reply with one word: OK."}],
                interceptors=[test_tracer],
            )
            await asyncio.sleep(0.01)
            if not (test_tracer.started and test_tracer.ended):
                raise ValueError("Tracer lifecycle hooks not fired.")

            usage_tokens = StateTraverser.resolve(response, "usage.total_tokens", "N/A")

            # 2) EdgeRedisCache Short-Circuit (0ms Network I/O)
            cached_res = await acompletion(
                model=self.target_model,
                messages=[{"role": "user", "content": "Hello, USE_CACHE."}],
                interceptors=[EdgeRedisCache()],
            )
            if "[CACHED] Hit!" not in cached_res.choices[0].message.content:
                raise ValueError("Cache miss. EdgeRedisCache failed to short-circuit.")

            is_success = True
        except Exception as e:
            self.log.error(str(e))

        self.record_result(1, "Baseline Tracer & Edge Cache Short-Circuit", is_success, t0, usage_tokens=usage_tokens)
        return PreTranslateMaskingMsg()

    @step
    async def phase_pre_translate_masking(self, msg: PreTranslateMaskingMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 2] PRE_TRANSLATE MASKING: In-Place PII & Payload Sanitization")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "N/A"

        try:
            pii_messages = [
                {"role": "user", "content": "User phone is 010-1234-5678. Reply with one word: ACK."}
            ]
            mask_guard = CustomRegexMaskingGuardrail([r"\b010-\d{4}-\d{4}\b"])
            sanitize_guard = PayloadSanitizationGuardrail()

            res = await acompletion(
                model=self.target_model,
                messages=pii_messages,
                interceptors=[sanitize_guard, mask_guard],
            )

            mutated_text = pii_messages[0]["content"]
            if "010-1234-5678" in mutated_text or "***REDACTED***" not in mutated_text:
                raise ValueError(f"CustomRegexMaskingGuardrail failed to mask PII in-place: {mutated_text}")

            usage_tokens = StateTraverser.resolve(res, "usage.total_tokens", "0")
            is_success = True
        except Exception as e:
            self.log.error(str(e))

        self.record_result(2, "PRE_TRANSLATE In-Place PII Masking", is_success, t0, usage_tokens=usage_tokens)
        return PostTranslateBlockingMsg()

    @step
    async def phase_post_translate_blocking(self, msg: PostTranslateBlockingMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 3] POST_TRANSLATE BLOCKING: Built-in Secret & Custom C-Regex")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "Blocked"

        try:
            # 1) Built-in ActiveBlockingGuardrail (Standard API Key)
            blocked_builtin = False
            try:
                await acompletion(
                    model=self.target_model,
                    messages=[{"role": "user", "content": "Key: sk-proj-1234567890abcdefghijklmnop"}],
                    interceptors=[ActiveBlockingGuardrail()],
                )
            except PermissionError:
                blocked_builtin = True

            if not blocked_builtin:
                raise RuntimeError("ActiveBlockingGuardrail failed to block built-in secret.")

            # 2) CustomRegexBlockingGuardrail (Domain Policy Pattern)
            blocked_custom = False
            try:
                await acompletion(
                    model=self.target_model,
                    messages=[{"role": "user", "content": "Confidential: SECRET-SSN-999"}],
                    interceptors=[CustomRegexBlockingGuardrail([r"SECRET-SSN-\d{3}"])],
                )
            except PermissionError:
                blocked_custom = True

            if not blocked_custom:
                raise RuntimeError("CustomRegexBlockingGuardrail failed to block custom pattern.")

            is_success = True
        except Exception as e:
            self.log.error(str(e))

        self.record_result(3, "POST_TRANSLATE Strict Blocking (Built-in & Custom)", is_success, t0, usage_tokens=usage_tokens)
        return SecretRegistryInjectionMsg()

    @step
    async def phase_secret_registry_injection(self, msg: SecretRegistryInjectionMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 4] SECRET REGISTRY: Dynamic Key-Value Rule Injection & DIP Verification")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "Blocked"

        # 초기 Key-Value 룰셋 및 키 백업 (테스트 완료 후 완벽 복원용)
        orig_rules = dict(BASE_SECRET_RULES)
        orig_keys = list(SECRET_KEY_PATTERNS)

        try:
            # Key-Value 스키마 테스트: 정상 패턴 1개 + 문법 오류 패턴 1개(Partial Update 검증) + 기존 룰 비활성화 1개
            custom_registry_payload = {
                "patterns": {
                    "fiber_internal_token": r"FIBER-INTERNAL-TOKEN-[0-9]{6}",
                    "broken_regex_rule": r"[INVALID_UNCLOSED_REGEX",  # 경고 후 건너뛰어야 함
                },
                "disabled_rules": ["context7_key"],
                "extra_secret_keys": ["FIBER_VAULT_PIN"],
            }

            with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8", delete=False) as tmp_file:
                json.dump(custom_registry_payload, tmp_file)
                tmp_path = tmp_file.name

            try:
                # 1) RedactRegistryIO를 통해 외부 JSON 로드 및 하위 redact 엔진 역주입(DIP)
                merged_rules, _ = RedactRegistryIO.initialize_and_inject(custom_path_env=tmp_path)

                if "context7_key" in merged_rules:
                    raise ValueError("Failed to disable 'context7_key' via disabled_rules.")

                # 2) 코드 수정 없이 ActiveBlockingGuardrail이 새로 주입된 패턴(FIBER-INTERNAL-TOKEN-123456)을 즉시 차단하는지 검증
                blocked_injected = False
                try:
                    await acompletion(
                        model=self.target_model,
                        messages=[{"role": "user", "content": "Leaked token: FIBER-INTERNAL-TOKEN-123456"}],
                        interceptors=[ActiveBlockingGuardrail()],
                    )
                except PermissionError:
                    blocked_injected = True

                if not blocked_injected:
                    raise RuntimeError("ActiveBlockingGuardrail did not enforce dynamically injected registry pattern.")

                is_success = True
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        except Exception as e:
            self.log.error(str(e), exc_info=True)
        finally:
            # 하위 redact 코어를 원래 베이스 룰셋으로 원복
            configure_redaction(
                rules=orig_rules,
                secret_keys=orig_keys,
            )

        self.record_result(4, "Secret Registry Dynamic Rule Injection (DIP)", is_success, t0, usage_tokens=usage_tokens)
        return ChainedDefenseAndFallbackMsg()

    @step
    async def phase_chained_defense_and_fallback(self, msg: ChainedDefenseAndFallbackMsg) -> WorkflowMessage:
        self.log.info(f"\n[{self.name}] [Phase 5] CHAINED DEFENSE & RESILIENCE: [Mask -> Block] & Fallback Shield")
        t0 = time.perf_counter()
        is_success = False
        usage_tokens = "N/A"

        try:
            # 1) Chained Defense: PRE_TRANSLATE에서 소프트 PII는 마스킹되어 POST_TRANSLATE 블로커를 무사히 통과해야 함
            tracer = DebugTracer()
            pii_masker = CustomRegexMaskingGuardrail([r"\b010-\d{4}-\d{4}\b"])
            secret_blocker = ActiveBlockingGuardrail()

            chained_msgs = [{"role": "user", "content": "Phone: 010-9999-8888. Reply with one word: DONE."}]
            await acompletion(
                model=self.target_model,
                messages=chained_msgs,
                interceptors=[tracer, pii_masker, secret_blocker],
            )
            await asyncio.sleep(0.01)
            if not (tracer.started and tracer.ended) or "***REDACTED***" not in chained_msgs[0]["content"]:
                raise ValueError("Chained [Mask -> Block] pipeline failed on soft PII.")

            # 2) Chained Defense: 소프트 PII와 하드 시크릿(sk-...)이 동시에 들어있으면 마스킹 후에도 POST_TRANSLATE에서 정확히 차단되어야 함
            blocked_mixed = False
            try:
                mixed_msgs = [{"role": "user", "content": "Phone: 010-9999-8888, Key: sk-proj-1234567890abcdefghijklmnop"}]
                await acompletion(
                    model=self.target_model,
                    messages=mixed_msgs,
                    interceptors=[pii_masker, secret_blocker],
                )
            except PermissionError:
                blocked_mixed = True

            if not blocked_mixed:
                raise RuntimeError("Chained pipeline failed to block hard secret when mixed with soft PII.")

            # 3) Fallback Shielding
            fallback_tracer = DebugTracer()
            fb_response = await acompletion(
                model="invalid/will-fail-model",
                messages=[{"role": "user", "content": "Reply with one word: PASS."}],
                fallbacks=[self.target_model],
                interceptors=[fallback_tracer],
            )
            await asyncio.sleep(0.01)
            if not (fallback_tracer.started and fallback_tracer.ended and not fallback_tracer.error):
                raise ValueError("Tracer leak detected during fallback shielding.")

            usage_tokens = StateTraverser.resolve(fb_response, "usage.total_tokens", "N/A")
            is_success = True
        except Exception as e:
            self.log.error(str(e))

        self.record_result(5, "Chained Defense (Mask->Block) & Fallback Shield", is_success, t0, usage_tokens=usage_tokens)
        return ReportMsg()


def main(args: list[str] = None):
    parser = create_e2e_parser("LLM Guardrail & Secret Registry E2E Suite Runner")
    parsed_args, _ = parser.parse_known_args(args)
    scope_kwargs, run_context = build_e2e_context(parsed_args)
    app = E2EBaseApplication(
        workflow_cls=LlmGuardrailWorkflow,
        workflow_name="GuardrailSuiteApp",
        scope_kwargs=scope_kwargs,
        run_context=run_context,
    )
    app.execute()

if __name__ == "__main__":
    main()