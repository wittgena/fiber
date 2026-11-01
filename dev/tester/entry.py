# fiber/dev/tester/entry.py
from __future__ import annotations

import argparse
import asyncio
import time
from typing import Any, Dict, List, Optional

from fiber.llm.entry import acompletion
from fiber.llm.pipeline.interceptor.guardrail import (
    ActiveBlockingGuardrail,
    CustomRegexMaskingGuardrail,
    PayloadSanitizationGuardrail,
)


def _extract_text_from_choice(choice: Any, is_stream: bool = False) -> str:
    if isinstance(choice, dict):
        target = choice.get("delta" if is_stream else "message") or {}
        return target.get("content") or ""
    target = getattr(choice, "delta" if is_stream else "message", None)
    if target is None:
        return ""
    if isinstance(target, dict):
        return target.get("content") or ""
    return getattr(target, "content", None) or ""


async def run_step(
    step_name: str,
    model: str,
    prompt: str,
    stream: bool = False,
    interceptors: Optional[List[Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    print(f"\n================================================================================")
    print(f"▶ [{step_name}] model={model} | stream={stream}")
    print(f"  └─ Prompt: \"{prompt}\"")
    print(f"--------------------------------------------------------------------------------")

    t0 = time.perf_counter()
    status = "OK"
    full_text = ""

    call_kwargs: Dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
        "interceptors": interceptors or [],
    }

    try:
        response = await asyncio.wait_for(
            acompletion(**call_kwargs),
            timeout=timeout,
        )

        if stream and hasattr(response, "__aiter__"):
            print("[Stream] ", end="", flush=True)
            async for chunk in response:
                choices = getattr(chunk, "choices", None) or (
                    chunk.get("choices") if isinstance(chunk, dict) else []
                )
                if choices:
                    text = _extract_text_from_choice(choices[0], is_stream=True)
                    if text:
                        full_text += text
                        print(text, end="", flush=True)
            print()
        else:
            if stream:
                print("[Note] Provider returned a non-streaming response object; falling back to unary read.")
            choices = getattr(response, "choices", None) or (
                response.get("choices") if isinstance(response, dict) else []
            )
            if choices:
                full_text = _extract_text_from_choice(choices[0], is_stream=False)
            print(f"[Response] {full_text}")

    except PermissionError as pe:
        status = "BLOCKED"
        print(f"[Guardrail Block] {pe}")
    except asyncio.TimeoutError:
        status = "TIMEOUT"
        print(f"[Timeout] Upstream call exceeded {timeout:.1f}s")
    except Exception as e:
        status = "ERROR"
        print(f"[Error] {type(e).__name__}: {e}")

    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"--------------------------------------------------------------------------------")
    print(f"✔ Status: {status} | Elapsed: {elapsed_ms:.1f}ms | Length: {len(full_text)} chars")

    return {
        "step": step_name,
        "stream": stream,
        "status": status,
        "elapsed_ms": elapsed_ms,
    }


def print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n================================================================================")
    print("                           ENTRY TEST SUMMARY                                   ")
    print("================================================================================")
    print(f"{'Step':<30} | {'Stream':<6} | {'Status':<8} | {'Time (ms)':<10}")
    print("-" * 80)
    for r in results:
        print(
            f"{r['step']:<30} | {str(r['stream']):<6} | "
            f"{r['status']:<8} | {r['elapsed_ms']:>8.1f}ms"
        )
    print("================================================================================\n")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Basic Fiber LLM Entry Tester")
    parser.add_argument("-m", "--model", default="gemini/gemini-3.1-flash-lite", help="Target model")
    parser.add_argument("-p", "--prompt", default=None, help="Run a single custom prompt")
    parser.add_argument("-s", "--stream", action="store_true", help="Enable streaming (single prompt mode)")
    parser.add_argument("-g", "--guardrail", action="store_true", help="Enable DLP guardrails (single prompt mode)")
    parser.add_argument("-t", "--timeout", type=float, default=30.0, help="Per-step timeout in seconds")
    args = parser.parse_args()

    default_guardrails = [
        PayloadSanitizationGuardrail(),
        CustomRegexMaskingGuardrail([r"\b010-\d{4}-\d{4}\b"]),
        ActiveBlockingGuardrail(),
    ]

    results: List[Dict[str, Any]] = []

    if args.prompt:
        results.append(
            await run_step(
                step_name="Custom Call",
                model=args.model,
                prompt=args.prompt,
                stream=args.stream,
                interceptors=default_guardrails if args.guardrail else [],
                timeout=args.timeout,
            )
        )
    else:
        # 1. 기본 단발 호출 (Unary)
        results.append(
            await run_step(
                step_name="1. Unary Completion",
                model=args.model,
                prompt="Reply with one short sentence defining an SDK.",
                stream=False,
                timeout=args.timeout,
            )
        )

        # 2. 스트리밍 호출 (Stream)
        results.append(
            await run_step(
                step_name="2. Stream Completion",
                model=args.model,
                prompt="Count from 1 to 5 separated by commas.",
                stream=True,
                timeout=args.timeout,
            )
        )

        # 3. 가드레일 마스킹 통과 (PRE_TRANSLATE PII Masking)
        results.append(
            await run_step(
                step_name="3. Guardrail (PII Masking)",
                model=args.model,
                prompt="My phone is 010-1234-5678. Say 'Received'.",
                stream=False,
                interceptors=default_guardrails,
                timeout=args.timeout,
            )
        )

        # 4. 가드레일 시크릿 차단 (POST_TRANSLATE Secret Block)
        results.append(
            await run_step(
                step_name="4. Guardrail (Secret Block)",
                model=args.model,
                prompt="Leaked key: sk-proj-1234567890abcdef1234567890abcdef",
                stream=False,
                interceptors=[ActiveBlockingGuardrail()],
                timeout=args.timeout,
            )
        )
    print_summary(results)

if __name__ == "__main__":
    asyncio.run(main())