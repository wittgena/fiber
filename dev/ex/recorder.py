# fiber.dev.ex.recorder
import os
import sys
import argparse
import asyncio
import time
from typing import Any
from pathlib import Path

from fiber.dev.trace.llm.vcr.manager import VCRIdentityRule

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path, get_invoker
from xphi.arch.bound.event.next import next_trace_id

log = get_emitter("ex.recorder")
FIXTURE_ROOT = resolve_path("fixture")
CURRENT_INVOKER, _ = get_invoker(Path(__file__))

def init_vcr_environment(mode: str, speed: str, chaos_ms: float, tick_ms: float):
    """Initialize VCR sandbox and inject network proxies."""
    if mode not in ("record", "replay"):
        log.info("🟢 [VCR] Mode: LIVE (Integration Bypassed)")
        return

    try:
        from fiber.phase.cli.sandbox import verify_local_dev_environment, create_security_sandbox
        verify_local_dev_environment(allow_ci=True)
        create_security_sandbox(vcr_mode=mode)
        log.info("🔒 [Sandbox] Strict Security Policy Injected")
    except ImportError:
        log.warning("⚠️ [Sandbox] Sandbox package not found, proceeding with injector only.")
    except Exception as e:
        log.error(f"🚨 [SANDBOX VIOLATION] Execution Denied: {e}")
        sys.exit(1)

    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    
    config = VCRPlaybackConfig(
        mode=mode, 
        speed=speed, 
        chaos_latency_ms=chaos_ms,
        record_tick_ms=tick_ms
    )
    VCRInjector.apply(config=config, fixture_dir=FIXTURE_ROOT)

    log.info(f"🔌 [VCR] Status: ENGAGED ({mode.upper()}) | Speed: {speed} | Chaos: {chaos_ms}ms | Tick: {tick_ms}ms")
    log.info(f"📂 [VCR] Fixture Storage: {os.path.abspath(FIXTURE_ROOT)}\n")


async def run_scenario(
    scenario_name: str,
    profile: "BaseLLMProfile",
    messages_data: list[dict],
    stream: bool = True
):
    from fiber.llm.model.message import Message
    from fiber.dev.ex.facade.driver import LLMFacade
    import fiber.dev.ex.facade.observer 
    from fiber.gateway.llm.mapper.traverser import StateTraverseRule
    
    try:
        from fiber.llm.param import ModelResponseStream
    except ImportError:
        ModelResponseStream = None

    log.info(f"\n--- Starting Scenario: {scenario_name} ---")
    log.info(f"Payload: model={profile.model}, stream={stream}")

    messages = []
    for m_data in messages_data:
        content = m_data.get("content")
        if isinstance(content, str):
            m_data["content"] = [{"type": "text", "text": content}]
        
        try:
            messages.append(Message.model_validate(m_data))
        except Exception as e:
            log.warning(f"Message validation failed, falling back to raw init: {e}")
            messages.append(Message(**m_data))

    counters = {
        "chunk_count": 0,
        "legacy_valid": 0,
        "fiber_valid": 0,
        "mismatch": 0
    }

    async def on_token(chunk: Any):
        counters["chunk_count"] += 1
        
        if counters["chunk_count"] == 1:
            print(f"\n[🔍 DUCK-TYPING INSPECTOR - First Chunk]")
            print(f"   ├─ Actual Type  : {type(chunk)}")
            if ModelResponseStream:
                is_compatible = isinstance(chunk, ModelResponseStream)
                print(f"   ├─ isinstance() : {'✅ PASS' if is_compatible else '❌ FAIL'}")
            print("-" * 40 + "\n")

        legacy_content = ""
        try:
            if hasattr(chunk, "choices") and chunk.choices:
                legacy_content = chunk.choices[0].delta.content or ""
            elif isinstance(chunk, dict) and "choices" in chunk:
                legacy_content = chunk["choices"][0].get("delta", {}).get("content", "")
                
            if legacy_content:
                counters["legacy_valid"] += 1
        except Exception:
            pass

        try:
            fiber_content = StateTraverseRule.extract_stream_content(chunk, default="")
            if fiber_content:
                counters["fiber_valid"] += 1
        except Exception:
            fiber_content = ""

        if legacy_content != fiber_content:
            counters["mismatch"] += 1

        if fiber_content:
            print(fiber_content, end="", flush=True)

    start_time = time.perf_counter()
    
    try:
        if stream:
            log.info("Receiving stream...")
        
        # 1. VCR 픽스처 생성을 위한 결정론적 Trace ID 계산 로직
        formatted_scenario = scenario_name.replace(" ", "_").lower()
        vcr_mode = os.environ.get("VCR_MODE", "live")
        
        seed = VCRIdentityRule.generate_seed(
            scenario_name=formatted_scenario,
            messages=messages_data,
            invoker=CURRENT_INVOKER
        )

        # Record나 Replay일 때는 seed를 써서 고정 ID, Live일 때는 랜덤 ID 발급
        trace_id = next_trace_id(seed) if vcr_mode in ("record", "replay") else next_trace_id()
        trace_metadata = {
            "vcr_scenario": formatted_scenario,
            "vcr_invoker": CURRENT_INVOKER
        }

        # Facade 호출 시 터널링(Tunneling)을 위한 예약어 명시적 주입
        response = await LLMFacade.make_completion(
            llm=profile,
            messages=messages,
            stream=stream,
            on_token=on_token if stream else None,
            trace_id=trace_id,
            metadata=trace_metadata
        )

        duration = (time.perf_counter() - start_time) * 1000
        
        if stream:
            print("\n\n" + "-"*40)
            log.info(f"✅ [STREAM CLOSED] Duration: {duration:.2f}ms")
            log.info(f"   ├─ Total Chunks Received : {counters['chunk_count']}")
            log.info(f"   ├─ Direct Extracted      : {counters['legacy_valid']}")
            log.info(f"   ├─ Fiber Mapper Extracted: {counters['fiber_valid']}")
            log.info(f"   └─ Data Mismatch Count   : {counters['mismatch']}\n")
        else:
            safe_text = response.message.content if response.message else ""
            log.info(f"⚡ Singular response: {safe_text[:50]}...")
            log.info(f"✅ [SUCCESS] Duration: {duration:.2f}ms")

        metrics = response.metrics
        log.info(f"💰 Cost: ${metrics.accumulated_cost:.6f}")
        
        if metrics.accumulated_token_usage:
            u = metrics.accumulated_token_usage
            total = getattr(u, 'total_tokens', getattr(u, 'prompt_tokens', 0) + getattr(u, 'completion_tokens', 0))
            log.info(f"📊 Tokens: [Prompt: {getattr(u, 'prompt_tokens', 0)} / Completion: {getattr(u, 'completion_tokens', 0)} / Total: {total}]")

    except Exception as e:
        log.error(f"🚨 [UNEXPECTED FATAL ERROR] {type(e).__name__}: {e}")
        raise
    finally:
        log.info(f"--- Scenario: {scenario_name} finished ---\n")


async def execute_scenarios(args: argparse.Namespace):
    log.info("🚀 Fiber VCR Utility Interactive Demonstration")
    from fiber.llm.model.profile import BaseLLMProfile
    
    primary_profile = BaseLLMProfile(
        model=args.model,
        api_key=os.environ.get("GEMINI_API_KEY", "sk-mock-key"),
        input_cost_per_token=0.00000015,
        output_cost_per_token=0.00000060,
    )

    long_prompt = (
        "As a principal enterprise architect, write a comprehensive and deeply technical guide "
        "(at least 5 paragraphs) on the architectural benefits of using a Facade pattern to wrap "
        "multiple LLM Provider SDKs (like OpenAI, Gemini, Anthropic). Discuss preventing vendor lock-in, "
        "centralizing cost and token metrics, ensuring secure data boundaries, and handling rate limits. "
        "Include practical examples of how a unified API surface simplifies microservice integration."
    )

    await run_scenario(
        scenario_name="Long Stream Document",
        profile=primary_profile,
        messages_data=[{"role": "user", "content": long_prompt}],
        stream=True
    )

    log.info("\n================================================================================")
    if args.vcr == "record":
        log.info("✅ [RECORD COMPLETE] Network I/O has been frozen into fixtures.")
        log.info(f"👉 Next Step: python -m dev.ex.recorder -m {args.model} --vcr replay --vcr-speed real")
    elif args.vcr == "replay":
        log.info("✅ [REPLAY COMPLETE] Time-travel API mocking successful.")
    else:
        log.info("✅ [LIVE COMPLETE] Normal API execution without VCR.")
    log.info("================================================================================\n")

def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber Facade VCR Utility")
    parser.add_argument("-m", "--model", type=str, default="gemini/gemini-3.1-flash-lite", help="Target LLM model")
    parser.add_argument("--vcr", type=str, choices=["live", "record", "replay"], default="record", help="VCR mode")
    parser.add_argument("--vcr-speed", type=str, choices=["max", "real"], default="max", help="Replay speed")
    parser.add_argument("--vcr-chaos", type=float, default=0.0, help="Latency jitter (ms)")
    parser.add_argument("--vcr-tick", type=float, default=100.0, help="Time-window (ms) for coalescing chunks during record (0 for raw)")
    
    parsed_args, _ = parser.parse_known_args(args)
    os.environ["VCR_MODE"] = parsed_args.vcr
    
    os.makedirs(FIXTURE_ROOT, exist_ok=True)
    init_vcr_environment(
        mode=parsed_args.vcr, 
        speed=parsed_args.vcr_speed, 
        chaos_ms=parsed_args.vcr_chaos,
        tick_ms=parsed_args.vcr_tick
    )
    
    asyncio.run(execute_scenarios(parsed_args))

if __name__ == "__main__":
    main()