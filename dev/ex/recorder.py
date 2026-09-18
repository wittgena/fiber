# fiber.dev.ex.recorder
import os
import argparse
import asyncio
from typing import Any

from fiber.llm.entry import acompletion
from fiber.llm.mapper.traverser import StateTraverser
from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig
from fiber.dev.trace.llm.debugger import DebugTracer

from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.bind.resolver import resolve_path

log = get_emitter("ex.recorder")
FIXTURE_ROOT = resolve_path("fixture")

async def run_scenario(scenario_name: str, model: str, expect_error: bool = False, **kwargs):
    log.info(f"\n--- 🎬 Starting Scenario: {scenario_name} ---")
    try:
        trace_id = f"demo_trace_{scenario_name.lower().replace(' ', '_')}"
        log.info(f"Payload: model={model}, kwargs={kwargs}")
        
        tracer = DebugTracer()
        response = await acompletion(
            model=model,
            trace_id=trace_id,
            interceptors=[tracer],
            trace_errors=not expect_error, 
            **kwargs
        )

        if kwargs.get("stream"):
            log.info("🌊 Receiving stream...")
            async for chunk in response:
                text = StateTraverser.resolve(chunk, "choices.0.delta.content", "")
                if text:
                    print(text, end="", flush=True)
            print()
        else:
            content = StateTraverser.resolve(response, "choices.0.message.content", "")
            log.info(f"⚡ Singular response: {content[:50]}...")
            
    except Exception as e:
        if expect_error:
            log.warning(f"🛡️ [Expected Exception Captured] VCR correctly recorded network rupture: {type(e).__name__} - {e}")
        else:
            log.error(f"🚨 [UNEXPECTED FATAL ERROR] {e}")
            raise
    finally:
        log.info(f"--- 🛑 Scenario: {scenario_name} finished ---\n")


async def execute_scenarios(model: str, config: VCRPlaybackConfig):
    log.info("🚀 Fiber VCR Utility Interactive Demonstration")
    log.info("===============================================")
    log.info(f"🔧 VCR Config: mode={config.mode.upper()}, speed={config.speed}, chaos={config.chaos_latency_ms}ms")
    VCRInjector.apply(config=config, fixture_dir=FIXTURE_ROOT)
    await run_scenario(
        "Singular Chat",
        model=model,
        messages=[{"role": "user", "content": "Say a very short hello."}],
        temperature=0.7
    )
    
    await run_scenario(
        "Stream Chat",
        model=model,
        messages=[{"role": "user", "content": "Count from 1 to 5 slowly."}],
        stream=True
    )
    
    await run_scenario(
        "Error Boundary",
        model="invalid/fake-model-name",
        expect_error=True,
        messages=[{"role": "user", "content": "This request must fail."}],
    )

    log.info("\n" + "=" * 80)
    if config.mode == "record":
        log.info("✅ [RECORD COMPLETE] Network I/O has been frozen into fixtures.")
        log.info("💡 Next Step: Try running the Replay mode with real-time speed and fake jitter!")
        log.info(f"👉 Command: python -m fiber.dev.ex.recorder -m {model} --vcr replay --vcr-speed real --vcr-chaos 1500")
    elif config.mode == "replay":
        log.info("✅ [REPLAY COMPLETE] You just experienced time-travel API mocking.")
        log.info("👀 Look at the DebugTracer logs above! The latency should exactly match the recorded time (plus any chaos jitter).")
        if config.chaos_latency_ms > 0:
            log.info(f"🌪️ Did you notice the {config.chaos_latency_ms}ms artificial delay before the response?")
    else:
        log.info("✅ [LIVE COMPLETE] Normal API execution without VCR.")
    log.info("=" * 80 + "\n")


def main(args: list[str] = None):
    parser = argparse.ArgumentParser(description="Fiber VCR Utility Interactive Demo")
    parser.add_argument("-m", "--model", type=str, default="gemini/gemini-3.1-flash-lite", help="Target LLM model to use.")
    parser.add_argument("--vcr", type=str, choices=["live", "record", "replay"], default="record", help="VCR mode for API Mocking (Default: record)")
    parser.add_argument("--vcr-speed", type=str, choices=["max", "real"], default="max", help="Replay speed control (max or real-time)")
    parser.add_argument("--vcr-chaos", type=float, default=0.0, help="Inject artificial latency jitter (in ms) during replay")
    
    parsed_args, _ = parser.parse_known_args(args)
    
    config = VCRPlaybackConfig(
        mode=parsed_args.vcr, 
        speed=parsed_args.vcr_speed,
        chaos_latency_ms=parsed_args.vcr_chaos
    )
    
    asyncio.run(execute_scenarios(parsed_args.model, config))


if __name__ == "__main__":
    main()