# fiber.dev.ex.switch
import os
import asyncio
import time
import sys
import json
import argparse

"""[Runtime Switch] Environment-based module aliasing Integration (Drop-in Block)"""
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()

def _init_bridge(mode: str, fixture_dir: str):
    """Initializes the VCR sandbox and direct modules aliasing"""
    from fiber.phase.cli.sandbox import verify_local_dev_environment, create_security_sandbox
    from fiber.dev.ex.space.bind.redirector import PhaseAirlock
    
    try:
        verify_local_dev_environment(allow_ci=True)
        create_security_sandbox(vcr_mode=mode)
        print(" 🔒 [Sandbox] Strict Security Policy INJECTED")
    except Exception as e:
        print(f"\n🚨 [SANDBOX VIOLATION] Execution Denied: {e}")
        sys.exit(1)

    import fiber.llm.entry as llm_entry
    import fiber.llm.response as llm_reponse
    
    PhaseAirlock.alias({
        "litellm": llm_entry.__name__,
        "litellm.types.utils": llm_reponse.__name__
    })


    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    config = VCRPlaybackConfig(mode=mode, speed="real", record_tick_ms=100.0)
    VCRInjector.apply(config=config, fixture_dir=fixture_dir)
    
    print(f" 🔌 [Integration] Status: ENGAGED ({mode.upper()}) | Tick: 100.0ms")
    print(f" 🔄 [Integration] Aliased 'litellm' -> 'fiber.llm.entry'")
    print(f" 📦 [Integration] Aliased 'litellm.types.utils' -> 'fiber.llm.response'")

"""Boot Sequence"""
print("=" * 80)
print("🛡️  FIBER INTEGRATION & SANDBOX")
print("=" * 80)

_resolved_fixture_dir = None

if VCR_MODE in ("record", "replay"):
    from xphi.kernel.space.bind.resolver import resolve_path
    _resolved_fixture_dir = str(resolve_path("abc") / "fixture")
    _init_bridge(mode=VCR_MODE, fixture_dir=_resolved_fixture_dir)
else:
    print(f" 🟢 [Integration] Status: BYPASSED (Live Mode)")

print("-" * 80 + "\n")


# ==============================================================================
# [Legacy Business Logic] modification boundary
# ==============================================================================
import litellm
from litellm.types.utils import ModelResponseStream

# ✨ model 매개변수 추가 (기본값 설정으로 하위 호환성 유지)
async def analyze_and_extract_stream(scenario_id: str, prompt: str, model_name: str = "gemini/gemini-3.1-flash-lite"):
    print(f"▶️ [BUSINESS LOGIC] Initiating LLM Call")
    print(f"   ├─ Scenario: {scenario_id}")
    print(f"   ├─ Model: {model_name}") # ✨ 동적 모델명 출력
    print(f"   └─ Prompt: {prompt[:50]}...\n")
    
    try:
        start_time = time.perf_counter()
        response = await litellm.acompletion(
            model=model_name, # ✨ 동적 모델 주입
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            temperature=0.7,
            metadata={
                "vcr_scenario": scenario_id.replace(" ", "_").lower(),
                "vcr_invoker": "ex.switch"
            }
        )

        print(f"📡 [STREAM OPENED] Awaiting chunks...\n")
        print("🤖 [AI RESPONSE] \n" + "-"*40 + "\n")
        
        chunk_count = 0
        legacy_valid_count = 0
        async for chunk in response:
            chunk_count += 1
            
            if chunk_count == 1:
                print(f"[🔍 DUCK-TYPING INSPECTOR - First Chunk]")
                print(f"   ├─ Actual Type  : {type(chunk)}")
                is_compatible = isinstance(chunk, ModelResponseStream)
                print(f"   ├─ isinstance() : {'✅ PASS' if is_compatible else '❌ FAIL'}\n")
                print("-" * 40 + "\n")
            
            # [Method A] Legacy Parsing: Manual, hardcoded defensive approach
            legacy_content = ""
            try:
                if hasattr(chunk, "choices") and chunk.choices:
                    legacy_content = chunk.choices[0].delta.content or ""
                elif isinstance(chunk, dict) and "choices" in chunk:
                    legacy_content = chunk["choices"][0].get("delta", {}).get("content", "")
                    
                if legacy_content:
                    legacy_valid_count += 1
            except Exception:
                pass  
                
            if legacy_content:
                print(legacy_content, end="", flush=True)

        duration = (time.perf_counter() - start_time) * 1000
        print("\n\n" + "-"*40)
        print(f"\n✅ [STREAM CLOSED] Duration: {duration:.2f}ms")
        print(f"   ├─ Total Chunks Received : {chunk_count}")
        print(f"   └─ Legacy Extracted      : {legacy_valid_count}\n")
        
        return scenario_id
    except Exception as e:
        print(f"\n🚨 [FATAL ERROR] Execution Fault: {type(e).__name__} - {e}\n")
        return None

def inspect_fixture(scenario_id: str):
    """Parses and visualizes the generated VCR fixture file (Local scoped tools)"""
    if not _resolved_fixture_dir:
        print("⚠️ [VCR] Fixture directory was not initialized.")
        return

    from fiber.dev.trace.llm.vcr.manager import VCRIdentityRule
    
    class MockCtx:
        system_meta = type('Meta', (), {'metadata': {'vcr_scenario': scenario_id, 'vcr_invoker': "ex.switch"}})()
        
    # Trace ID를 몰라도 메타데이터(시나리오명)만으로 올바른 파일명을 찾아내는 VCR 룰 활용
    filename = VCRIdentityRule.get_fixture_filename("unknown", MockCtx())
    filepath = os.path.join(_resolved_fixture_dir, filename) # 동적 경로 사용
    
    if not os.path.exists(filepath):
        print(f"⚠️ [VCR] Fixture file not found at: {filepath}")
        return

    print("=" * 80)
    print(f"💾 [VCR FIXTURE INSPECTOR]")
    print(f"   ├─ Path: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    latest_trace_id = data.get("latest_trace_id")
    target_fixture = data.get("traces", {}).get(latest_trace_id, {})
    metrics = target_fixture.get("network_metrics", {})
    
    print(f"⏱️  [METRICS] TTFB: {metrics.get('ttfb_ms', 0):.2f}ms | Total Duration: {metrics.get('total_duration_ms', 0):.2f}ms")
    print("=" * 80 + "\n")


async def main():
    parser = argparse.ArgumentParser(description="Runtime Switch VCR Test Suite")
    parser.add_argument(
        "-m", "--model", 
        type=str, 
        default="gemini/gemini-3.1-flash-lite", 
        help="Target LLM model to execute (e.g., ollama/gemma:2b)"
    )
    args = parser.parse_args()

    prompt = (
        "As a senior software architect, explain why relying on a temporary 'Zero-Code "
        "Integration' (like Python module aliasing or monkey-patching) is dangerous as a "
        "long-term production solution. Provide a 3-step action plan for safely migrating "
        "to a native SDK implementation. Use bullet points."
    )
    
    scenario_name = "tech_debt_migration"
    returned_scenario = await analyze_and_extract_stream(scenario_name, prompt, model_name=args.model)
    if VCR_MODE == "record" and returned_scenario:
        inspect_fixture(returned_scenario.replace(" ", "_").lower())

if __name__ == "__main__":
    asyncio.run(main())