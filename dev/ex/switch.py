# fiber.dev.ex.switch
import os
import asyncio
import time
import sys
import json

"""[Runtime Switch] Environment-based module aliasing Integration (Drop-in Block)"""
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()
FIXTURE_DIR = "./fixtures"

def _init_bridge(mode: str, fixture_dir: str):
    """Initializes the VCR sandbox and direct sys.modules aliasing"""
    from fiber.phase.cli.sandbox import verify_local_dev_environment, create_security_sandbox
    
    try:
        verify_local_dev_environment(allow_ci=True)
        create_security_sandbox(vcr_mode=mode)
        print(" 🔒 [Sandbox] Strict Security Policy INJECTED")
    except Exception as e:
        print(f"\n🚨 [SANDBOX VIOLATION] Execution Denied: {e}")
        sys.exit(1)

    import fiber.llm.entry as litellm_entry
    import fiber.llm.param as fiber_param
    
    sys.modules["litellm"] = litellm_entry
    sys.modules["litellm.types.utils"] = fiber_param

    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    config = VCRPlaybackConfig(mode=mode, speed="real", record_tick_ms=100.0)
    VCRInjector.apply(config=config, fixture_dir=fixture_dir)
    
    print(f" 🔌 [Integration] Status: ENGAGED ({mode.upper()}) | Tick: 100.0ms")
    print(f" 🔄 [Integration] Aliased 'litellm' -> 'fiber.llm.entry'")
    print(f" 📦 [Integration] Aliased 'litellm.types.utils' -> 'fiber.llm.param'")

"""Boot Sequence"""
print("=" * 80)
print("🛡️  FIBER INTEGRATION & SANDBOX")
print("=" * 80)

if VCR_MODE in ("record", "replay"):
    _init_bridge(mode=VCR_MODE, fixture_dir=FIXTURE_DIR)
else:
    print(f" 🟢 [Integration] Status: BYPASSED (Live Mode)")

print("-" * 80 + "\n")


# ==============================================================================
# """[Legacy Business Logic] modification boundary"""
# 이 아래로는 사용자의 기존 비즈니스 로직입니다. xphi/fiber 패키지에 직접 의존하지 않습니다.
# ==============================================================================
import litellm
from litellm.types.utils import ModelResponseStream

async def analyze_and_extract_stream(scenario_id: str, prompt: str):
    print(f"▶️ [BUSINESS LOGIC] Initiating LLM Call")
    print(f"   ├─ Scenario: {scenario_id}")
    print(f"   ├─ Model: gemini/gemini-3.1-flash-lite")
    print(f"   └─ Prompt: {prompt[:50]}...\n")
    
    try:
        start_time = time.perf_counter()
        
        # 💡 [핵심 개선] 프레임워크 전용 ID 생성 함수를 제거하고, 표준 LiteLLM 스펙인 metadata만 활용.
        # 하단에 숨어있는 Fiber VCR 엔진이 이 메타데이터를 낚아채어 파일명과 Trace ID를 완벽히 통제합니다.
        response = await litellm.acompletion(
            model="gemini/gemini-3.1-flash-lite",
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
    # 💡 [핵심] 인스펙터용 유틸리티도 함수 내부에서만 임포트하여 오염을 막습니다.
    from fiber.dev.trace.llm.vcr.manager import VCRIdentityRule
    
    class MockCtx:
        system_meta = type('Meta', (), {'metadata': {'vcr_scenario': scenario_id, 'vcr_invoker': "ex.switch"}})()
        
    # Trace ID를 몰라도 메타데이터(시나리오명)만으로 올바른 파일명을 찾아내는 VCR 룰 활용
    filename = VCRIdentityRule.get_fixture_filename("unknown", MockCtx())
    filepath = os.path.join(FIXTURE_DIR, filename)
    
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
    prompt = (
        "As a senior software architect, explain why relying on a temporary 'Zero-Code "
        "Integration' (like Python module aliasing or monkey-patching) is dangerous as a "
        "long-term production solution. Provide a 3-step action plan for safely migrating "
        "to a native SDK implementation. Use bullet points."
    )
    
    scenario_name = "tech_debt_migration"
    returned_scenario = await analyze_and_extract_stream(scenario_name, prompt)
    
    if VCR_MODE == "record" and returned_scenario:
        inspect_fixture(returned_scenario.replace(" ", "_").lower())

if __name__ == "__main__":
    asyncio.run(main())