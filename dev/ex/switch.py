# fiber.dev.ex.switch
import os
import asyncio
import time
import sys
import json

"""[Runtime Switch] Environment-based module aliasing Integration"""
VCR_MODE = os.environ.get("VCR_MODE", "live").lower()
FIXTURE_DIR = "./fixtures"


def _init_bridge(mode: str, fixture_dir: str):
    """Initializes the VCR sandbox, environment fencing, and direct sys.modules aliasing."""
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
    
    sys.modules["litellm"] = litellm_entry                # Proxy functions
    sys.modules["litellm.types.utils"] = fiber_param       # Proxy objects

    from fiber.dev.trace.llm.vcr import VCRInjector, VCRPlaybackConfig
    config = VCRPlaybackConfig(mode=mode, speed="real")
    VCRInjector.apply(config=config, fixture_dir=fixture_dir)
    
    print(f" 🔌 [Integration] Status: ENGAGED ({mode.upper()})")
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

"""[Legacy Business Logic] modification boundary"""
import litellm
from litellm.types.utils import ModelResponseStream

# Import Fiber Mapper/Traverser utilities for legacy migration demo
from fiber.gateway.llm.mapper.traverser import StateTraverseRule

async def analyze_and_extract_stream(scenario_id: str, prompt: str):
    trace_id = f"fixture_obj_{scenario_id}"
    
    print(f"▶️ [BUSINESS LOGIC] Initiating LLM Call")
    print(f"   ├─ Scenario: {scenario_id}")
    print(f"   ├─ Model: gemini/gemini-3.1-flash-lite")
    print(f"   └─ Prompt: {prompt}\n")
    
    try:
        start_time = time.perf_counter()
        
        response = await litellm.acompletion(
            model="gemini/gemini-3.1-flash-lite",
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            temperature=0.7,
            metadata={"trace_id": trace_id}
        )

        print(f"📡 [STREAM OPENED] Awaiting chunks...\n")
        print("🤖 [AI RESPONSE] \n" + "-"*40 + "\n")
        
        chunk_count = 0
        legacy_valid_count = 0
        fiber_valid_count = 0
        mismatch_count = 0
        
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
                choices = getattr(chunk, "choices", None) or (chunk.get("choices", []) if isinstance(chunk, dict) else [])
                if choices and len(choices) > 0:
                    first_choice = choices[0]
                    delta = getattr(first_choice, "delta", None) or (first_choice.get("delta", {}) if isinstance(first_choice, dict) else {})
                    legacy_content = getattr(delta, "content", None) or (delta.get("content", "") if isinstance(delta, dict) else "")
                    
                    if legacy_content:
                        legacy_valid_count += 1
            except Exception:
                pass  # Legacy silently ignores errors
                
            # [Method B] Fiber Parsing: Declarative traversal with multi-topology support
            try:
                # Flawlessly extracts content in one line, regardless of object/dict or OpenAI/Gemini schema
                fiber_content = StateTraverseRule.extract_stream_content(chunk, default="")
                
                if fiber_content:
                    fiber_valid_count += 1
            except Exception:
                fiber_content = ""
                
            # Validation & Output
            if legacy_content != fiber_content:
                mismatch_count += 1
                
            # Output text extracted using the Fiber method to the screen (identical output assumed)
            if fiber_content:
                print(fiber_content, end="", flush=True)

        duration = (time.perf_counter() - start_time) * 1000
        print("\n\n" + "-"*40)
        print(f"\n✅ [STREAM CLOSED] Duration: {duration:.2f}ms")
        print(f"   ├─ Total Chunks Received : {chunk_count}")
        print(f"   ├─ Legacy Extracted      : {legacy_valid_count}")
        print(f"   ├─ Fiber Mapper Extracted: {fiber_valid_count}")
        print(f"   └─ Data Mismatch Count   : {mismatch_count}\n")
        return trace_id
    except Exception as e:
        print(f"\n🚨 [FATAL ERROR] Execution Fault: {type(e).__name__} - {e}\n")
        return None

def inspect_fixture(trace_id: str):
    """Parses and visualizes the generated VCR fixture file."""
    filepath = os.path.join(FIXTURE_DIR, f"fixture_{trace_id}.json")
    if not os.path.exists(filepath):
        return

    print("=" * 80)
    print(f"💾 [VCR FIXTURE INSPECTOR]")
    print(f"   ├─ Path: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    metrics = data.get("network_metrics", {})
    print(f"⏱️  [METRICS] TTFB: {metrics.get('ttfb_ms', 0):.2f}ms | Total Duration: {metrics.get('total_duration_ms', 0):.2f}ms")
    print("=" * 80 + "\n")


async def main():
    prompt = (
        "As a senior software architect, explain why relying on a temporary 'Zero-Code "
        "Integration' (like Python module aliasing or monkey-patching) is dangerous as a "
        "long-term production solution. Provide a 3-step action plan for safely migrating "
        "to a native SDK implementation. Use bullet points."
    )
    
    trace_id = await analyze_and_extract_stream("tech_debt_migration", prompt)
    if VCR_MODE == "record" and trace_id:
        inspect_fixture(trace_id)

if __name__ == "__main__":
    asyncio.run(main())