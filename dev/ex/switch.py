# fiber.dev.ex.switch
import os
import asyncio
import time
import sys
import json
import argparse

# vcr injection
from fiber.dev.ex.bridge import vcr_setup_sequence, inspect_fixture
VCR_MODE, RESOLVED_FIXTURE_DIR = vcr_setup_sequence()

"""[Legacy Business Logic] modification boundary"""
import litellm
from litellm.types.utils import ModelResponseStream

async def analyze_and_extract_stream(scenario_id: str, prompt: str, model_name: str = "gemini/gemini-3.1-flash-lite"):
    print(f"[BUSINESS LOGIC] Initiating LLM Call")
    print(f"   ├─ Scenario: {scenario_id}")
    print(f"   ├─ Model: {model_name}")
    print(f"   └─ Prompt: {prompt[:50]}...\n")
    
    try:
        start_time = time.perf_counter()
        response = await litellm.acompletion(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
            temperature=0.7,
            metadata={
                "vcr_scenario": scenario_id.replace(" ", "_").lower(),
                "vcr_invoker": "ex.switch"
            }
        )

        print(f"[STREAM OPENED] Awaiting chunks...\n")
        print("[AI RESPONSE] \n" + "-"*40 + "\n")
        
        chunk_count = 0
        legacy_valid_count = 0
        async for chunk in response:
            chunk_count += 1
            
            if chunk_count == 1:
                print(f"[DUCK-TYPING INSPECTOR - First Chunk]")
                print(f"   ├─ Actual Type  : {type(chunk)}")
                is_compatible = isinstance(chunk, ModelResponseStream)
                print(f"   ├─ isinstance() : {'PASS' if is_compatible else 'FAIL'}\n")
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
        print(f"\n[STREAM CLOSED] Duration: {duration:.2f}ms")
        print(f"   ├─ Total Chunks Received : {chunk_count}")
        print(f"   └─ Legacy Extracted      : {legacy_valid_count}\n")
        return scenario_id
    except Exception as e:
        print(f"\n[FATAL ERROR] Execution Fault: {type(e).__name__} - {e}\n")
        return None

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