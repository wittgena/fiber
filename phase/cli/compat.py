# fiber.phase.cli.compat
import os
import sys
import asyncio
import time
from typing import Optional

from fiber.llm.model.provider.resolver import get_llm_provider
import fiber.llm.entry as llm_entry 

from fiber.gateway.llm.state.traverser import StateMapper
from fiber.gateway.llm.stream.parser.chunk import StreamChunkParser
from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
from fiber.dev.trace.llm.vcr.proxy import VCRInjector

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("cli.compat")

STANDARD_PROMPTS = [
    {
        "scenario": "Stream Compat Traverser", 
        "stream": True, 
        "messages": [
            {
                "role": "user", 
                "content": "Briefly explain the role of a 'State Traverser' in an LLM gateway using one short sentence. Then, provide a minimal JSON example of a declarative extraction rule that maps the path 'choices.0.delta.content'."
            }
        ]
    },
    {
        "scenario": "Sync Declarative Rule", 
        "stream": False, 
        "messages": [
            {
                "role": "user", 
                "content": "In one concise sentence, what is the primary architectural benefit of using external declarative JSON rulesets instead of hardcoding Python parsing logic for new LLM providers?"
            }
        ]
    }
]

def _resolve_target_dir(model: str, target_dir: Optional[str]) -> tuple[str, str, str]:
    _, resolved_provider, dynamic_api_key, _ = get_llm_provider(model=model)
    safe_model_name = model.replace("/", "_").replace(":", "_")
    
    if target_dir:
        target_dir = target_dir
    else:
        target_dir = os.path.join(os.getcwd(), "fiber-compats", resolved_provider, safe_model_name)
        
    return target_dir, resolved_provider, dynamic_api_key

def _init_vcr(mode: str, target_dir: str, include_raw: bool = False):
    os.environ["VCR_MODE"] = mode
    if include_raw:
        os.environ["VCR_INCLUDE_RAW"] = "true"
        
    config = VCRPlaybackConfig(
        mode=mode, 
        speed="max", 
        chaos_latency_ms=0.0, 
        record_tick_ms=100.0,
        include_raw_payload=include_raw
    )
    VCRInjector.apply(config=config, fixture_dir=target_dir)
    log.info(f"[Compat: VCR] Injector applied. Mode: {mode.upper()}, Dir: {target_dir}")

async def _execute_suite(model: str, final_api_key: str, invoker_mode: str, provider: str) -> int:
    success_count = 0
    for test in STANDARD_PROMPTS:
        scenario_id = test["scenario"].replace(" ", "_").lower()
        is_stream = test["stream"]
        
        print(f"\n{'='*80}")
        print(f"[COMPAT: {invoker_mode.upper()}] Executing Test: {test['scenario']}")
        print(f"   ├─ Model    : {model}")
        print(f"   ├─ Provider : {provider} (Rule targeting)")
        print(f"   ├─ Stream   : {is_stream}")
        print(f"   └─ Prompt   : {test['messages'][0]['content'][:60]}...")
        print(f"{'-'*80}")
        
        try:
            kwargs = {"stream_options": {"include_usage": True}} if is_stream else {}
            if final_api_key:
                kwargs["api_key"] = final_api_key

            start_time = time.perf_counter()
            response = await llm_entry.acompletion(
                model=model,
                messages=test["messages"],
                stream=is_stream,
                metadata={
                    "vcr_scenario": scenario_id,
                    "vcr_invoker": f"cli.compat.{invoker_mode}",
                    "vcr_filename": f"{scenario_id}.json"
                },
                **kwargs
            )

            full_content = ""
            usage_info = "N/A"
            chunk_count = 0

            print(f"[NETWORK] Connection established. Receiving payload...\n")
            print("[AI RESPONSE]")
            print("-" * 40)

            if is_stream:
                async for chunk in response:
                    chunk_count += 1
                    parsed_chunk = StreamChunkParser.parse(provider, chunk)
                    if not parsed_chunk:
                        continue
                        
                    text = parsed_chunk.get("text", "")
                    if text:
                        full_content += text
                        sys.stdout.write(text)
                        sys.stdout.flush()
                    
                    chunk_usage = parsed_chunk.get("usage")
                    if chunk_usage:
                        has_real_value = any(v is not None for v in chunk_usage.values()) if isinstance(chunk_usage, dict) else True
                        if has_real_value:
                            usage_info = str(chunk_usage)
            else:
                content, usage_dict = StateMapper.extract_sync_response(response, provider)
                full_content = content or ""
                if usage_dict:
                    usage_info = str(usage_dict)
                    
                if full_content:
                    sys.stdout.write(full_content)
                    sys.stdout.flush()

            duration = (time.perf_counter() - start_time) * 1000
            print("\n\n" + "-" * 40)
            print("[VALIDATION PHASE]")
            print(f"full_content = {full_content}")
            if not full_content.strip():
                raise ValueError(f"Parsed content is empty! Missing or invalid extraction rule for provider: '{provider}'")

            print(f"   ├─ Extracted Content : OK ({len(full_content)} chars)")
            print(f"   ├─ Extracted Usage   : {usage_info}")
            if is_stream:
                print(f"   └─ Total Chunks      : {chunk_count}")
            
            log.info(f"[Compat: {invoker_mode.upper()}] Scenario Passed! ({duration:.2f}ms)")
            success_count += 1
            
        except Exception as e:
            print("\n" + "-" * 40)
            log.error(f"[Compat: {invoker_mode.upper()}] Failed: {type(e).__name__} - {e}")
            
    return success_count


async def run_gen_fixture(
    model: str,
    api_key: Optional[str] = None,
    target_dir: Optional[str] = None
):
    log.info(f"[Compat: GEN-FIXTURE] Resolving configuration for {model}...")

    try:
        target_dir, resolved_provider, dynamic_api_key = _resolve_target_dir(model, target_dir)
    except Exception as e:
        log.error(f"[Compat: GEN-FIXTURE] Invalid model identifier: {e}")
        sys.exit(1)
    
    LOCAL_PROVIDERS = ["lm_studio", "llama_server", "ollama", "vllm"]
    if api_key or dynamic_api_key:
        final_api_key = api_key or dynamic_api_key
    elif resolved_provider in LOCAL_PROVIDERS:
        final_api_key = "sk-mock-local-key"
    else:
        final_api_key = None

    os.makedirs(target_dir, exist_ok=True)
    _init_vcr(mode="record", target_dir=target_dir, include_raw=True)
    success_count = await _execute_suite(model, final_api_key, "gen_fixture", resolved_provider)

    print(f"\n{'='*80}")
    log.info(f"[COMPAT: FIXTURE GENERATION COMPLETE] ({success_count}/{len(STANDARD_PROMPTS)}) Fixtures saved at: {target_dir}")


async def run_test_fixture(model: str, target_dir: Optional[str] = None):
    log.info(f"[Compat: TEST-FIXTURE] Preparing Replay environment for {model}...")

    try:
        target_dir, resolved_provider, _ = _resolve_target_dir(model, target_dir)
    except Exception as e:
        log.error(f"[Compat: TEST-FIXTURE] Invalid model identifier: {e}")
        sys.exit(1)

    if not os.path.exists(target_dir):
        log.error(f"[Compat: TEST-FIXTURE] Fixture directory not found: {target_dir}")
        log.error("Run 'fiber compat fixture --vcr-mode record' first to generate required test data.")
        sys.exit(1)

    _init_vcr(mode="replay", target_dir=target_dir, include_raw=False)
    
    mock_api_key = "sk-vcr-replay-mock-key"
    success_count = await _execute_suite(model, mock_api_key, "test_fixture", resolved_provider)
    print(f"\n{'='*80}")
    if success_count == len(STANDARD_PROMPTS):
        log.info(f"[COMPAT: TEST SUCCESS] All rules and parsers verified successfully for {model}.")
    else:
        log.error(f"[COMPAT: TEST FAILED] ({success_count}/{len(STANDARD_PROMPTS)}) scenarios passed. Review extraction rules.")
        sys.exit(1)