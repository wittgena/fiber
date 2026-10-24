# fiber.dev.ex.tester.gateway
import argparse
import asyncio
import json
import os
import time
from typing import Any, Dict, List
import httpx

from fiber.infra.vcr.manager import VCRIdentityRule, add_vcr_arguments
from xphi.node.space.bind.resolver import resolve_path


def _resolve_expected_fixture_path(scenario: str, invoker: str = "rest_gateway") -> str:
    fixture_root = os.getenv("VCR_FIXTURE_DIR") or str(resolve_path("fixture"))
    invoker_prefix = VCRIdentityRule._get_flattened_invoker(invoker)
    clean_scenario = scenario.replace(" ", "_").lower()
    filename = f"fixture_{invoker_prefix}_{clean_scenario}.json"
    return os.path.abspath(os.path.join(fixture_root, filename))


def _inspect_fixture_on_disk(filepath: str) -> str:
    if not os.path.exists(filepath):
        return f"MISSING ({filepath})"
    try:
        size_kb = os.path.getsize(filepath) / 1024.0
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        latest_tid = data.get("latest_trace_id", "N/A")
        trace_count = len(data.get("traces", {}))
        return f"FOUND ({size_kb:.1f} KB | traces={trace_count} | latest_trace={latest_tid}) -> {filepath}"
    except Exception as e:
        return f"ERROR reading {filepath}: {e}"


async def execute_call(
    client: httpx.AsyncClient,
    url: str,
    step_name: str,
    model: str,
    prompt: str,
    stream: bool,
    vcr_mode: str,
    scenario: str,
    vcr_speed: str = "max",
    vcr_chaos: float = 0.0,
    vcr_tick: float = 100.0,
    show_raw: bool = False,
) -> Dict[str, Any]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
        "vcr": vcr_mode,
        "vcr_scenario": scenario,
        "vcr_speed": vcr_speed,
        "vcr_chaos": vcr_chaos,
        "vcr_tick": vcr_tick,
    }

    expected_fixture_path = _resolve_expected_fixture_path(scenario)

    print(f"\n================================================================================")
    print(f"▶ [{step_name}] mode={vcr_mode.upper()} | stream={stream} | scenario='{scenario}'")
    print(f"  ├─ Prompt      : \"{prompt}\"")
    print(f"  └─ Req Payload : {json.dumps(payload, ensure_ascii=False)}")
    print(f"--------------------------------------------------------------------------------")

    t0 = time.perf_counter()
    status_code = 0
    resp_id = ""
    full_text = ""
    chunk_count = 0
    error_detail = None
    usage_info = None
    sys_meta = None
    raw_chunks: List[Dict[str, Any]] = []
    full_json_resp: Dict[str, Any] = {}

    try:
        if stream:
            async with client.stream("POST", url, json=payload) as resp:
                status_code = resp.status_code
                if status_code >= 400:
                    err_raw = (await resp.aread()).decode("utf-8", errors="replace")
                    error_detail = err_raw
                    print(f"[Error {status_code}] {err_raw}")
                else:
                    print("[Stream] ", end="", flush=True)
                    async for line in resp.aiter_lines():
                        if not line or not line.startswith("data: "):
                            continue
                        data_str = line[6:].strip()
                        if data_str == "[DONE]":
                            break
                        chunk = json.loads(data_str)
                        raw_chunks.append(chunk)
                        if not resp_id and chunk.get("id"):
                            resp_id = chunk["id"]
                        if chunk.get("usage"):
                            usage_info = chunk["usage"]
                        if chunk.get("system_meta"):
                            sys_meta = chunk["system_meta"]
                        choices = chunk.get("choices") or []
                        if choices:
                            delta_text = choices[0].get("delta", {}).get("content") or ""
                            if delta_text:
                                chunk_count += 1
                                full_text += delta_text
                                print(delta_text, end="", flush=True)
                    print()
                    if show_raw and raw_chunks:
                        print(f"\n[Raw Stream First Chunk]\n{json.dumps(raw_chunks[0], indent=2, ensure_ascii=False)}")
                        print(f"[Raw Stream Last Chunk]\n{json.dumps(raw_chunks[-1], indent=2, ensure_ascii=False)}")
        else:
            resp = await client.post(url, json=payload)
            status_code = resp.status_code
            full_json_resp = resp.json()
            if status_code >= 400:
                error_detail = json.dumps(full_json_resp, ensure_ascii=False)
                print(f"[Error {status_code}] {error_detail}")
            else:
                resp_id = full_json_resp.get("id", "")
                usage_info = full_json_resp.get("usage")
                sys_meta = full_json_resp.get("system_meta")
                choices = full_json_resp.get("choices") or []
                if choices:
                    full_text = choices[0].get("message", {}).get("content") or ""
                print(f"[Response] {full_text[:200]}{'...' if len(full_text) > 200 else ''}")
                if show_raw:
                    print(f"\n[Raw Full JSON Response]\n{json.dumps(full_json_resp, indent=2, ensure_ascii=False)}")
    except Exception as e:
        error_detail = str(e)
        print(f"[Exception] {e}")

    elapsed_ms = (time.perf_counter() - t0) * 1000
    is_vcr_hit = str(resp_id).startswith("vcr-")

    trace_id = (sys_meta or {}).get("trace_id") or "N/A"
    meta_inner = (sys_meta or {}).get("metadata") or {}

    print(f"--------------------------------------------------------------------------------")
    print(
        f"✔ Status: {status_code} | ID: {resp_id or 'N/A'} "
        f"({'VCR_REPLAY' if is_vcr_hit else 'LIVE_MODEL'}) | "
        f"Elapsed: {elapsed_ms:.1f}ms | Length: {len(full_text)} chars"
    )
    print(f"  ├─ Trace ID    : {trace_id}")
    print(f"  ├─ Usage       : {json.dumps(usage_info, ensure_ascii=False)}")
    print(f"  ├─ System Meta : vcr_mode={meta_inner.get('vcr_mode', 'live')} | scenario={meta_inner.get('vcr_scenario')} | invoker={meta_inner.get('vcr_invoker')}")
    if vcr_mode in ("record", "replay"):
        print(f"  └─ Fixture File: {_inspect_fixture_on_disk(expected_fixture_path)}")

    return {
        "step": step_name,
        "mode": vcr_mode,
        "stream": stream,
        "status": status_code,
        "id": resp_id,
        "trace_id": trace_id,
        "is_vcr_hit": is_vcr_hit,
        "elapsed_ms": elapsed_ms,
        "text": full_text,
        "chunks": chunk_count,
        "fixture_path": expected_fixture_path if vcr_mode in ("record", "replay") else None,
        "error": error_detail,
    }


def print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n================================================================================")
    print("                           GATEWAY TEST SUITE SUMMARY                           ")
    print("================================================================================")
    print(f"{'Step':<26} | {'Mode':<6} | {'Stream':<6} | {'HTTP':<4} | {'Source':<10} | {'Time (ms)':<10}")
    print("-" * 80)
    for r in results:
        src = "VCR_CACHE" if r["is_vcr_hit"] else "LIVE_LLM"
        print(
            f"{r['step']:<26} | {r['mode']:<6} | {str(r['stream']):<6} | "
            f"{r['status']:<4} | {src:<10} | {r['elapsed_ms']:>8.1f}ms"
        )
    print("================================================================================")
    for r in results:
        if r.get("fixture_path"):
            print(f"💾 [{r['mode'].upper()}] {r['step']} -> {r['fixture_path']}")
    print()


async def main():
    parser = argparse.ArgumentParser(description="Fiber REST Gateway Multi-Step Tester")
    parser.add_argument("-u", "--url", default="http://127.0.0.1:8356/v1/chat/completions")
    parser.add_argument("-m", "--model", default="llama_server/gemma-3-1b-it-Q4_K_M.gguf")
    parser.add_argument("-p", "--prompt", default=None, help="Run a single custom prompt instead of the 3-step suite")
    parser.add_argument("-s", "--stream", action="store_true", help="Enable streaming (for single prompt mode)")
    parser.add_argument("--scenario", default="gw_suite", help="Base VCR scenario prefix")
    parser.add_argument("--raw", action="store_true", help="Dump full raw JSON / stream chunks")
    add_vcr_arguments(parser)
    args = parser.parse_args()

    results: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        if args.prompt is not None:
            res = await execute_call(
                client=client,
                url=args.url,
                step_name="Single Custom Call",
                model=args.model,
                prompt=args.prompt,
                stream=args.stream,
                vcr_mode=args.vcr,
                scenario=args.scenario,
                vcr_speed=args.vcr_speed,
                vcr_chaos=args.vcr_chaos,
                vcr_tick=args.vcr_tick,
                show_raw=args.raw,
            )
            results.append(res)
        else:
            # [Step 1] Unary
            results.append(
                await execute_call(
                    client=client,
                    url=args.url,
                    step_name="1. Unary Completion",
                    model=args.model,
                    prompt="Reply with exactly one short sentence defining an API gateway.",
                    stream=False,
                    vcr_mode=args.vcr,
                    scenario=f"{args.scenario}_unary",
                    vcr_speed=args.vcr_speed,
                    vcr_chaos=args.vcr_chaos,
                    vcr_tick=args.vcr_tick,
                    show_raw=args.raw,
                )
            )

            # [Step 2] SSE Streaming
            results.append(
                await execute_call(
                    client=client,
                    url=args.url,
                    step_name="2. SSE Stream Completion",
                    model=args.model,
                    prompt="Count from 1 to 5 separated by commas.",
                    stream=True,
                    vcr_mode=args.vcr,
                    scenario=f"{args.scenario}_stream",
                    vcr_speed=args.vcr_speed,
                    vcr_chaos=args.vcr_chaos,
                    vcr_tick=args.vcr_tick,
                    show_raw=args.raw,
                )
            )

            # [Step 3] Dynamic VCR Record -> Replay
            roundtrip_prompt = "Give me a 3-word motto for deterministic testing."
            roundtrip_scenario = f"{args.scenario}_roundtrip"

            if args.vcr == "replay":
                results.append(
                    await execute_call(
                        client=client,
                        url=args.url,
                        step_name="3. VCR Replay Check",
                        model=args.model,
                        prompt=roundtrip_prompt,
                        stream=False,
                        vcr_mode="replay",
                        scenario=roundtrip_scenario,
                        vcr_speed=args.vcr_speed,
                        vcr_chaos=args.vcr_chaos,
                        show_raw=args.raw,
                    )
                )
            else:
                rec_res = await execute_call(
                    client=client,
                    url=args.url,
                    step_name="3A. Dynamic VCR [RECORD]",
                    model=args.model,
                    prompt=roundtrip_prompt,
                    stream=False,
                    vcr_mode="record",
                    scenario=roundtrip_scenario,
                    vcr_tick=args.vcr_tick,
                    show_raw=args.raw,
                )
                results.append(rec_res)

                rep_res = await execute_call(
                    client=client,
                    url=args.url,
                    step_name="3B. Dynamic VCR [REPLAY]",
                    model=args.model,
                    prompt=roundtrip_prompt,
                    stream=False,
                    vcr_mode="replay",
                    scenario=roundtrip_scenario,
                    vcr_speed="max",
                    show_raw=args.raw,
                )
                results.append(rep_res)

                if rec_res["status"] == 200 and rep_res["status"] == 200:
                    match = rec_res["text"].strip() == rep_res["text"].strip()
                    print(f"\n🔍 [VCR Roundtrip Parity] Content Match: {'PASS ✅' if match else 'MISMATCH ❌'}")

    print_summary(results)


if __name__ == "__main__":
    asyncio.run(main())