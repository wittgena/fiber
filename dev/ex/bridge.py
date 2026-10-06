# fiber.dev.ex.bridge
import os
import sys

VCR_MODE = os.environ.get("VCR_MODE", "live").lower()

def _init_bridge(mode: str, fixture_dir: str):
    """Init the VCR sandbox and direct modules aliasing"""
    from fiber.phase.cli.sandbox import verify_local_dev_environment, create_security_sandbox
    from fiber.dev.ex.space.bind.redirector import PhaseAirlock
    
    try:
        verify_local_dev_environment(allow_ci=True)
        create_security_sandbox(vcr_mode=mode)
        print("[Sandbox] Strict Security Policy INJECTED")
    except Exception as e:
        print(f"\n[SANDBOX VIOLATION] Execution Denied: {e}")
        sys.exit(1)

    import fiber.llm.entry as llm_entry
    import fiber.llm.response as llm_response
    
    PhaseAirlock.alias({
        "litellm": llm_entry.__name__,
        "litellm.types.utils": llm_response.__name__
    })

    from fiber.dev.trace.llm.vcr.manager import VCRPlaybackConfig
    from fiber.dev.trace.llm.vcr.proxy import VCRInjector
    config = VCRPlaybackConfig(mode=mode, speed="real", record_tick_ms=100.0)
    VCRInjector.apply(config=config, fixture_dir=fixture_dir)
    
    print(f" [Integration] Status: ENGAGED ({mode.upper()}) | Tick: 100.0ms")
    print(f" [Integration] Aliased 'litellm' -> 'fiber.llm.entry'")
    print(f" [Integration] Aliased 'litellm.types.utils' -> 'fiber.llm.response'")

def inspect_fixture(scenario_id: str, fixture_dir: str):
    """Parses and visualizes the generated VCR fixture file (Reusable Utility)"""
    if not fixture_dir:
        print("[VCR] Fixture directory was not initialized.")
        return

    from fiber.dev.trace.llm.vcr.manager import VCRIdentityRule
    
    class MockCtx:
        system_meta = type('Meta', (), {'metadata': {'vcr_scenario': scenario_id, 'vcr_invoker': "ex.switch"}})()
        
    filename = VCRIdentityRule.get_fixture_filename("unknown", MockCtx())
    filepath = os.path.join(fixture_dir, filename)
    
    if not os.path.exists(filepath):
        print(f"[VCR] Fixture file not found at: {filepath}")
        return

    print("=" * 80)
    print(f"[VCR FIXTURE INSPECTOR]")
    print(f"   ├─ Path: {filepath}")
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        
    latest_trace_id = data.get("latest_trace_id")
    target_fixture = data.get("traces", {}).get(latest_trace_id, {})
    metrics = target_fixture.get("network_metrics", {})
    
    print(f" [METRICS] TTFB: {metrics.get('ttfb_ms', 0):.2f}ms | Total Duration: {metrics.get('total_duration_ms', 0):.2f}ms")
    print("=" * 80 + "\n")


def vcr_setup_sequence():
    print("=" * 80)
    print(" FIBER INTEGRATION & SANDBOX")
    print("=" * 80)

    resolved_fixture_dir = None
    if VCR_MODE in ("record", "replay"):
        from xphi.kernel.space.bind.resolver import resolve_path
        resolved_fixture_dir = str(resolve_path("abc") / "fixture")
        _init_bridge(mode=VCR_MODE, fixture_dir=resolved_fixture_dir)
    else:
        print(f" [Integration] Status: BYPASSED (Live Mode)")

    print("-" * 80 + "\n")
    return VCR_MODE, resolved_fixture_dir