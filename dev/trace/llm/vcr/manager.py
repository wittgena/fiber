# fiber.dev.trace.llm.vcr.manager
import os
import json
import hashlib
import glob
from dataclasses import dataclass
from typing import Optional, Any, Dict, List

from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.arch.contract.config import env
from xphi.watcher.plane.emitter import get_emitter

class VCRRequestContext(DynamicSurgeModel):
    model: str
    provider: str
    messages: List[Dict[str, Any]]
    parameters: Dict[str, Any]

class VCRNetworkMetrics(DynamicSurgeModel):
    ttfb_ms: float = 0.0
    total_duration_ms: float = 0.0

class VCRExceptionBoundary(DynamicSurgeModel):
    occurred: bool = False
    error_type: Optional[str] = None
    message: Optional[str] = None

class VCRFixtureTraceState(DynamicSurgeModel):
    trace_id: str
    request_context: VCRRequestContext
    network_metrics: VCRNetworkMetrics = VCRNetworkMetrics()
    response_timeline: List[Dict[str, Any]] = []
    usage: Optional[Dict[str, Any]] = None
    exception_boundary: VCRExceptionBoundary = VCRExceptionBoundary()
    raw_payload: Optional[Dict[str, Any]] = None

log = get_emitter("llm.vcr.manager")

@dataclass
class VCRPlaybackConfig:
    mode: str = "live"
    speed: str = "max"
    chaos_latency_ms: float = 0.0
    target_traces: str = ""
    record_tick_ms: float = 100.0
    include_raw_payload: bool = env.VCR_INCLUDE_RAW.lower() == "true"

class VCRIdentityRule:
    @staticmethod
    def _get_flattened_invoker(invoker_path: Optional[str]) -> str:
        if not invoker_path:
            return "unknown_invoker"
        return invoker_path.replace(".", "_")

    @staticmethod
    def generate_seed(
        scenario_name: Optional[str] = None, 
        messages: Optional[list] = None,
        invoker: Optional[str] = None
    ) -> str:
        invoker_prefix = VCRIdentityRule._get_flattened_invoker(invoker)
        if scenario_name:
            return f"vcr_seed_{invoker_prefix}_{scenario_name}"
            
        msg_str = json.dumps(messages, sort_keys=True) if messages else "empty"
        hashed_msg = hashlib.md5(msg_str.encode()).hexdigest()[:16]
        return f"vcr_seed_{invoker_prefix}_auto_{hashed_msg}"

    @staticmethod
    def get_fixture_filename(trace_id: str, ctx: Any) -> str:
        system_meta = getattr(ctx, "system_meta", None)
        
        scenario = None
        invoker = None
        if system_meta and system_meta.metadata:
            explicit_filename = system_meta.metadata.get("vcr_filename")
            if explicit_filename:
                return explicit_filename if explicit_filename.endswith(".json") else f"{explicit_filename}.json"
            scenario = system_meta.metadata.get("vcr_scenario")
            invoker = system_meta.metadata.get("vcr_invoker")
        
        invoker_prefix = VCRIdentityRule._get_flattened_invoker(invoker)
        if scenario:
            return f"fixture_{invoker_prefix}_{scenario}.json"
            
        short_id = trace_id[:8]
        return f"fixture_{invoker_prefix}_auto_{short_id}.json"

class VCRManager:
    MAX_HISTORY = 5

    def __init__(self, config: VCRPlaybackConfig, fixture_dir: str):
        self.config = config
        self.fixture_dir = fixture_dir
        self.memory_fixtures: Dict[str, dict] = {}
        self.trace_routing_map: Dict[str, str] = {}
        
        if self.config.mode in ("record", "replay"):
            os.makedirs(self.fixture_dir, exist_ok=True)
            self._parse_and_resolve_targets(self.config.target_traces)

    def _parse_and_resolve_targets(self, trace_str: str):
        if not trace_str:
            return
            
        implicit_pool = set()
        for item in trace_str.split(","):
            item = item.strip()
            if not item: continue
            
            if ":" in item:
                scenario, t_id = item.split(":", 1)
                self.trace_routing_map[scenario.strip()] = t_id.strip()
            else:
                implicit_pool.add(item)
                
        if implicit_pool:
            log.warning(f"[VCR] 암시적 Trace ID 감지됨. 디렉토리 프리-스캔 시작.")
            for filepath in glob.glob(os.path.join(self.fixture_dir, "*.json")):
                if not implicit_pool:
                    break
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        scenario = data.get("scenario")
                        available_traces = set(data.get("traces", {}).keys())
                        
                        matched = implicit_pool & available_traces
                        for t_id in matched:
                            if scenario:
                                self.trace_routing_map[scenario] = t_id
                            implicit_pool.remove(t_id)
                            log.info(f"[VCR] 🔍 매핑 성공: {t_id} -> Scenario '{scenario}'")
                except Exception:
                    continue
                    
            if implicit_pool:
                log.error(f"[VCR] 다음 Trace ID를 어떤 픽스처 파일에서도 찾을 수 없습니다: {implicit_pool}")

    def _get_filepath(self, trace_id: str, ctx: Any) -> str:
        filename = VCRIdentityRule.get_fixture_filename(trace_id, ctx)
        return os.path.join(self.fixture_dir, filename)

    def get_fixture(self, trace_id: str, ctx: Any, raise_on_missing: bool = True) -> Optional[dict]:
        filepath = self._get_filepath(trace_id, ctx)
        
        if filepath not in self.memory_fixtures:
            if os.path.exists(filepath):
                with open(filepath, "r", encoding="utf-8") as f:
                    self.memory_fixtures[filepath] = json.load(f)
            else:
                if self.config.mode == "replay" and raise_on_missing:
                    raise FileNotFoundError(f"VCR Fixture not found for '{filepath}'.")
                return None
                
        file_data = self.memory_fixtures[filepath]
        
        system_meta = getattr(ctx, "system_meta", None)
        scenario = system_meta.metadata.get("vcr_scenario") if system_meta and system_meta.metadata else "auto"
        
        target_trace = self.trace_routing_map.get(scenario)
        if not target_trace:
            target_trace = file_data.get("latest_trace_id")
            
        if not target_trace or target_trace not in file_data.get("traces", {}):
            if self.config.mode == "replay" and raise_on_missing:
                raise ValueError(f"Valid trace data not found in '{filepath}' for trace_id '{target_trace}'")
            return None
            
        fixture = file_data["traces"][target_trace]
        fixture["_actual_replayed_trace_id"] = target_trace
        return fixture

    def save_fixture(self, trace_id: str, fixture_data: dict, ctx: Any):
        if self.config.mode != "record": return
        
        filepath = self._get_filepath(trace_id, ctx)
        file_data = {"scenario": "", "latest_trace_id": "", "history_keys": [], "traces": {}}

        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    file_data = json.load(f)
            except Exception:
                pass

        system_meta = getattr(ctx, "system_meta", None)
        scenario = system_meta.metadata.get("vcr_scenario") if system_meta and system_meta.metadata else "auto"

        file_data["scenario"] = scenario
        file_data["latest_trace_id"] = trace_id
        
        if "traces" not in file_data: file_data["traces"] = {}
        if "history_keys" not in file_data: file_data["history_keys"] = []
        
        file_data["traces"][trace_id] = fixture_data
        
        if trace_id in file_data["history_keys"]:
            file_data["history_keys"].remove(trace_id)
        file_data["history_keys"].append(trace_id)
        
        while len(file_data["history_keys"]) > self.MAX_HISTORY:
            oldest_id = file_data["history_keys"].pop(0)
            file_data["traces"].pop(oldest_id, None)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(file_data, f, indent=2, ensure_ascii=False)
            
        self.memory_fixtures[filepath] = file_data
        log.info(f"💾 [VCR RECORD] Saved readable fixture to: {filepath} (Trace: {trace_id[:8]})")

    @staticmethod
    def create_empty_fixture(trace_id: str, ctx: Any) -> dict:
        original_kwargs = getattr(ctx, "original_kwargs", {})
        req_ctx = VCRRequestContext(
            model=getattr(ctx, "model", "unknown-model"),
            provider=getattr(ctx, "custom_llm_provider", "unknown-provider"),
            messages=original_kwargs.get("messages", []),
            parameters={
                "temperature": original_kwargs.get("temperature"),
                "max_tokens": original_kwargs.get("max_tokens"),
                "stream": original_kwargs.get("stream", False)
            }
        )
        
        trace_state = VCRFixtureTraceState(
            trace_id=trace_id,
            request_context=req_ctx
        )
        return trace_state.model_dump(exclude_none=True)