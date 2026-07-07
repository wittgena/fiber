# bound.watcher.prom.receptor
## @lineage: xphi.watcher.sphere.prom.receptor
## @lineage: bound.watcher.sphere.prom.receptor
"""
@desc: Structural Prometheus Phase Ingress Receptor
@flow: 
  [Sense] TICK ↦ Adapters.query ↦ PromClient 
  [Map]   PromClient(Raw) ↦ Adapters.translate() ↦ Ψ (Universal Payload)
  [Emit]  Ψ ↦ EventBus ↦ Σ_field
"""
import asyncio
import time
import httpx
from typing import List, Dict, Any, Optional
from bound.watcher.prom.adapter import IPromAdapter, DeclarativePromAdapter

from arch.proto.event.psi import PsiEvent
from arch.contract.interface import IPhaseAtor, IPhaseField
from arch.proto.event.bus import AsyncEventBus
from arch.contract.registry.unified import contract
from watcher.plane.emitter import get_emitter

log = get_emitter("prom.receptor")

class PrometheusClient:
    """
    @role: External protocol adapter (HTTP/PromQL)
    @desc: Pure executor decoupled from phase dynamics.
    """
    def __init__(self, endpoint: str):
        self.endpoint = endpoint.rstrip("/")

    async def query(self, promql: str) -> List[Dict[str, Any]]:
        """@map: PromQL ↦ Raw Timeseries Vector"""
        url = f"{self.endpoint}/api/v1/query"
        params = {"query": promql}
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, params=params, timeout=5.0)
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") == "success":
                    return data["data"].get("result", [])
            except Exception as e:
                log.error(f"[PromClient] Query failed for '{promql}': {e}")
            return []

@contract.ator("prom.receptor")
class PromReceptor(IPhaseAtor):
    """
    @role: Pluggable Phase Ingress Router (Ω_ingress)
    @desc: Orchestrates data ingestion via declarative LLM rules and imperative fallback adapters.
           Strictly isolated from direct field (Φ) mutation.
    """
    def __init__(
        self, 
        ator_id: str, 
        prom_url: str, 
        llm_rules: Optional[List[Dict[str, Any]]] = None,
        custom_adapters: Optional[List[IPromAdapter]] = None,
        **kwargs
    ):
        self._id = ator_id
        self._state = "IDLE"
        self.prom_url = prom_url
        self.client = None
        self.max_emit = kwargs.get("max_emit", 100)
        
        ## @topos: Adapter Array Initialization
        self.adapters: List[IPromAdapter] = custom_adapters or []
        
        ## @flow: LLM Declarative Rules ↦ DeclarativePromAdapter Injection
        if llm_rules:
            for rule in llm_rules:
                self.adapters.append(DeclarativePromAdapter(rule))

    @property
    def ator_id(self) -> str: 
        return self._id
    
    @property
    def state(self) -> str: 
        return self._state

    async def _init_client(self):
        """@flow: Lazy initialization of network client"""
        if self.client is None:
            self.client = PrometheusClient(endpoint=self.prom_url)
            log.info(f"[Receptor] Prometheus Connection initialized: {self.prom_url}")

    async def react(self, event: PsiEvent, field: IPhaseField, bus: AsyncEventBus) -> None:
        """
        @flow: 
          1. Synchronize (TICK)
          2. Scatter Queries (I/O Bound)
          3. Gather & Translate (Polymorphism)
          4. Emit Phase Metrics (Ψ)
        """
        if event.event_type not in ["TICK", "SYSTEM_TICK"]:
            return

        await self._init_client()

        ## @exec: Parallel PromQL execution across all registered adapters
        tasks = [self.client.query(adapter.query) for adapter in self.adapters]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)

        emit_count = 0
        
        ## @map: Iterate over polymorphic adapters and raw results
        for adapter, raw_results in zip(self.adapters, results_list):
            if isinstance(raw_results, Exception):
                log.error(f"[Receptor] Adapter query failed: {raw_results}")
                continue

            for raw_item in raw_results:
                if emit_count >= self.max_emit:
                    break
                
                try:
                    ## @transform: Heterogeneous Raw Data ↦ Universal Payload
                    payload = adapter.translate(raw_item)
                    resource_id = payload.pop("resource_id", "global")
                    
                    ## @emit: Inject Ψ_event into Topos Stream
                    await bus.publish(PsiEvent(
                        event_id=f"prom-{int(time.time() * 1000)}-{resource_id}",
                        parent_id=event.event_id,
                        event_type="PHASE_METRIC",
                        source_id=resource_id,
                        scope="LOCAL",
                        payload=payload,
                        tick=int(time.time())
                    ))
                    emit_count += 1
                    
                except Exception as e:
                    log.error(f"[Receptor] Translation fault: {e} | Trace: {raw_item}")

        self._state = "SYNCED"
        if emit_count > 0:
            log.debug(f"[Receptor] Emitted {emit_count} Universal Metric Vectors (Ψ).")