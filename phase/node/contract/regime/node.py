# meta.phase.contract.regime.node
## @lineage: phase.contract.regime.node
## @lineage: phase.node.contract.field.regime
from __future__ import annotations
import json
import asyncio
import math
import random
from typing import Optional, List

from arch.contract.event.psi import PsiCarrier, PsiEvent
from arch.contract.interface import IPhaseField, ICriticalDetector, ISystemRegime, IPhaseAtor
from arch.contract.registry.unified import contract 
from arch.contract.discovery import discover_modules

from arch.contract.registry.unified import contract
from arch.contract.interface import ISystemRegime, IPhaseField, IPhaseAtor
from arch.contract.event.psi import PsiEvent
from kernel.phase.runtime.executor.dynamics import DynamicsExecutor
from kernel.phase.runtime.node import NodeRuntime
from kernel.bind.resolver import find_current_self

from kernel.phase.runtime.flow.cont import LoopCarrier
from watcher.plane.emitter import get_emitter

log = get_emitter("field.regime", phase="BOOT")

@contract.regime("node.regime")
class NodeRegime(ISystemRegime):
    """
    @role: Γ_rupture actuator | System reset & topological realignment
    @flow: XeCont → Φ.commit() → Γ.modify_field()
    """
    def __init__(self, **kwargs):
        self.params = kwargs

    def modify_field(self, field: IPhaseField) -> None:
        """
        @phase.execution: Topology Reset
        - @step.1: tension(all) → 0.0
        - @step.2: phase(NORMAL) → random(0, 2π) [Scattering]
        - @step.3: phase(REFLECTOR) → 0.0 [New Baseline]
        - @step.4: pressure(Φ) → 0.0 [Absolute Vacuum]
        """
        states = field.get_state()
        
        for node_id, data in states.items():
            ## @trace: release condensed fatigue
            data["tension"] = 0.0
            
            if data["state"] == "NORMAL":
                data["phase"] = random.uniform(0, 2 * math.pi)
            elif data["state"] == "REFLECTOR":
                data["phase"] = 0.0  

        if hasattr(field, 'pressure'):
            field.pressure = 0.0
            
        print("[Regime] Field collapsed and reformed. Tension reset to 0.0")

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        ## @flow: Γ → constraint(Ator)
        pass

    def filter_event(self, event: PsiEvent) -> Optional[PsiEvent]:
        """
        @invariant: Ψ_event.epoch == "new"
        @step: Drop residues from prior epochs (Ψ_old → ∅)
        """
        if event.context.get("epoch") != "new":
            pass
        return event

@contract.watcher("topos.watcher")
class ToposWatcher(ICriticalDetector):
    """
    @role: ∂Φ 임계 감시자
    @desc: Receptor가 주입한 Field의 텐션(CPU 등)을 평가하여 스케일링 임계점 돌파를 감지
    """
    def __init__(self, upper_limit: float = 80.0, lower_limit: float = 20.0):
        self.upper_limit = upper_limit
        self.lower_limit = lower_limit

    def evaluate(self, field: IPhaseField, history: list, current_tick: int, parent: PsiEvent) -> Optional[PsiEvent]:
        ## 시스템 필드에 등록된 모든 리소스(Pod/Deployment) 상태 스캔
        for node_id, data in field.get_state().items():
            tension = data.get("tension", 0.0)
            state = data.get("state", "NORMAL")

            ## 이미 확장/수축 중인 상태(Cooldown)라면 무시하여 중복 Commit 방지
            if state in ["SCALED_OUT", "SCALED_IN"]:
                continue

            carrier = None
            ## 팽창 임계점 돌파 (Φ0 전이 요청)
            if tension >= self.upper_limit:
                carrier = PsiCarrier(kind="AWS_SCALE_REQUEST", tag=node_id, payload="Φ0")
            
            ## 수축 임계점 돌파 (∂Φ 전이 요청)
            elif tension <= self.lower_limit:
                carrier = PsiCarrier(kind="AWS_SCALE_REQUEST", tag=node_id, payload="∂Φ")

            if carrier:
                log.warn(f"Threshold breached for {node_id} (Tension: {tension:.1f}). Emitting Phase Transition.")
                return PsiEvent(
                    event_id=f"scale-{current_tick}-{node_id}", parent_id=parent.event_id,
                    source_id="receptor.watcher", scope="SYSTEMIC", tick=current_tick, carrier=carrier
                )
        return None

@contract.regime("scale.regime")
class ScaleRegime(ISystemRegime):
    """
    @role: 위상 전이 체제
    @desc: Watcher가 이벤트를 발생시키면, 해당 노드를 '냉각 상태(Cooldown)'로 전이시켜 중복 스케일링 차단
    """
    def modify_field(self, field: IPhaseField, target_id: str) -> None:
        target_data = field.get_state().get(target_id)
        if target_data:
            ## 텐션에 따라 상태를 분기 (향후 Tick에 따라 NORMAL로 복구하는 로직 추가 가능)
            tension = target_data.get("tension", 0.0)
            target_data["state"] = "SCALED_OUT" if tension >= 50.0 else "SCALED_IN"
            target_data["tension"] = 50.0  # 스케일링 후 텐션 초기화 (안정 상태)

    def constrain_ator(self, ator: IPhaseAtor) -> None:
        pass ## Projector나 Receptor의 직접적인 상태를 제약할 필요는 없음


async def main():
    """@entry: watcher.sphere System Boot Sequence"""
    ## 컴포넌트 자동 탐색 및 레지스트리 등록 (Receptor, Projector, Watcher 등)
    discover_modules(find_current_self())
    
    ## 시스템 선언 페이로드 (Config)
    config_payload = """
    {
      "system_type": "SPHERE_GITOPS_AUTOSCALER",
      "runtime": { "seed": 42, "max_ticks": -1, "sleep_interval": 15.0, "dt": 1.0 },
      "field": { "type": "systemic.field", "params": { "size": 0 } },
      "kernel": { "type": "entropy.kernel", "params": { "tension_rate": 0.0 } },
      "watcher": { 
          "type": "topos.watcher",
          "params": { "upper_limit": 80.0, "lower_limit": 20.0 } 
      },
      "regime": { 
          "type": "scale.regime", 
          "params": {} 
      },
      "ators": [
          {
              "type": "prom.receptor",
              "id": "sensor.prom",
              "initial_state": "IDLE",
              "params": {
                  "prom_url": "http://prometheus:9090",
                  "metric_specs": [
                      {
                          "name": "cpu_tension", 
                          "promql": "rate(container_cpu_usage_seconds_total{namespace='production'}[1m]) * 100", 
                          "resource_key": "pod"
                      }
                  ]
              }
          },
          {
              "type": "gitops.projector",
              "id": "actuator.gitops",
              "initial_state": "IDLE",
              "params": { 
                  "repo_path": "/var/git-workspace/cluster-manifests", 
                  "manifest_file": "production/deployments.yaml",
                  "branch": "main"
              }
          }
      ]
    }
    """
    config_dict = json.loads(config_payload)

    ## 코어 시스템 Executor 생성 (SystemBuilder 우회 호출)
    watcher_xe = DynamicsExecutor(config_dict=config_dict)

    ## 15초(sleep_interval)마다 한 번씩 전체 시스템(Prom 쿼리 -> 감시 -> Git 반영)이 순환
    loop_xe = LoopCarrier(
        xe=watcher_xe, 
        max_ticks=config_dict["runtime"]["max_ticks"], 
        interval=config_dict["runtime"]["sleep_interval"]
    )
    node = NodeRuntime(executor=loop_xe)
    async def boot_clock():
        await asyncio.sleep(2.0)
        log.info(">>> Injecting GitOps Systemic Boot Pulse... <<<")
        seed_carrier = PsiCarrier(kind="TICK", tag="SEED", payload={})
        seed_event = PsiEvent(
            event_id="boot-watcher", parent_id=None, source_id="system.boot",
            scope="GLOBAL", tick=1, carrier=seed_carrier, context={"phase": "loop"}
        )
        await node.bus.publish(seed_event)

    asyncio.create_task(boot_clock())
    log.info(f"Watcher Node launching System-GitOps...")
    await node.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("System gracefully shutting down.")
        