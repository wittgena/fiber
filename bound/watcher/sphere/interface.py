# bound.watcher.sphere.interface
## @lineage: xphi.watcher.sphere.interface
"""
@desc: Vendor-Agnostic Phase Abstraction Layer
@flow: 
  [Vendor API] → IMetricsAdapter(Ingress) → UniversalPhaseSnapshot(Ψ)
  UniversalPhaseSnapshot(Ψ) → Node(Core Field) → Tension(∂Φ)
  Tension 해소 명령 → IInterventionAdapter(Egress) → [Vendor API]
"""
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Protocol

@dataclass
class UniversalPhaseSnapshot:
    """
    @phase.role: 범용 Ψ (Universal Phase Carrier)
    특정 벤더(AWS, K8s)에 종속되지 않은 시스템 상태의 순수 투영체입니다.
    기존의 desired_capacity, scp_allows_scaling 등의 용어를 추상화합니다.
    """
    timestamp: float
    resource_id: str
    metadata: Dict[str, str] = field(default_factory=dict) # AWS Tags, K8s Labels
    
    # Scale & Entropy Metrics
    target_scale: int = 0         # AWS desired_capacity, K8s replicas
    actual_scale: int = 0         # AWS running_instances, K8s readyReplicas
    
    # Error & Constraint Metrics
    error_weight: float = 0.0     # 다양한 에러(health_check, crash_loop 등)를 합산/정규화한 수치
    is_locked: bool = False       # 스케일링/제어 불가 상태 (AWS SCP 제한, K8s PDB 등)

class IMetricsAdapter(Protocol):
    """
    @role: 수집 포트 (Ingress Interface)
    외부 인프라의 상태를 읽어와 UniversalPhaseSnapshot으로 번역합니다.
    """
    async def fetch_snapshot(self, resource_id: str) -> UniversalPhaseSnapshot:
        ...

class IInterventionAdapter(Protocol):
    """
    @role: 제어 포트 (Egress Interface - Re-anchor)
    위상 필드(Phase Field)에서 결정된 조정 명령을 특정 벤더의 API로 번역하여 실행합니다.
    """
    async def apply_correction(self, resource_id: str, adjustments: Dict[str, Any]) -> bool:
        ...

class AwsAsgAdapter(IMetricsAdapter):
    """AWS Auto Scaling Group 구현체"""
    
    def __init__(self, aws_client):
        self.client = aws_client

    async def fetch_snapshot(self, resource_id: str) -> UniversalPhaseSnapshot:
        # 가상의 AWS API 응답
        raw_data = {
            "tags": {"Env": "prodction"}, # Semantic drift
            "desired_capacity": 3,
            "running_instances": 2,
            "failed_health_checks": 1,
            "scp_allows_scaling": True
        }
        
        return UniversalPhaseSnapshot(
            timestamp=time.time(),
            resource_id=resource_id,
            metadata=raw_data["tags"],
            target_scale=raw_data["desired_capacity"],
            actual_scale=raw_data["running_instances"],
            error_weight=float(raw_data["failed_health_checks"]), # 단순 매핑
            is_locked=not raw_data["scp_allows_scaling"]
        )

class K8sDeploymentAdapter(IMetricsAdapter):
    """Kubernetes Deployment 구현체"""
    
    def __init__(self, k8s_client):
        self.client = k8s_client

    async def fetch_snapshot(self, resource_id: str) -> UniversalPhaseSnapshot:
        # 가상의 K8s API 응답
        raw_data = {
            "labels": {"environment": "production"},
            "replicas": 3,
            "ready_replicas": 2,
            "restart_count": 5,
            "pdb_prevents_eviction": False
        }
        
        return UniversalPhaseSnapshot(
            timestamp=time.time(),
            resource_id=resource_id,
            metadata=raw_data["labels"],
            target_scale=raw_data["replicas"],
            actual_scale=raw_data["ready_replicas"],
            # K8s의 restart_count를 에러 가중치로 환산하여 Core 모델에 전달
            error_weight=raw_data["restart_count"] * 0.2, 
            is_locked=raw_data["pdb_prevents_eviction"]
        )