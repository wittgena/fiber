# phase.node.resonance.aligner
## @lineage: watcher.ator.resonance.aligner
## @lineage: arch.topos.ator.resonance.aligner
## @lineage: phase.ator.resonance.aligner
import httpx
import json
import asyncio
import inspect
from pathlib import Path
from typing import Any, Dict
from arch.model.phase.flow import PhaseFlow, FlowState, Align
from arch.contract.registry.unified import contract
from watcher.plane.emitter import get_logger

log = get_logger('resonance.aligner')

@contract.ator("file.writer")
class FileWriter(Align):
    """@flow: 내부의 ψ를 물리적 Φ(파일)로 투영"""
    def align(self, flow: PhaseFlow, spec: Dict[str, Any]) -> Dict[str, Any]:
        code = flow.payload.get("code")
        target_path = (spec.get("target") or spec.get("context", {}).get("target"))
        if not code or not target_path:
            log.error(f"  [Aligner] Failure: code({bool(code)}), target({target_path})")
            return {
                "status": "error",
                "payload": flow.payload,
                "state": {
                    "alignment_status": "failed",
                    "reason": "missing_data"
                }
            }

        try:
            p = Path(target_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(code, encoding="utf-8")
            log.info(f"  [Aligner] Materialized: {p.resolve()}")
            return {
                "status": "stable",
                "payload": {
                    **flow.payload,
                    "materialized_path": str(p.resolve())
                },
                "state": {
                    "alignment_status": "success"
                }
            }
        except Exception as e:
            log.error(f"  [Aligner] Exception: {e}", exc_info=True)
            return {
                "status": "fractured",
                "payload": flow.payload,
                "state": {
                    "alignment_status": "failed",
                    "error": str(e)
                }
            }

@contract.ator("http.probe.aligner")
class HttpProbeAligner(Align):
    """@flow: 외부 시스템의 계약(OpenAPI)을 내부 상태(Φ)로 동기화"""
    def align(self, flow: PhaseFlow, spec: Dict[str, Any]) -> Dict[str, Any]:
        context = spec.get("context", {})
        endpoint = context.get("endpoint", "http://0.0.0.0:8000/openapi.json")
        timeout = context.get("timeout", 5.0)

        log.info(f"  [Probe Aligner] Extracting live contract from {endpoint}")
        
        try:
            response = httpx.get(endpoint, timeout=timeout)
            response.raise_for_status()
            openapi_spec = response.json()
            
            log.info("  [Probe Aligner] Contract successfully synchronized.")
            return {
                "status": "stable",
                "payload": {
                    **flow.payload,
                    "openapi_spec": openapi_spec
                },
                "state": {"alignment_status": "success"}
            }
        except Exception as e:
            log.error(f"  [Probe Aligner] Fracture detected: {e}")
            return {
                "status": "fractured",
                "payload": flow.payload,
                "state": {
                    "alignment_status": "failed",
                    "error": str(e)
                }
            }

@contract.ator("spec.projection.aligner")
class SpecProjectionAligner(Align):
    """@flow: 내부 메모리에 바인딩된 계약(Spec)을 콘솔 표면에 투영하여 인간의 개입 유도"""
    def align(self, flow: PhaseFlow, spec: Dict[str, Any]) -> Dict[str, Any]:
        payload = flow.payload
        openapi_spec = payload.get("openapi_spec", {})
        
        # 이전 노드(Transductor)에서 실패했던 컨텍스트 추출
        failed_path = payload.get("last_failed_path", "unknown")
        failed_method = payload.get("last_failed_method", "post")
        
        log.info(f"  [Projection Aligner] Projecting contract for {failed_method.upper()} {failed_path}")
        
        if openapi_spec:
            route_spec = openapi_spec.get("paths", {}).get(failed_path, {}).get(failed_method.lower())
            if route_spec:
                log.info("    [Φ:contract] Required Spec:")
                body = route_spec.get("requestBody")
                if body:
                    body_str = json.dumps(body, indent=2, ensure_ascii=False)
                    indented_body = "\n".join(f"      {line}" for line in body_str.splitlines())
                    log.info(f"      - Body:\n{indented_body}")
            else:
                log.info("    [Φ:contract] No spec found in synchronized contract.")
        else:
            log.info("    [Φ:contract] Contract was not synchronized. Projection impossible.")

        # 투영 후에도 시스템 루프를 계속 돌리기 위해 상태는 stable로 반환 (Force Traversal)
        return {
            "status": "stable",
            "payload": payload,
            "state": {"alignment_status": "projected"}
        }