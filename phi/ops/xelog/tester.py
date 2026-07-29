# phi.ops.xelog.tester
import sys
import argparse
import asyncio
import uuid
from typing import Tuple

import httpx

from phi.ops.xelog.router import E2EConfig
from phi.ops.xelog.scene import SceneRunner
from watcher.xelog.rest import api as rest_app  

from phase.wasm.builder import WasmBuilder
from phase.wasm.tracer import WasmTracer
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("xelog.tester")

class HttpFlowTracer:
    """HTTP(S) 파이프라인의 Inbound/Outbound 트래픽을 관측하고 Flow ID로 묶어내는 Tracer"""
    
    async def trace_request(self, request: httpx.Request):
        # 1. 고유 Flow ID 생성 및 헤더 주입 (Correlation)
        flow_id = f"http_{uuid.uuid4().hex[:8]}"
        request.headers["x-flow-id"] = flow_id
        
        # 2. flow_scope를 통해 TX 로깅 컨텍스트 통일
        with flow_scope(flow_id=flow_id, phase="HTTP_TX", bound="tester"):
            log.info(f"[Trace:TX] {request.method} {request.url}")
            log.debug(f"  └─ Headers: {dict(request.headers)}")

    async def trace_response(self, response: httpx.Response):
        # 1. 원래 요청 객체에서 주입했던 Flow ID 추출
        flow_id = response.request.headers.get("x-flow-id", "unknown_flow")
        
        # 2. flow_scope를 통해 RX 로깅 컨텍스트 통일 (TX와 연결됨)
        with flow_scope(flow_id=flow_id, phase="HTTP_RX", bound="tester"):
            await response.aread()  # 본문을 메모리에 로드
            elapsed = getattr(response, "elapsed", None)
            elapsed_str = f" in {elapsed.total_seconds():.3f}s" if elapsed else ""
            
            status_log = f"[Trace:RX] {response.status_code} {response.reason_phrase}{elapsed_str}"
            if response.status_code >= 400:
                log.warning(f"{status_log}\n  └─ Body: {response.text[:200]}")
            else:
                log.info(status_log)


# =====================================================================
# Adapters & Orchestrators
# =====================================================================
class XelogTester:
    """HTTP Client(ASGITransport) 설정 및 SceneRunner 주입을 담당하는 Adapter"""
    def __init__(self, config: E2EConfig):
        self.config = config
        self.runner = SceneRunner(config)
        self.tracer = HttpFlowTracer() # Tracer 핸들러 인스턴스화

    async def execute(self) -> Tuple[bool, str]:
        try:
            log.info(f"\n[WebTesterAdapter] Starting In-Memory ASGI Workflows...")
            transport = httpx.ASGITransport(app=rest_app)
            target_url = self.config.base_url 
            
            # Event Hook에 Tracer 클래스의 메서드 매핑
            async with httpx.AsyncClient(
                transport=transport, 
                base_url=target_url,
                event_hooks={
                    'request': [self.tracer.trace_request],
                    'response': [self.tracer.trace_response]
                }
            ) as client:
                # 1. Transport와 Tracer가 결합된 Client를 시나리오 러너에 주입
                self.runner.client = client
                
                # 2. 캡슐화된 전체 E2E 파이프라인 관통! 
                # (세부 시나리오 로직은 scene.py의 워크플로우에 위임)
                success = await self.runner.execute()
                
                if not success:
                    return False, "E2E Validation Failed. See logs for detailed phase ruptures."
                
                return True, "All scenarios passed successfully."
                
        except Exception as e:
            return False, f"Critical Exception: {str(e)}"


class XelogPipeline:
    """빌드 -> 테스트 -> 추적(Trace)으로 이어지는 전체 파이프라인 라이프사이클 관리"""
    def __init__(self, config: E2EConfig):
        self.config = config
        self.log = get_emitter("xelog.pipeline")

    async def run(self) -> bool:
        self.log.info("[Pipeline] Starting Xelog Full E2E Pipeline (Build ➔ HTTP E2E ➔ Trace/Seal)...")
        
        self.log.info("[Pipeline] [Step 1] Running WasmBuilder...")
        builder = WasmBuilder()
        await builder.trace()
        if builder.rupture_confirmed:
            self.log.error("[Pipeline] Builder encountered a fatal rupture.")
            return False

        self.log.info("[Pipeline] [Step 2] Running HTTP E2E Tester via WasmTracer...")
        web_tester = XelogTester(self.config)
        tracer = WasmTracer(tester=web_tester)
        
        # 내부적으로 web_tester.execute()가 호출됨
        await tracer.trace() 
        
        if getattr(tracer, 'rupture_confirmed', False):
            self.log.error("[Pipeline] E2E Pipeline ended in a Rupture/Collapse state.")
            return False
            
        self.log.info("[Pipeline] E2E Pipeline executed & Lineage Sealed successfully.")
        return True


# =====================================================================
# CLI Entry Point
# =====================================================================
def main():
    """순수하게 CLI Arguments를 파싱하고 파이프라인을 구동하는 진입점"""
    parser = argparse.ArgumentParser(description="Xelog E2E HTTP Pipeline Tester")
    parser.add_argument("--host", type=str, default="localhost", help="Target API Host")
    parser.add_argument("--port", type=int, default=8000, help="Target API Port")
    
    args = parser.parse_args()
    config = E2EConfig(host=args.host, port=args.port)
    pipeline = XelogPipeline(config)
    
    try:
        success = asyncio.run(pipeline.run())
        if not success:
            sys.exit(1)
    except KeyboardInterrupt:
        get_emitter("xelog.cli").warning("\n[CLI] Process interrupted by user. Shutting down gracefully...")
        sys.exit(0)

if __name__ == "__main__":
    main()