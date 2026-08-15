# dphi.tracer.kube.bound
import asyncio
import httpx
from typing import Optional, Any, Dict, List

from arch.topos.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel
from arch.topos.network.channel.codec import JsonMessageCodec
from watcher.tracer.bound import BaseBoundary, BaseStreamAuditor, BaseAuditor
from watcher.plane.emitter import get_emitter

class KubeWatchTransport:
    """@desc: K8s Watch API의 HTTP Chunk 스트림을 읽어 파이프라인으로 주입하는 Async Transport"""
    def __init__(self, pipeline: ChannelPipeline, client: httpx.AsyncClient, url: str):
        self.pipeline = pipeline
        self.client = client
        self.url = url
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.log = get_emitter("kube.transport")

    async def connect(self) -> None:
        self.is_running = True
        await self.pipeline.fire_channel_active()
        self._task = asyncio.create_task(self._stream_watch())

    async def _stream_watch(self) -> None:
        try:
            # httpx를 활용한 네이티브 비동기 스트림 (블로킹 없음)
            async with self.client.stream("GET", self.url) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    if not self.is_running:
                        break
                    if chunk:
                        # 파이프라인으로 원시 바이트(Bytes) 주입 
                        # -> 이후 JsonMessageCodec이 처리하여 Dict로 변환함
                        await self.pipeline._process_read(chunk)
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.error(f"  [TRANSPORT_FAULT] Stream interrupted: {e}")
            await self.pipeline._process_exception(e)

    def disconnect(self) -> None:
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()


class KubeBound(BaseBoundary):
    """@desc: ThreadPool을 완벽히 제거하고 비동기 Pipeline과 httpx로 구동되는 새로운 KubeBound"""
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }
        # 네이티브 비동기 통신을 위한 통합 클라이언트
        self.http_client = httpx.AsyncClient(verify=False, headers=self.headers)
        
        # 이벤트 파이프라인 조립
        self.pipeline = ChannelPipeline()
        self.pipeline.add_last(JsonMessageCodec())  # 1단계: Bytes -> JSON 변환기
        
        self.transports: List[KubeWatchTransport] = []
        self.log = get_emitter("tracer.kube.bound")

    async def api_call(self, method: str, path: str, **kwargs) -> Any:
        """@desc: ThreadPool(run_in_executor)을 대체하는 네이티브 비동기 API 호출"""
        url = f"{self.base_url}{path}"
        try:
            response = await self.http_client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.log.warning(f"  [API_FAULT] Request failed ({method} {path}): {e}")
            raise

    def attach_stream(self, path: str) -> KubeWatchTransport:
        """새로운 Watch 스트림을 생성하여 파이프라인에 연결"""
        url = f"{self.base_url}{path}"
        transport = KubeWatchTransport(self.pipeline, self.http_client, url)
        self.transports.append(transport)
        return transport

    def collapse(self) -> None:
        """@desc: 자원 누수 없는 우아한 붕괴(Graceful Shutdown) 처리"""
        self.log.info("## @trace.teardown: Collapsing KubeBound async space...")
        
        # 1. 모든 Watch 스트림 정지
        for transport in self.transports:
            transport.disconnect()
            
        # 2. 비동기 HTTP 클라이언트 세션 종료
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(self.http_client.aclose())
        else:
            loop.run_until_complete(self.http_client.aclose())


class KubeStreamAuditor(BaseStreamAuditor, DuplexChannel):
    """
    @desc: Queue와 Thread 통신 방식을 버리고, 
           자신 스스로가 파이프라인의 끝단 채널(Channel)이 되어 이벤트를 수신하는 모듈
    """
    def __init__(self, target: str, boundary: KubeBound, watch_path: str):
        super().__init__(target, boundary, delay=0)
        self.watch_path = watch_path
        self._transport: Optional[KubeWatchTransport] = None
        
    def attach(self) -> None:
        # 1. 자기 자신을 Boundary 파이프라인의 핸들러로 등록
        self.boundary.pipeline.add_last(self)
        # 2. 전용 Transport 스트림 생성
        self._transport = self.boundary.attach_stream(self.watch_path)
        super().attach()

    async def run_stream(self) -> None:
        # Transport 스트리밍 엔진 점화
        if self._transport:
            await self._transport.connect()
            
        try:
            # BaseStreamAuditor의 라이프사이클 유지를 위한 무한 대기 (실제 처리는 channel_read에서 발생)
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            if self._transport:
                self._transport.disconnect()

    # --- DuplexChannel Implementation ---
    async def channel_read(self, ctx: ChannelContext, msg: Any) -> None:
        """@desc: 파이프라인을 타고 넘어온 JSON 객체 수신"""
        if isinstance(msg, dict):
            # 비즈니스 로직(K8s Event 분석) 실행
            await self.process_kube_event(msg)
            
        # 다음 채널이 있다면 통과(Bypass)
        await ctx.fire_channel_read(msg)

    async def process_kube_event(self, event: dict) -> None:
        """자식 클래스에서 오버라이드하여 비즈니스 상태 변화 처리"""
        pass 


class ApiToposAuditor(BaseAuditor):
    """@desc: 네이티브 비동기(httpx)를 적용하여 완전히 Non-blocking으로 동작하는 ToposAuditor"""
    def __init__(self, target: str, namespace: str, boundary: KubeBound):
        super().__init__(target, namespace, boundary)
        self.current_replicas = 0
        self.log = get_emitter(f"auditor.api_topos.{self.target}")

    async def _observe(self) -> None:
        try:
            while True:
                try:
                    path = f"/apis/apps/v1/namespaces/{self.namespace}/deployments"
                    # 완전한 비동기 호출 (스레드풀 미사용)
                    response = await self.boundary.api_call('GET', path, params={"labelSelector": f"app={self.target}"})
                    
                    if response and response.get("items"):
                        deployment = response["items"][0]
                        self.current_replicas = deployment.get("spec", {}).get("replicas", 0)
                        
                except Exception as e:
                    self.log.warning(f"  [API_TOPOS] Topology sync failed: {str(e)}")
                    
                await asyncio.sleep(2)
                
        except asyncio.CancelledError:
            # Tracer의 collapse() 발생 시 정상 종료
            pass