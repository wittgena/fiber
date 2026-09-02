# fiber.phase.kernel.tracer.kube
## @lineage: fiber.phase.debug.tracer.kube
import os
import json
import yaml
import ssl
import inspect
import asyncio
import httpx
from typing import Optional, Any, Dict, List

# Arch & Topos Imports
from xphi.kernel.phase.network.channel.pipeline import ChannelPipeline, ChannelContext, DuplexChannel
from xphi.kernel.phase.network.channel.codec import JsonMessageCodec

# Watcher Imports
from xphi.watcher.plane.metric.scale.emitter import IScaleAdapter
from xphi.watcher.plane.emitter import get_emitter
from xphi.watcher.tracer.bound import BaseBoundary, BaseStreamAuditor, BaseAuditor

"""METADATA & CONFIGURATION"""
META_INFO = {
    "VERSION": "2.0.0 (Pure Async Pipeline & httpx Edition)",
    "SYSTEM": "Kube-Self ISO Engine (Zero-Dependency Micro-SDK)"
}

KUBE_API_SPECS = {
    "configmaps":  {"base": "/api/v1",       "verbs": ["list", "watch"], "ns": True},
    "deployments": {"base": "/apis/apps/v1", "verbs": ["list", "patch"], "ns": True}
}

log = get_emitter("tracer.kube")

"""CORE CLIENT & BOUNDARY"""
class AsyncKubeClient:
    """@desc: urllib를 제거하고 httpx 기반으로 재탄생한 비동기 KubeClient"""
    def __init__(self):
        self.server = ""
        self.token = None
        self.ctx = ssl.create_default_context()
        self._load_config()

        hdrs = {'Accept': 'application/json'}
        if self.token:
            hdrs['Authorization'] = f"Bearer {self.token}"
            
        self.http = httpx.AsyncClient(verify=self.ctx, headers=hdrs)

    def _load_config(self):
        tk_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        
        if os.path.exists(tk_path):
            self.server = f"https://{os.environ.get('KUBERNETES_SERVICE_HOST')}:{os.environ.get('KUBERNETES_SERVICE_PORT')}"
            with open(tk_path, "r") as f:
                self.token = f.read().strip()
            return self.ctx.load_verify_locations(ca_path)
            
        cfg = yaml.safe_load(open(os.path.expanduser("~/.kube/config")))
        ctx = next(c['context'] for c in cfg.get('contexts', []) if c['name'] == cfg.get('current-context'))
        self.server = next(c['cluster']['server'] for c in cfg.get('clusters', []) if c['name'] == ctx['cluster'])
        self.ctx.check_hostname = False
        self.ctx.verify_mode = ssl.CERT_NONE

    def get_entry(self, res: str, ns: str = None, name: str = None, **queries) -> str:
        spec = KUBE_API_SPECS.get(res, {"base": "", "ns": False})
        path = f"{spec['base']}{f'/namespaces/{ns}' if spec.get('ns') and ns else ''}/{res}{f'/{name}' if name else ''}"
        q_str = "&".join(f"{k}={v}" for k, v in queries.items() if v)
        return f"{path}?{q_str}" if q_str else path

    async def validate(self):
        log.info(f"[Φ:Validate] K8s 비동기 API 스캔 시작 ({META_INFO['VERSION']})")
        checked = {}
        for res, spec in KUBE_API_SPECS.items():
            base = spec["base"]
            if base not in checked:
                resp = await self.http.get(f"{self.server}{base}")
                checked[base] = {r["name"]: r["verbs"] for r in resp.json().get("resources", [])}
                
            if res not in checked[base]: 
                raise RuntimeError(f"리소스 누락: {base}/{res}")
            if any(v not in checked[base][res] for v in spec["verbs"]): 
                raise RuntimeError(f"권한 누락: '{res}' 접근 불가")
        log.info("[Φ:Validate] 스펙/권한 100% 일치 확인.")

    async def request(self, method: str, path: str, payload: dict = None):
        url = f"{self.server}{path}"
        hdrs = {'Content-Type': 'application/strategic-merge-patch+json'} if method == 'PATCH' else {}
        resp = await self.http.request(method, url, json=payload, headers=hdrs)
        resp.raise_for_status()
        return resp.json()

    async def list_deployments(self, ns="default", label_selector=""):
        return await self.request('GET', self.get_entry("deployments", ns=ns, labelSelector=label_selector))

    async def close(self):
        await self.http.aclose()

class KubeBound(BaseBoundary):
    """@desc: ThreadPool을 완벽히 제거하고 비동기 Pipeline과 httpx로 구동되는 새로운 KubeBound"""
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        self.http_client = httpx.AsyncClient(verify=False, headers=self.headers)
        
        self.pipeline = ChannelPipeline()
        self.pipeline.add_last(JsonMessageCodec())  # 1단계: Bytes -> JSON 변환기
        self.transports: List['KubeWatchTransport'] = []
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

    def attach_stream(self, path: str) -> 'KubeWatchTransport':
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

"""TRANSPORT & CHANNELS"""
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

class IsoEngineChannel(DuplexChannel):
    """
    @desc: 
    - Threading 무한루프를 버리고 Network Pipeline 구조에 완벽히 융합된 이벤트 라우터
    - 수신된 JSON(Watch Event)을 분석하여 선언적 데코레이터 함수로 바인딩
    """
    def __init__(self, client: AsyncKubeClient):
        self.client = client
        self.create_h, self.timer_h = [], []
        self._processed = set()
        self._tasks = []  # 백그라운드 타이머 태스크 관리용

    def on_create(self, res: str, labels: dict = None):
        def dec(func): self.create_h.append((res, labels or {}, func)); return func
        return dec

    def timer(self, res: str, labels: dict = None, interval: float = 15.0):
        def dec(func): self.timer_h.append((res, labels or {}, interval, func)); return func
        return dec

    async def _inject(self, func, k8s_obj):
        meta = k8s_obj.get("metadata", {})
        eid = f"{meta.get('namespace')}/{meta.get('name')}/{meta.get('resourceVersion')}"

        if eid in self._processed: return
        if len(self._processed) > 10000: self._processed.clear()
        self._processed.add(eid)

        sig = inspect.signature(func).parameters
        kwargs = {k: v for k, v in [
            ('name', meta.get("name")), 
            ('namespace', meta.get("namespace", "default")), 
            ('data', k8s_obj.get("data", {})), 
            ('logger', get_emitter(f"iso.{func.__name__}"))
        ] if k in sig}
        
        try:
            # 핸들러가 async 함수인지 확인 후 안전하게 실행
            if inspect.iscoroutinefunction(func): await func(**kwargs)
            else: func(**kwargs)
        except Exception as e: 
            log.error(f"Handler '{func.__name__}' failed: {e}")

    # --- Pipeline Channel Implementations ---
    async def channel_active(self, ctx: ChannelContext):
        """파이프라인 연결(Ignite) 시 타이머 태스크들을 이벤트 루프에 등록"""
        log.info("[Supervisor] 점화 시퀀스 개시 (ISO Engine Channel)")
        await self.client.validate()
        
        for res, labels, intv, func in self.timer_h:
            task = asyncio.create_task(self._run_timers(res, labels, intv, func))
            self._tasks.append(task)
            
        await ctx.fire_channel_active()

    async def channel_inactive(self, ctx: ChannelContext):
        """파이프라인 정지(Collapse) 시 모든 타이머 태스크 취소 (우아한 종료)"""
        log.info("[Supervisor] ISO Engine 가동 중지 신호 수신")
        for task in self._tasks:
            if not task.done(): task.cancel()
        await self.client.close()
        await ctx.fire_channel_inactive()

    async def channel_read(self, ctx: ChannelContext, msg: any):
        """
        JsonMessageCodec을 거쳐 들어온 K8s Watch Event(Dict)를 수신합니다.
        스레드 기반의 _watch 루프를 완벽히 대체하는 이벤트 드리븐 로직입니다.
        """
        if isinstance(msg, dict) and msg.get('type') == 'ADDED':
            obj = msg.get('object', {})
            kind = obj.get('kind', '').lower() + "s"  # 예: ConfigMap -> configmaps
            
            for res, labels, func in self.create_h:
                # 리소스 타입 매칭 및 라벨 필터링 처리
                if res == kind:
                    obj_labels = obj.get("metadata", {}).get("labels", {})
                    if all(obj_labels.get(k) == v for k, v in labels.items()):
                        await self._inject(func, obj)
                        
        # 로직 처리 후 다음 파이프라인 채널로 메시지 패스(Bypass)
        await ctx.fire_channel_read(msg)

    async def _run_timers(self, res: str, labels: dict, interval: float, func):
        """스레드를 대체하는 Asyncio 네이티브 타이머 루프"""
        ls = ",".join(f"{k}={v}" for k, v in labels.items())
        try:
            while True:
                if res == "deployments":
                    resp = await self.client.list_deployments(label_selector=ls)
                    for item in resp.get('items', []):
                        await self._inject(func, item)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

"""AUDITORS & ADAPTERS"""
class KubeStreamAuditor(BaseStreamAuditor, DuplexChannel):
    """@desc: Queue와 Thread 통신 방식을 버리고, 자신 스스로가 파이프라인의 끝단 채널(Channel)이 되어 이벤트를 수신하는 모듈"""
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

class KubeScaleAdapter(IScaleAdapter):
    """@desc: 추상화된 스케일 명령을 실제 K8s Deployment 패치(Patch) API로 변환하는 브릿지"""
    def __init__(self, namespace: str = "default"):
        self.namespace = namespace
        self.client = None
        self.log = get_emitter("adapter.kube")

    async def initialize(self) -> None:
        self.client = AsyncKubeClient()
        await self.client.validate()
        self.log.info(f"KubeScaleAdapter initialized in namespace '{self.namespace}'")

    async def apply_scale(self, target_resource: str, replicas: int) -> bool:
        if not self.client:
            return False
            
        patch_payload = {
            "spec": {
                "replicas": replicas
            }
        }
        try:
            # AsyncKubeClient의 동적 엔트리 라우터를 활용한 PATCH 요청
            path = self.client.get_entry("deployments", ns=self.namespace, name=target_resource)
            await self.client.request('PATCH', path, payload=patch_payload)
            self.log.info(f"Successfully scaled K8s Deployment '{target_resource}' to {replicas}.")
            return True
        except Exception as e:
            self.log.error(f"Failed to scale K8s Deployment '{target_resource}': {e}")
            return False