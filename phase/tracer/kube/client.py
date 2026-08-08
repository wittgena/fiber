# phase.tracer.kube.client
## @lineage: entry.tracer.kube.client
## @lineage: receptor.surface.tracer.kube.client
## @lineage: surface.tracer.kube.client
## @lineage: watcher.tracer.kube.client
import os, json, yaml, ssl, inspect, asyncio
import httpx

from arch.topos.network.channel.pipeline import DuplexChannel, ChannelContext
from watcher.plane.metric.scale.emitter import IScaleAdapter
from watcher.plane.emitter import get_emitter

META_INFO = {
    "VERSION": "2.0.0 (Pure Async Pipeline & httpx Edition)",
    "SYSTEM": "Kube-Self ISO Engine (Zero-Dependency Micro-SDK)"
}

KUBE_API_SPECS = {
    "configmaps":  {"base": "/api/v1",       "verbs": ["list", "watch"], "ns": True},
    "deployments": {"base": "/apis/apps/v1", "verbs": ["list", "patch"], "ns": True}
}

log = get_emitter("tracer.kube")

class AsyncKubeClient:
    """@desc: urllib를 제거하고 httpx 기반으로 재탄생한 비동기 KubeClient"""
    def __init__(self):
        self.server, self.token, self.ctx = "", None, ssl.create_default_context()
        self._load_config()
        
        # 재사용 가능한 비동기 HTTP 커넥션 풀 구성
        hdrs = {'Accept': 'application/json', **({'Authorization': f"Bearer {self.token}"} if self.token else {})}
        self.http = httpx.AsyncClient(verify=self.ctx, headers=hdrs)

    def _load_config(self):
        tk_path, ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/token", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        if os.path.exists(tk_path):
            self.server = f"https://{os.environ.get('KUBERNETES_SERVICE_HOST')}:{os.environ.get('KUBERNETES_SERVICE_PORT')}"
            self.token = open(tk_path).read().strip()
            return self.ctx.load_verify_locations(ca_path)
            
        cfg = yaml.safe_load(open(os.path.expanduser("~/.kube/config")))
        ctx = next(c['context'] for c in cfg.get('contexts', []) if c['name'] == cfg.get('current-context'))
        self.server = next(c['cluster']['server'] for c in cfg.get('clusters', []) if c['name'] == ctx['cluster'])
        self.ctx.check_hostname, self.ctx.verify_mode = False, ssl.CERT_NONE

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
                
            if res not in checked[base]: raise RuntimeError(f"리소스 누락: {base}/{res}")
            if any(v not in checked[base][res] for v in spec["verbs"]): raise RuntimeError(f"권한 누락: '{res}' 접근 불가")
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


class IsoEngineChannel(DuplexChannel):
    """
    @desc: Threading 무한루프를 버리고 Network Pipeline 구조에 완벽히 융합된 이벤트 라우터.
           수신된 JSON(Watch Event)을 분석하여 선언적 데코레이터 함수로 바인딩합니다.
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

        # 비동기 단일 스레드 구조이므로 Lock이 필요 없음 (메모리 파편화 방지만 수행)
        if eid in self._processed: return
        if len(self._processed) > 10000: self._processed.clear()
        self._processed.add(eid)

        sig = inspect.signature(func).parameters
        kwargs = {k: v for k, v in [('name', meta.get("name")), ('namespace', meta.get("namespace", "default")), ('data', k8s_obj.get("data", {})), ('logger', get_emitter(f"iso.{func.__name__}"))] if k in sig}
        
        try:
            # 핸들러가 async 함수인지 확인 후 안전하게 실행
            if inspect.iscoroutinefunction(func): await func(**kwargs)
            else: func(**kwargs)
        except Exception as e: log.error(f"Handler '{func.__name__}' failed: {e}")

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