# fiber.infra.ops.plane.flare.tunnel
import json
import asyncio
import httpx
import time
from typing import Optional, Any, List, Tuple

from xphi.watcher.plane.emitter import get_emitter
from xphi.arch.contract.config.tunnel import BackendProtocol, resolve_default_config, parse_connection_urls
from xphi.node.runtime.wasm.method import WasmMethod

log = get_emitter("tunnel.flare")

class PayloadKey:
    """Matches broker.PayloadKey to minimize dependencies"""
    JOB_ID = "job_id"
    METHOD_FUNC = "target_func" 
    PAYLOAD = "payload"
    TIER = "tier"
    WASM_PATH = "wasm_path"
    CONTEXT = "context"

class ResultKey:
    SUCCESS = "success"
    OUTPUT = "output"
    ERROR = "error"
    EDGE_HASH = "edge_canonical_hash" 

class _MockPubSub:
    """Adapter for broker's pubsub.listen() using an asyncio.Queue"""
    def __init__(self, response_queue: asyncio.Queue):
        self.q = response_queue

    async def subscribe(self, *args, **kwargs): pass
    async def unsubscribe(self, *args, **kwargs): pass
    async def close(self, *args, **kwargs): pass

    async def listen(self):
        while True:
            msg = await self.q.get()
            yield msg

class _MockStateStore:
    """Adapter to convert broker's Redis xadd() into Cloudflare HTTP POST requests"""
    def __init__(self, http_client: httpx.AsyncClient, response_queue: asyncio.Queue):
        self.http_client = http_client
        self.q = response_queue
        self._background_tasks = set()

    async def xadd(self, name: str, fields: dict, *args, **kwargs):
        job_id = "unknown"
        try:
            # 1. 안전하게 페이로드 추출 (실패 시 즉시 종료)
            data_val = fields.get('data') or fields.get(b'data')
            if not data_val: 
                log.debug(f"[FlareTunnel:xadd] No data field found in xadd fields. Returning mock id.")
                return b'0-0'

            payload = json.loads(data_val)
            job_id = payload.get(PayloadKey.JOB_ID, "unknown")

            log.debug(f"[FlareTunnel:xadd] Intercepted payload for job_id: {job_id}. Scheduling background task.")

            # 2. HTTP 요청을 백그라운드 태스크로 스케줄링 (Fire-and-Forget)
            task = asyncio.create_task(self._process_edge_request(job_id, payload))
            self._background_tasks.add(task)
            
            # 콜백을 통해 완료된 태스크를 Set에서 안전하게 제거 (메모리 누수 방지)
            task.add_done_callback(self._background_tasks.discard)

            # Redis 메시지 ID처럼 임의의 ID 즉시 반환
            return b'edge-mock-id'

        except Exception as e:
            log.error(f"[FlareTunnel:xadd] Invalid payload format: {e}")
            return b'error-id'

    async def _process_edge_request(self, job_id: str, payload: dict):
        """실제 Cloudflare Edge로 HTTP 요청을 보내고 응답을 큐에 주입하는 비동기 워커"""
        start_time = time.time()
        log.debug(f"[FlareTunnel:_process_edge_request] Task started for job_id: {job_id}")

        try:
            method_func = payload.get(PayloadKey.METHOD_FUNC, "")
            wasm_path = payload.get(PayloadKey.WASM_PATH)
            base_payload = payload.get(PayloadKey.PAYLOAD, {})
            context = payload.get(PayloadKey.CONTEXT, {})

            requested_timeout = float(context.get("timeout", 15.0))
            dynamic_timeout = httpx.Timeout(requested_timeout + 3.0, connect=5.0)
            methods = {
                m.value for m in WasmMethod 
                if m.value not in (WasmMethod.EXECUTE_CODE.value, WasmMethod.EXECUTE_FSM.value)
            }

            if wasm_path == "phase.wasm" or method_func in methods:
                vm_tgt = "PHASE"
            elif wasm_path == "fsm.wasm" or method_func == WasmMethod.EXECUTE_FSM.value:
                vm_tgt = "FSM"
            else:
                vm_tgt = "PYTHON"

            if vm_tgt == "PHASE":
                actual_edge_payload = {
                    "method": method_func,     
                    "context": context,        
                    "payload": base_payload    
                }
                rpc_method = "invoke_wasm"     
            else:
                actual_edge_payload = base_payload
                rpc_method = method_func.replace("wasm:", "").split(":")[-1] if "wasm:" in method_func else "execute"

            # 최종 Edge API 파라미터 조립
            edge_params = {
                "vm_target": vm_tgt,
                "payload": actual_edge_payload,
                "context": context,
            }

            if vm_tgt == "PYTHON" and isinstance(actual_edge_payload, dict):
                edge_params["code"] = actual_edge_payload.get("code", "")
                edge_params["callables"] = actual_edge_payload.get("callables", [])
                edge_params["variables"] = actual_edge_payload.get("variables", {})

            if payload.get(PayloadKey.TIER) == "SYSTEM":
                edge_params["fuel"] = None

            rpc_request = {
                "jsonrpc": "2.0",
                "method": rpc_method, 
                "params": edge_params,
                "id": job_id
            }

            log.info(f"[FlareTunnel:_process_edge_request] ➔ Sending POST to Edge [{vm_tgt}] (job_id: {job_id}, method: {rpc_method}, timeout: {dynamic_timeout.read}s)")

            # 3. Transmit via HTTP
            response = await self.http_client.post("/", json=rpc_request, timeout=dynamic_timeout)

            elapsed_time = time.time() - start_time
            log.debug(f"[FlareTunnel:_process_edge_request] ⬅ Received HTTP {response.status_code} in {elapsed_time:.2f}s (job_id: {job_id})")
            
            # [핵심 로직] router.ts가 생성하여 삽입한 Edge-Native Hash 추출
            edge_hash = response.headers.get("X-XPHI-Canonical-Hash")
            if edge_hash:
                log.info(f"[FlareTunnel:_process_edge_request] 🔗 Captured Edge-Sealed Hash: {edge_hash}")

            response.raise_for_status()

            # 4. Parse HTTP response safely
            try:
                res_json = response.json()
            except json.JSONDecodeError:
                raise ValueError("Edge did not return valid JSON")

            broker_result = {
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: "error" not in res_json,
            }
            
            # 해시가 존재하면 결과에 주입
            if edge_hash:
                broker_result[ResultKey.EDGE_HASH] = edge_hash

            if broker_result[ResultKey.SUCCESS]:
                broker_result[ResultKey.OUTPUT] = res_json.get("result", {}).get("output", "")
                log.debug(f"[FlareTunnel:_process_edge_request] Success response parsed for job_id: {job_id}")
            else:
                err_data = res_json.get("error", {})
                broker_result[ResultKey.ERROR] = err_data.get("message", "Unknown Edge Error")
                log.debug(f"[FlareTunnel:_process_edge_request] Error response parsed for job_id: {job_id} | Error: {broker_result[ResultKey.ERROR]}")

            # 5. Inject into queue
            await self._put_success(broker_result)

        except httpx.HTTPStatusError as e:
            elapsed_time = time.time() - start_time
            err_msg = f"Edge HTTP {e.response.status_code}: {e.response.text}"
            if e.response.status_code in (502, 503):
                err_msg = "Kinetic Trap: Edge returned 502 Bad Gateway (CPU Limit Exceeded or Crashed)"
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.ReadTimeout:
            elapsed_time = time.time() - start_time
            err_msg = f"Kinetic Trap: Edge Execution Read Timeout (V8 Isolate Hang)"
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.ConnectError:
            elapsed_time = time.time() - start_time
            err_msg = "Connection Refused: Local Edge Server (Wrangler) is unreachable."
            log.error(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.RemoteProtocolError:
            elapsed_time = time.time() - start_time
            err_msg = "Kinetic Trap: Edge Connection Severed Forcefully (V8 Silent Abort)"
            log.warning(f"[FlareTunnel:_process_edge_request] {err_msg} [{job_id}] (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, err_msg)

        except httpx.RequestError as e:
            elapsed_time = time.time() - start_time
            log.error(f"[FlareTunnel:_process_edge_request] Network Error [{job_id}]: {type(e).__name__} - {str(e)} (after {elapsed_time:.2f}s)")
            await self._put_error(job_id, f"Network Error: {str(e)}")

        except Exception as e:
            elapsed_time = time.time() - start_time
            log.error(f"[FlareTunnel:_process_edge_request] Internal Error [{job_id}]: {e} (after {elapsed_time:.2f}s)", exc_info=True)
            await self._put_error(job_id, str(e))

    async def _put_success(self, broker_result: dict):
        log.debug(f"[FlareTunnel:_put_success] Putting success result into queue for job_id: {broker_result.get(PayloadKey.JOB_ID)}")
        msg = {
            'type': 'message',
            'data': json.dumps(broker_result).encode('utf-8')
        }
        await self.q.put(msg)

    async def _put_error(self, job_id: str, error_msg: str):
        log.debug(f"[FlareTunnel:_put_error] Putting error result into queue for job_id: {job_id}")
        msg = {
            'type': 'message',
            'data': json.dumps({
                PayloadKey.JOB_ID: job_id,
                ResultKey.SUCCESS: False,
                ResultKey.ERROR: error_msg
            }).encode('utf-8')
        }
        await self.q.put(msg)

    async def aclose(self):
        # 실행 중인 모든 백그라운드 태스크 취소 및 대기 (Safe Shutdown)
        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

    async def close(self):
        await self.aclose()


class FlareFacade:
    def __init__(self, state_url: str, mq_url: str, mq_protocol: BackendProtocol, **kwargs):
        self.mq_protocol = mq_protocol
        self.mq_url = mq_url
        self.wasm_broker = None

        pool_kwargs = {
            "timeout": httpx.Timeout(15.0, connect=5.0), # Default fallback (Dynamic timeout overrides this)
            "limits": httpx.Limits(max_keepalive_connections=0),
            "verify": False
        }
        # HTTP/1.1 강제 적용으로 로컬 Wrangler 서버와의 통신 안정성 극대화
        self.http_client = httpx.AsyncClient(base_url=self.mq_url, http1=True, http2=False, **pool_kwargs)
        self._response_queue = asyncio.Queue()
        self.state_store = _MockStateStore(self.http_client, self._response_queue)
        log.info(f"[FlareTunnel] Initialized Duck-Typed HTTP Bridge targeting Edge: {self.mq_url}")

    def pubsub(self):
        """Mock pubsub to satisfy broker's listener"""
        log.debug(f"[FlareTunnel:pubsub] Initializing mock pubsub listener")
        return _MockPubSub(self._response_queue)

    async def publish(self, channel: str, message: Any):
        """Handle Control Plane publish events to prevent Broker timeouts"""
        log.debug(f"[FlareTunnel:publish] Intercepted publish on channel: {channel}")
        try:
            if isinstance(message, bytes): message = message.decode('utf-8')
            payload = json.loads(message)
            job_id = payload.get(PayloadKey.JOB_ID)
            method_func = payload.get(PayloadKey.METHOD_FUNC)

            control_methods = {"update_policy"}
            if method_func in control_methods:
                log.debug(f"[FlareTunnel:publish] Intercepted Control Message '{method_func}'. Dropping HTTP transmission.")
                if job_id:
                    ack_msg = {
                        'type': 'message',
                        'data': json.dumps({
                            PayloadKey.JOB_ID: job_id,
                            ResultKey.SUCCESS: True,
                            ResultKey.OUTPUT: f"Policy '{method_func}' mocked successfully at edge boundary"
                        }).encode('utf-8')
                    }
                    await self._response_queue.put(ack_msg)
                return 1

            # 일반 실행 요청(메서드가 있는 경우)은 정상적으로 백그라운드 전송
            if method_func:
                task = asyncio.create_task(self.state_store._process_edge_request(job_id, payload))
                self.state_store._background_tasks.add(task)
                task.add_done_callback(self.state_store._background_tasks.discard)
            else:
                # payload에 method_func조차 없는 단순 이벤트 브로드캐스트의 경우
                if job_id:
                    log.debug(f"[FlareTunnel:publish] Mocking successful ACK for empty method publish event (job_id: {job_id})")
                    ack_msg = {
                        'type': 'message',
                        'data': json.dumps({
                            PayloadKey.JOB_ID: job_id,
                            ResultKey.SUCCESS: True,
                            ResultKey.OUTPUT: "Event synchronized at edge boundary"
                        }).encode('utf-8')
                    }
                    await self._response_queue.put(ack_msg)
                    
        except Exception as e:
            log.error(f"[FlareTunnel:publish] Exception processing publish mock: {e}")
        return 1

    def bind_wasm_broker(self, broker):
        self.wasm_broker = broker

    async def stream_produce(self, *args, **kwargs): return ""
    async def stream_consume(self, *args, **kwargs): return []
    async def stream_ack(self, *args, **kwargs): return 1

    async def close(self):
        log.debug(f"[FlareTunnel:close] Closing FlareFacade and HTTP client")
        await self.state_store.aclose()
        await self.http_client.aclose()


class FlareTunnelFactory:
    _async_instance: Optional[FlareFacade] = None

    @classmethod
    async def get_default(cls, **kwargs) -> FlareFacade:
        if cls._async_instance is None:
            config = resolve_default_config()
            scheme, state_url, mq_url = parse_connection_urls(config.default_url)

            mq_url = kwargs.pop("mq_url", mq_url)
            state_url = kwargs.pop("state_url", state_url)
            scheme = kwargs.pop("mq_protocol", scheme)

            cls._async_instance = FlareFacade(state_url, mq_url, scheme, **kwargs)
            log.info(f"[FlareTunnelFactory] Provisioned Async Flare Tunnel: {mq_url}")
        return cls._async_instance

    @classmethod
    async def get_isolated(cls, **kwargs) -> FlareFacade:
        return await cls.get_default(**kwargs)

    @classmethod
    async def get_provenant(cls, wasm_broker, **kwargs) -> FlareFacade:
        tunnel = await cls.get_default(**kwargs)
        tunnel.bind_wasm_broker(wasm_broker)
        return tunnel

    @classmethod
    async def close_all(cls):
        if cls._async_instance:
            await cls._async_instance.close()
            cls._async_instance = None