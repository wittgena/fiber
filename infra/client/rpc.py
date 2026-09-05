# fiber.infra.client.rpc
## @lineage: fiber.dphi.rpc.client
import uuid
import json
import asyncio
import logging
from typing import Dict, Any, Optional

from fastapi import HTTPException
from xphi.kernel.space.topos.tunnel.factory import TunnelFactory
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("rpc.client")

class InternalRpcClient:
    """
    Message Bus 기반의 Transport-Agnostic RPC 클라이언트.
    Gateway(Public Edge)에서 내부 Worker(Handler)를 호출할 때 사용됩니다.
    """
    def __init__(self, queue_name: str = "internal.rpc.queue"):
        self.queue_name = queue_name

    async def call(self, method: str, params: Dict[str, Any], timeout: float = 15.0) -> Dict[str, Any]:
        """
        Request-Reply 패턴의 비동기 RPC 호출 (응답을 기다림)
        """
        tunnel = await TunnelFactory.get_default()
        job_id = f"rpc_{uuid.uuid4().hex[:12]}"
        reply_channel = f"reply.{job_id}"
        
        pubsub = tunnel.pubsub()
        await pubsub.subscribe(reply_channel)
        
        try:
            # RPC Payload 구성
            rpc_payload = json.dumps({
                "id": job_id,
                "method": method,
                "params": params,
                "reply_to": reply_channel
            })
            
            # Message Bus(Worker Queue)에 태스크 주입
            log.debug(f"[RPC Request] {method} ({job_id})")
            await tunnel.stream_produce(self.queue_name, {"payload": rpc_payload})
            
            # 지정된 채널에서 응답 대기
            async with asyncio.timeout(timeout):
                async for msg in pubsub.listen():
                    if msg and msg["type"] == "message":
                        response = json.loads(msg["data"])
                        
                        # Worker에서 반환한 RPC 에러를 표준 HTTP Exception으로 자동 변환
                        err_data = response.get("error")
                        if err_data:
                            code = err_data.get("code", 500)
                            message = err_data.get("message", "Internal RPC Error")
                            log.warning(f"[RPC Error] {method} failed: {message}")
                            raise HTTPException(status_code=code, detail=message)
                            
                        return response.get("result", {})
                        
        except asyncio.TimeoutError:
            log.error(f"[RPC Timeout] Method {method} exceeded {timeout}s.")
            raise HTTPException(
                status_code=504, 
                detail=f"Gateway Timeout: Upstream edge worker failed to respond ({method})"
            )
        except HTTPException:
            # 내부에서 이미 raise 된 HTTPException은 그대로 패스스루
            raise
        except Exception as e:
            log.error(f"[RPC Exception] Method {method} crashed: {e}")
            raise HTTPException(status_code=500, detail="Internal Edge Communication Error")
        finally:
            await pubsub.unsubscribe(reply_channel)
            await pubsub.close()

    async def publish_intent(self, channel: str, payload: Dict[str, Any]):
        """
        응답을 기다리지 않고, 특정 타겟(예: Connector Sidecar)의 구독 채널로 
        비동기 이벤트를 쏘고 즉시 리턴하는 Fire-and-forget 큐잉 메서드.
        """
        tunnel = await TunnelFactory.get_default()
        try:
            # Pub/Sub 브로드캐스트로 전송
            await tunnel.publish(channel, json.dumps(payload))
            log.debug(f"[RPC Broadcast] Sent intent to {channel}")
        except Exception as e:
            log.error(f"[RPC Broadcast Error] Failed to publish to {channel}: {e}")
            raise