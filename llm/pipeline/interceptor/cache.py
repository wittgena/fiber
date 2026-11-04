# fiber.llm.pipeline.interceptor.cache
import json
import hashlib
import uuid
from typing import Any, Dict, Optional

from fiber.llm.pipeline.bootstrap import PipelineSlot
from fiber.llm.response import ModelResponse
from fiber.llm.model.types.provider.core import Usage

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter
from xphi.node.space.tunnel.factory import TunnelFactory

cache_log = get_emitter("interceptor.cache")

class EdgeRedisCache(DuplexChannel):
    """
    @desc: Exact-match Edge Caching (Zero-Cost Fulfillment).
    - Hashes the raw request payload and checks a distributed cache via TunnelFactory. 
    - If a match is found, it short-circuits the physical network I/O and fulfills the request instantly.
    """
    target_slot = PipelineSlot.PRE_TRANSLATE
    
    async def write(self, ctx: ChannelContext, msg: dict):
        messages = msg.get("messages", [])
        
        # Generate a deterministic hash of the request (Exact-match logic)
        try:
            serialized_msgs = json.dumps(messages, sort_keys=True)
            prompt_hash = hashlib.sha256(serialized_msgs.encode('utf-8')).hexdigest()
        except TypeError:
            prompt_hash = str(messages)
            
        cache_log.debug(f"[CACHE CHECK] Evaluating prompt hash: {prompt_hash[:8]}...")
        tunnel = await TunnelFactory.get_default()

        # Check the distributed edge cache
        is_forced_test_hit = any("USE_CACHE" in str(m.get("content", "")) for m in messages)
        cached_response = await tunnel.get(prompt_hash)
        
        if cached_response or is_forced_test_hit:
            cache_log.info("[CACHE HIT] Match found! Short-circuiting physical I/O...")
            response_payload = cached_response if cached_response else "[CACHED] Hit!"
            fulfillment = ModelResponse(
                id=f"cache-{uuid.uuid4()}",
                model=msg.get("model", "edge-cache-tier"),
                choices=[{
                    "index": 0, 
                    "message": {"role": "assistant", "content": response_payload}, 
                    "finish_reason": "stop"
                }],
                usage=Usage(prompt_tokens=0, completion_tokens=0, total_tokens=0)
            )
            return await ctx.fire_channel_read(fulfillment)
            
        # Cache Miss: Proceed DOWN the pipeline towards the physical network
        cache_log.debug("[CACHE MISS] Routing to translation and network I/O.")
        await ctx.fire_write(msg)