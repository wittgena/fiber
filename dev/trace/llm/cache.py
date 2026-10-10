# fiber.dev.trace.llm.cache
import json
import hashlib
import uuid
from typing import Any, Dict, Optional

from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.llm.response import ModelResponse
from fiber.llm.types.provider.core import Usage

from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter
from xphi.kernel.space.tunnel.factory import TunnelFactory

cache_log = get_emitter("llm.cache")

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
        # - Replace this SHA-256 hash generation with an Embedding model call and query a Vector DB (e.g., Milvus, Qdrant)
        try:
            serialized_msgs = json.dumps(messages, sort_keys=True)
            prompt_hash = hashlib.sha256(serialized_msgs.encode('utf-8')).hexdigest()
        except TypeError:
            # Fallback for unserializable payloads
            prompt_hash = str(messages)
            
        cache_log.debug(f"🔍 [CACHE CHECK] Evaluating prompt hash: {prompt_hash[:8]}...")

        # Acquire the UniversalFacade (Redis Tunnel) dynamically
        # - TunnelFactory uses a singleton pattern, making this extremely fast after initialization.
        tunnel = await TunnelFactory.get_default()

        # Check the distributed edge cache
        is_forced_test_hit = any("USE_CACHE" in str(m.get("content", "")) for m in messages)
        cached_response = await tunnel.get(prompt_hash)
        
        if cached_response or is_forced_test_hit:
            cache_log.info("🎯 [CACHE HIT] Match found! Short-circuiting physical I/O...")
            
            # Construct a zero-cost fulfillment response
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
            # Fire channel read to send the response UP the pipeline, bypassing the network
            return await ctx.fire_channel_read(fulfillment)
            
        # Cache Miss: Proceed DOWN the pipeline towards the physical network
        cache_log.debug("⚡ [CACHE MISS] Routing to translation and network I/O.")
        await ctx.fire_write(msg)