# fiber.dev.trace.llm.router
"""
ONNX Semantic Router Guide:
1. Discover: Find a lightweight text classification model (e.g., DeBERTa, MiniLM) on Hugging Face Hub.
2. Export: Convert and quantize using `optimum-cli export onnx --model <model_id> --task text-classification <output_dir>`.
3. Inject: Pass the generated `.onnx` path to `model_path` and target intents to `labels` during router initialization.
"""
import asyncio
import numpy as np
import onnxruntime as ort
import os
from typing import Optional, Dict, Any, List

from fiber.gateway.llm.pipeline import PipelineSlot
from fiber.llm.model.token.encoder import encode
from xphi.state.phase.channel import DuplexChannel, ChannelContext
from xphi.watcher.plane.emitter import get_emitter

router_log = get_emitter("llm.router")

class OnnxSemanticRouter(DuplexChannel):
    target_slot = PipelineSlot.PRE_TRANSLATE 

    def __init__(self, 
                 routing_rules: Dict[str, Dict[str, Any]], 
                 tool_model: str,
                 fallback_model: Optional[str] = None,
                 model_path: str = "enterprise_router_quantized_int8.onnx",
                 labels: Optional[List[str]] = None):
        super().__init__()
        router_log.info(f"Initializing ONNX Semantic Router with model: {model_path}")
        
        self.routing_rules = routing_rules
        self.tool_model = tool_model
        self.fallback_model = fallback_model
        self.max_length = 512
        self.custom_tokenizer = {"identifier": "local-intent-tokenizer"}
        
        if not os.path.exists(model_path):
            router_log.error(f"ONNX model file not found at: {model_path}")
            raise FileNotFoundError(f"ONNX model file missing: {model_path}")

        self.session = ort.InferenceSession(
            model_path, 
            providers=['CPUExecutionProvider']
        )
        self.labels = labels or ["Transform", "Knowledge_Retrieval", "Complex_Reasoning"]

    def _extract_pure_content(self, messages: list) -> str:
        extracted = []
        for msg in messages:
            content = msg.get("content", "") if isinstance(msg, dict) else getattr(msg, "content", "")
            if isinstance(content, str) and content.strip():
                extracted.append(content.strip())
        return " ".join(extracted)

    def _run_onnx_inference(self, text: str) -> dict:
        token_ids = encode(text, custom_tokenizer=self.custom_tokenizer)
        token_ids = token_ids[:self.max_length]
        
        if not token_ids:
            return {label: 0.0 for label in self.labels}
            
        input_ids = np.array([token_ids], dtype=np.int64)
        attention_mask = np.ones_like(input_ids, dtype=np.int64)
        
        outputs = self.session.run(None, {
            "input_ids": input_ids,
            "attention_mask": attention_mask
        })
        
        logits = outputs[0][0]
        exp_logits = np.exp(logits - np.max(logits))
        probs = exp_logits / exp_logits.sum()
        return {label: float(prob) for label, prob in zip(self.labels, probs)}

    def _apply_routing(self, ctx: ChannelContext, payload: dict, target_model: str):
        payload["model"] = target_model
        meta = payload.get("system_meta") or ctx.get_attr("system_meta")
        if meta:
            meta.base_model = target_model

    async def write(self, ctx: ChannelContext, payload: dict):
        original_model = payload.get("model")
        safe_fallback = self.fallback_model or original_model
        
        messages = payload.get("messages", [])
        tools_requested = bool(payload.get("tools"))
        
        if tools_requested:
            router_log.debug(f"Tools requested. Routing to tool_model: {self.tool_model}")
            self._apply_routing(ctx, payload, self.tool_model)
            return await ctx.fire_write(payload)

        prompt_text = self._extract_pure_content(messages)
        if not prompt_text:
            self._apply_routing(ctx, payload, safe_fallback)
            return await ctx.fire_write(payload)

        try:
            intent_probs = await asyncio.to_thread(self._run_onnx_inference, prompt_text)
            
            routed = False
            sorted_intents = sorted(intent_probs.items(), key=lambda x: x[1], reverse=True)
            for intent, prob in sorted_intents:
                rule = self.routing_rules.get(intent)
                if rule and prob >= rule.get("threshold", 0.5):
                    self._apply_routing(ctx, payload, rule["target"])
                    router_log.debug(f"Routed to {intent}: {rule['target']} (Prob: {prob:.2f})")
                    routed = True
                    break
            
            if not routed:
                self._apply_routing(ctx, payload, safe_fallback)
                router_log.debug(f"Below thresholds. Routed to Fallback: {safe_fallback}")
                
        except Exception as e:
            router_log.error(f"[Router Error] Inference failed: {e}. Falling back to {safe_fallback}")
            self._apply_routing(ctx, payload, safe_fallback)
            
        return await ctx.fire_write(payload)