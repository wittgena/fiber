# dphi.model.inter.embedding
## @lineage: agent.llm.router.inter.embedding
## @lineage: bound.xor.model.router.inter.embedding
## @lineage: eco.model.router.inter.embedding
## @lineage: engine.router.inter.embedding
from typing import List, Union

from fiber.llm.execution import EmbeddingContext
from fiber.dphi.model.inter.adapter import BaseProviderAdapter
from fiber.dphi.model.registry.embedding import EmbeddingRouter

class InterEmbeddingAdapter(BaseProviderAdapter):
    def __init__(self):
        self.router = EmbeddingRouter()

    async def execute(self, ctx: EmbeddingContext):
        llama_kwargs = {
            "api_key": ctx.api_key,
            "api_base": ctx.api_base,
            "timeout": ctx.timeout if isinstance(ctx.timeout, (int, float)) else 60.0,
        }
        
        for k, v in ctx.optional_params.items():
            if k not in llama_kwargs:
                llama_kwargs[k] = v

        llama_kwargs = {k: v for k, v in llama_kwargs.items() if v is not None}

        try:
            embed_model = self.router.route_and_load(model_name=ctx.model, **llama_kwargs)
        except Exception as e:
            raise RuntimeError(f"[LlamaBridge] Embedding 모델 인스턴스 생성 실패: {e}")

        raw_inputs: Union[str, List[str]] = ctx.input
        texts = raw_inputs if isinstance(raw_inputs, list) else [raw_inputs]

        if getattr(ctx, "aembedding", False):
            embeddings = await embed_model.aget_text_embedding_batch(texts)
        else:
            embeddings = embed_model.get_text_embedding_batch(texts)

        data_objects = []
        for idx, emb in enumerate(embeddings):
            data_objects.append({
                "object": "embedding",
                "index": idx,
                "embedding": emb
            })

        if hasattr(ctx, "model_response"):
            setattr(ctx.model_response, "data", data_objects)
            setattr(ctx.model_response, "model", ctx.model)
            setattr(ctx.model_response, "object", "list")
            setattr(ctx.model_response, "usage", {"prompt_tokens": -1, "total_tokens": -1})
            return ctx.model_response
        else:
            return {
                "object": "list",
                "data": data_objects,
                "model": ctx.model,
                "usage": {"prompt_tokens": -1, "total_tokens": -1}
            }