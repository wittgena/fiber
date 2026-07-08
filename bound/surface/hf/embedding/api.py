# bound.surface.hf.embedding.api
## @lineage: anchor.surface.hf.embedding.api
import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Union

from bound.surface.hf.utils import format_query, format_text
from bound.surface.hf.embedding.pooling import Pooling
from anchor.inter.bound.base.embeddings.base import BaseEmbedding, Embedding
from anchor.inter.bound.bridge.pydantic import Field, PrivateAttr
from bound.surface.hf.bridge import HFInferenceBridge

logger = logging.getLogger(__name__)

class HuggingFaceInferenceAPIEmbedding(BaseEmbedding):
    pooling: Optional[Pooling] = Field(default=Pooling.CLS)
    query_instruction: Optional[str] = Field(default=None)
    text_instruction: Optional[str] = Field(default=None)
    model_name: Optional[str] = Field(default=None)
    token: Union[str, bool, None] = Field(default=None)
    timeout: Optional[float] = Field(default=None)
    headers: Optional[Dict[str, str]] = Field(default=None)
    cookies: Optional[Dict[str, str]] = Field(default=None)
    task: Optional[str] = Field(default=None)
    _bridge: Any = PrivateAttr()

    def _get_inference_client_kwargs(self) -> Dict[str, Any]:
        return {
            "model": self.model_name,
            "token": self.token,
            "timeout": self.timeout,
            "headers": self.headers,
            "cookies": self.cookies,
        }

    def __init__(self, **kwargs: Any) -> None:
        if kwargs.get("model_name") is None:
            task = kwargs.get("task", "")
            kwargs["model_name"] = HFInferenceBridge.get_recommended_model(task=task)
            logger.debug(
                f"Using Hugging Face's recommended model {kwargs['model_name']} given task {task}."
            )
            print(kwargs["model_name"], flush=True)

        super().__init__(**kwargs)
        
        # 💡 런타임에 Bridge 초기화
        self._bridge = HFInferenceBridge(**self._get_inference_client_kwargs())

    def validate_supported(self, task: str) -> None:
        all_models = self._bridge.list_deployed_models(frameworks="all")
        try:
            if self.model_name not in all_models[task]:
                raise ValueError(f"The Inference API service doesn't have the model {self.model_name!r} deployed.")
        except KeyError as exc:
            raise KeyError(f"Input task {task!r} not in possible tasks {list(all_models.keys())}.") from exc

    def get_model_info(self, **kwargs: Any) -> Any:
        return self._bridge.get_model_info(self.model_name, **kwargs)

    @classmethod
    def class_name(cls) -> str:
        return "HuggingFaceInferenceAPIEmbedding"

    async def _async_embed_single(self, text: str) -> Embedding:
        # 💡 Bridge를 통한 호출
        embedding = await self._bridge.afeature_extraction(text)
        if len(embedding.shape) == 1:
            return embedding.tolist()
        embedding = embedding.squeeze(axis=0)
        if len(embedding.shape) == 1:
            return embedding.tolist()
        try:
            return self.pooling(embedding).tolist()
        except TypeError as exc:
            raise ValueError(
                f"Pooling is required for {self.model_name} because it returned"
                " a > 1-D value, please specify pooling as not None."
            ) from exc

    async def _async_embed_bulk(self, texts: Sequence[str]) -> List[Embedding]:
        tasks = [self._async_embed_single(text) for text in texts]
        return await asyncio.gather(*tasks)

    def _get_query_embedding(self, query: str) -> Embedding:
        return asyncio.run(self._aget_query_embedding(query))

    def _get_text_embedding(self, text: str) -> Embedding:
        return asyncio.run(self._aget_text_embedding(text))

    def _get_text_embeddings(self, texts: List[str]) -> List[Embedding]:
        loop = asyncio.new_event_loop()
        try:
            tasks = [loop.create_task(self._aget_text_embedding(text)) for text in texts]
            loop.run_until_complete(asyncio.wait(tasks))
        finally:
            loop.close()
        return [task.result() for task in tasks]

    async def _aget_query_embedding(self, query: str) -> Embedding:
        return await self._async_embed_single(
            text=format_query(query, self.model_name, self.query_instruction)
        )

    async def _aget_text_embedding(self, text: str) -> Embedding:
        return await self._async_embed_single(
            text=format_text(text, self.model_name, self.text_instruction)
        )

    async def _aget_text_embeddings(self, texts: List[str]) -> List[Embedding]:
        return await self._async_embed_bulk(
            texts=[format_text(text, self.model_name, self.text_instruction) for text in texts]
        )