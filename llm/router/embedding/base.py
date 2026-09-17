# fiber.llm.router.embedding.base
import asyncio
import math
import uuid
from abc import abstractmethod
from collections import defaultdict
from enum import Enum
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)
from typing_extensions import Self

import numpy as np

# Pydantic Imports
from fiber.llm.mapper.pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    model_serializer,
    model_validator,
)

# Framework & Context Imports
from fiber.llm.router.manager import CallbackManager
from fiber.llm.context.cbevent import (
    CBEventType,
    EventPayload,
    EmbeddingEndEvent,
    EmbeddingStartEvent,
    SparseEmbeddingEndEvent,
    SparseEmbeddingStartEvent,
)
from fiber.llm.constants import DEFAULT_EMBED_BATCH_SIZE
from fiber.llm.types.inter.schema import BaseNode, MetadataMode, TransformComponent

from fiber.llm.router.util import get_tqdm_iterable
from fiber.llm.router.jobs import run_jobs
from fiber.llm.router.dispatcher import dispatcher


# ==========================================
# 1. Types & Enums
# ==========================================
Embedding = List[float]
SparseEmbedding = Dict[int, float]

class SimilarityMode(str, Enum):
    """Modes for similarity/distance."""
    DEFAULT = "cosine"
    DOT_PRODUCT = "dot_product"
    EUCLIDEAN = "euclidean"


# ==========================================
# 2. Dense Embedding Utilities
# ==========================================
def dense_mean_agg(embeddings: List[Embedding]) -> Embedding:
    """Mean aggregation for dense embeddings."""
    if not embeddings:
        raise ValueError("No embeddings to aggregate")
    return np.array(embeddings).mean(axis=0).tolist()


def similarity(
    embedding1: Embedding,
    embedding2: Embedding,
    mode: SimilarityMode = SimilarityMode.DEFAULT,
) -> float:
    """Get embedding similarity."""
    if mode == SimilarityMode.EUCLIDEAN:
        # Using -euclidean distance as similarity to achieve same ranking order
        return -float(np.linalg.norm(np.array(embedding1) - np.array(embedding2)))
    elif mode == SimilarityMode.DOT_PRODUCT:
        return float(np.dot(embedding1, embedding2))
    else:
        product = np.dot(embedding1, embedding2)
        norm = np.linalg.norm(embedding1) * np.linalg.norm(embedding2)
        return float(product / norm)


# ==========================================
# 3. Sparse Embedding Utilities
# ==========================================
def sparse_similarity(
    embedding1: SparseEmbedding,
    embedding2: SparseEmbedding,
) -> float:
    """Get sparse embedding similarity."""
    if not embedding1 or not embedding2:
        return 0.0

    # Use the smaller embedding as the primary iteration set
    if len(embedding1) > len(embedding2):
        embedding1, embedding2 = embedding2, embedding1

    # Precompute norms and find common indices
    norm1 = norm2 = dot_product = 0.0
    common_indices = set(embedding1.keys()) & set(embedding2.keys())

    for idx, value in embedding1.items():
        norm1 += value**2
        if idx in common_indices:
            dot_product += value * embedding2[idx]

    for value in embedding2.values():
        norm2 += value**2

    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0

    return dot_product / (math.sqrt(norm1) * math.sqrt(norm2))


def sparse_mean_agg(embeddings: List[SparseEmbedding]) -> SparseEmbedding:
    """Get mean aggregation of sparse embeddings."""
    if not embeddings:
        return {}

    sum_dict: Dict[int, float] = defaultdict(float)
    for embedding in embeddings:
        for idx, value in embedding.items():
            sum_dict[idx] += value

    return {idx: value / len(embeddings) for idx, value in sum_dict.items()}


# ==========================================
# 4. Dense BaseEmbedding Component
# ==========================================
class BaseEmbedding(TransformComponent):
    """Base class for dense embeddings."""

    model_config = ConfigDict(
        protected_namespaces=("pydantic_model_",), arbitrary_types_allowed=True
    )
    model_name: str = Field(
        default="unknown", description="The name of the embedding model."
    )
    embed_batch_size: int = Field(
        default=DEFAULT_EMBED_BATCH_SIZE,
        description="The batch size for embedding calls.",
        gt=0,
        le=2048,
    )
    callback_manager: CallbackManager = Field(
        default_factory=lambda: CallbackManager([]), exclude=True
    )
    num_workers: Optional[int] = Field(
        default=None,
        description="The number of workers to use for async embedding calls.",
    )
    # Use Any to avoid import loops
    embeddings_cache: Optional[Any] = Field(
        default=None,
        description="Cache for the embeddings: if None, the embeddings are not cached",
    )
    # Expected type: BaseRateLimiter (from llama_index.core.rate_limiter)
    rate_limiter: Optional[Any] = Field(
        default=None,
        description="Rate limiter instance to throttle API calls.",
        exclude=True,
    )

    @model_validator(mode="after")
    def check_base_embeddings_class(self) -> Self:
        from fiber.llm.router.storage.kvstore.types import BaseKVStore

        if self.callback_manager is None:
            self.callback_manager = CallbackManager([])
        if self.embeddings_cache is not None and not isinstance(
            self.embeddings_cache, BaseKVStore
        ):
            raise TypeError("embeddings_cache must be of type BaseKVStore")
        return self

    @abstractmethod
    def _get_query_embedding(self, query: str) -> Embedding:
        """Embed the input query synchronously."""

    @abstractmethod
    async def _aget_query_embedding(self, query: str) -> Embedding:
        """Embed the input query asynchronously."""

    @dispatcher.span
    def get_query_embedding(self, query: str) -> Embedding:
        """Embed the input query."""
        model_dict = self.to_dict()
        model_dict.pop("api_key", None)
        dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
        
        with self.callback_manager.event(
            CBEventType.EMBEDDING, payload={EventPayload.SERIALIZED: self.to_dict()}
        ) as event:
            if not self.embeddings_cache:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire()
                query_embedding = self._get_query_embedding(query)
            elif self.embeddings_cache is not None:
                cached_emb = self.embeddings_cache.get(
                    key=query, collection="embeddings"
                )
                if cached_emb is not None:
                    cached_key = next(iter(cached_emb.keys()))
                    query_embedding = cached_emb[cached_key]
                else:
                    if self.rate_limiter is not None:
                        self.rate_limiter.acquire()
                    query_embedding = self._get_query_embedding(query)
                    self.embeddings_cache.put(
                        key=query,
                        val={str(uuid.uuid4()): query_embedding},
                        collection="embeddings",
                    )
            event.on_end(
                payload={
                    EventPayload.CHUNKS: [query],
                    EventPayload.EMBEDDINGS: [query_embedding],
                },
            )
            
        dispatcher.event(
            EmbeddingEndEvent(chunks=[query], embeddings=[query_embedding])
        )
        return query_embedding

    @dispatcher.span
    async def aget_query_embedding(self, query: str) -> Embedding:
        """Get query embedding asynchronously."""
        model_dict = self.to_dict()
        model_dict.pop("api_key", None)
        dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
        
        with self.callback_manager.event(
            CBEventType.EMBEDDING, payload={EventPayload.SERIALIZED: self.to_dict()}
        ) as event:
            if not self.embeddings_cache:
                if self.rate_limiter is not None:
                    await self.rate_limiter.async_acquire()
                query_embedding = await self._aget_query_embedding(query)
            elif self.embeddings_cache is not None:
                cached_emb = await self.embeddings_cache.aget(
                    key=query, collection="embeddings"
                )
                if cached_emb is not None:
                    cached_key = next(iter(cached_emb.keys()))
                    query_embedding = cached_emb[cached_key]
                else:
                    if self.rate_limiter is not None:
                        await self.rate_limiter.async_acquire()
                    query_embedding = await self._aget_query_embedding(query)
                    await self.embeddings_cache.aput(
                        key=query,
                        val={str(uuid.uuid4()): query_embedding},
                        collection="embeddings",
                    )

            event.on_end(
                payload={
                    EventPayload.CHUNKS: [query],
                    EventPayload.EMBEDDINGS: [query_embedding],
                },
            )
            
        dispatcher.event(
            EmbeddingEndEvent(chunks=[query], embeddings=[query_embedding])
        )
        return query_embedding

    def get_agg_embedding_from_queries(
        self,
        queries: List[str],
        agg_fn: Optional[Callable[..., Embedding]] = None,
    ) -> Embedding:
        """Get aggregated embedding from multiple queries."""
        query_embeddings = [self.get_query_embedding(query) for query in queries]
        agg_fn = agg_fn or dense_mean_agg
        return agg_fn(query_embeddings)

    async def aget_agg_embedding_from_queries(
        self,
        queries: List[str],
        agg_fn: Optional[Callable[..., Embedding]] = None,
    ) -> Embedding:
        """Async get aggregated embedding from multiple queries."""
        query_embeddings = [await self.aget_query_embedding(query) for query in queries]
        agg_fn = agg_fn or dense_mean_agg
        return agg_fn(query_embeddings)

    @abstractmethod
    def _get_text_embedding(self, text: str) -> Embedding:
        """Embed the input text synchronously."""

    async def _aget_text_embedding(self, text: str) -> Embedding:
        """Embed the input text asynchronously."""
        return self._get_text_embedding(text)

    def _get_text_embeddings(self, texts: List[str]) -> List[Embedding]:
        return [self._get_text_embedding(text) for text in texts]

    async def _aget_text_embeddings(self, texts: List[str]) -> List[Embedding]:
        return await asyncio.gather(
            *[self._aget_text_embedding(text) for text in texts]
        )

    async def _aget_text_embeddings_rate_limited(self, texts: List[str]) -> List[Embedding]:
        if self.rate_limiter is not None:
            await self.rate_limiter.async_acquire()
        return await self._aget_text_embeddings(texts)

    def _get_text_embeddings_cached(self, texts: List[str]) -> List[Embedding]:
        if self.embeddings_cache is None:
            raise ValueError("embeddings_cache must be defined")

        embeddings: List[Optional[Embedding]] = [None for i in range(len(texts))]
        non_cached_texts: List[Tuple[int, str]] = []
        
        for i, txt in enumerate(texts):
            cached_emb = self.embeddings_cache.get(key=txt, collection="embeddings")
            if cached_emb is not None:
                cached_key = next(iter(cached_emb.keys()))
                embeddings[i] = cached_emb[cached_key]
            else:
                non_cached_texts.append((i, txt))
                
        if len(non_cached_texts) > 0:
            text_embeddings = self._get_text_embeddings([x[1] for x in non_cached_texts])
            for j, text_embedding in enumerate(text_embeddings):
                orig_i = non_cached_texts[j][0]
                embeddings[orig_i] = text_embedding
                self.embeddings_cache.put(
                    key=texts[orig_i],
                    val={str(uuid.uuid4()): text_embedding},
                    collection="embeddings",
                )
        return cast(List[Embedding], embeddings)

    async def _aget_text_embeddings_cached(self, texts: List[str]) -> List[Embedding]:
        if self.embeddings_cache is None:
            raise ValueError("embeddings_cache must be defined")

        embeddings: List[Optional[Embedding]] = [None for i in range(len(texts))]
        non_cached_texts: List[Tuple[int, str]] = []
        
        for i, txt in enumerate(texts):
            cached_emb = await self.embeddings_cache.aget(
                key=txt, collection="embeddings"
            )
            if cached_emb is not None:
                cached_key = next(iter(cached_emb.keys()))
                embeddings[i] = cached_emb[cached_key]
            else:
                non_cached_texts.append((i, txt))

        if len(non_cached_texts) > 0:
            text_embeddings = await self._aget_text_embeddings([x[1] for x in non_cached_texts])
            for j, text_embedding in enumerate(text_embeddings):
                orig_i = non_cached_texts[j][0]
                embeddings[orig_i] = text_embedding
                await self.embeddings_cache.aput(
                    key=texts[orig_i],
                    val={str(uuid.uuid4()): text_embedding},
                    collection="embeddings",
                )
        return cast(List[Embedding], embeddings)

    @dispatcher.span
    def get_text_embedding(self, text: str) -> Embedding:
        model_dict = self.to_dict()
        model_dict.pop("api_key", None)
        dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
        
        with self.callback_manager.event(
            CBEventType.EMBEDDING, payload={EventPayload.SERIALIZED: self.to_dict()}
        ) as event:
            if not self.embeddings_cache:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire()
                text_embedding = self._get_text_embedding(text)
            elif self.embeddings_cache is not None:
                cached_emb = self.embeddings_cache.get(
                    key=text, collection="embeddings"
                )
                if cached_emb is not None:
                    cached_key = next(iter(cached_emb.keys()))
                    text_embedding = cached_emb[cached_key]
                else:
                    if self.rate_limiter is not None:
                        self.rate_limiter.acquire()
                    text_embedding = self._get_text_embedding(text)
                    self.embeddings_cache.put(
                        key=text,
                        val={str(uuid.uuid4()): text_embedding},
                        collection="embeddings",
                    )

            event.on_end(
                payload={
                    EventPayload.CHUNKS: [text],
                    EventPayload.EMBEDDINGS: [text_embedding],
                }
            )
            
        dispatcher.event(
            EmbeddingEndEvent(chunks=[text], embeddings=[text_embedding])
        )
        return text_embedding

    @dispatcher.span
    async def aget_text_embedding(self, text: str) -> Embedding:
        model_dict = self.to_dict()
        model_dict.pop("api_key", None)
        dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
        
        with self.callback_manager.event(
            CBEventType.EMBEDDING, payload={EventPayload.SERIALIZED: self.to_dict()}
        ) as event:
            if not self.embeddings_cache:
                if self.rate_limiter is not None:
                    await self.rate_limiter.async_acquire()
                text_embedding = await self._aget_text_embedding(text)
            elif self.embeddings_cache is not None:
                cached_emb = await self.embeddings_cache.aget(
                    key=text, collection="embeddings"
                )
                if cached_emb is not None:
                    cached_key = next(iter(cached_emb.keys()))
                    text_embedding = cached_emb[cached_key]
                else:
                    if self.rate_limiter is not None:
                        await self.rate_limiter.async_acquire()
                    text_embedding = await self._aget_text_embedding(text)
                    await self.embeddings_cache.aput(
                        key=text,
                        val={str(uuid.uuid4()): text_embedding},
                        collection="embeddings",
                    )

            event.on_end(
                payload={
                    EventPayload.CHUNKS: [text],
                    EventPayload.EMBEDDINGS: [text_embedding],
                }
            )
            
        dispatcher.event(
            EmbeddingEndEvent(chunks=[text], embeddings=[text_embedding])
        )
        return text_embedding

    @dispatcher.span
    def get_text_embedding_batch(
        self,
        texts: List[str],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[Embedding]:
        cur_batch: List[str] = []
        result_embeddings: List[Embedding] = []

        queue_with_progress = enumerate(
            get_tqdm_iterable(texts, show_progress, "Generating embeddings")
        )

        model_dict = self.to_dict()
        model_dict.pop("api_key", None)
        
        for idx, text in queue_with_progress:
            cur_batch.append(text)
            if idx == len(texts) - 1 or len(cur_batch) == self.embed_batch_size:
                dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
                
                with self.callback_manager.event(
                    CBEventType.EMBEDDING,
                    payload={EventPayload.SERIALIZED: self.to_dict()},
                ) as event:
                    if self.rate_limiter is not None:
                        self.rate_limiter.acquire()
                    if not self.embeddings_cache:
                        embeddings = self._get_text_embeddings(cur_batch)
                    elif self.embeddings_cache is not None:
                        embeddings = self._get_text_embeddings_cached(cur_batch)
                        
                    result_embeddings.extend(embeddings)
                    event.on_end(
                        payload={
                            EventPayload.CHUNKS: cur_batch,
                            EventPayload.EMBEDDINGS: embeddings,
                        },
                    )
                    
                dispatcher.event(
                    EmbeddingEndEvent(chunks=cur_batch, embeddings=embeddings)
                )
                cur_batch = []

        return result_embeddings

    @dispatcher.span
    async def aget_text_embedding_batch(
        self,
        texts: List[str],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[Embedding]:
        num_workers = self.num_workers
        model_dict = self.to_dict()
        model_dict.pop("api_key", None)

        cur_batch: List[str] = []
        embeddings_coroutines: List[Coroutine] = []
        callback_payloads: List[Tuple[str, List[str]]] = []

        for idx, text in enumerate(texts):
            cur_batch.append(text)
            if idx == len(texts) - 1 or len(cur_batch) == self.embed_batch_size:
                dispatcher.event(EmbeddingStartEvent(model_dict=model_dict))
                
                event_id = self.callback_manager.on_event_start(
                    CBEventType.EMBEDDING,
                    payload={EventPayload.SERIALIZED: self.to_dict()},
                )
                callback_payloads.append((event_id, cur_batch))

                if not self.embeddings_cache:
                    embeddings_coroutines.append(self._aget_text_embeddings_rate_limited(cur_batch))
                elif self.embeddings_cache is not None:
                    embeddings_coroutines.append(self._aget_text_embeddings_cached(cur_batch))

                cur_batch = []

        if len(embeddings_coroutines) > 0:
            if num_workers and num_workers > 1:
                nested_embeddings = await run_jobs(
                    embeddings_coroutines,
                    show_progress=show_progress,
                    workers=self.num_workers,
                    desc="Generating embeddings",
                )
            elif show_progress:
                try:
                    from tqdm.asyncio import tqdm_asyncio
                    nested_embeddings = await tqdm_asyncio.gather(
                        *embeddings_coroutines,
                        total=len(embeddings_coroutines),
                        desc="Generating embeddings",
                    )
                except ImportError:
                    nested_embeddings = await asyncio.gather(*embeddings_coroutines)
            else:
                nested_embeddings = await asyncio.gather(*embeddings_coroutines)
        else:
            nested_embeddings = []

        result_embeddings = [
            embedding for embeddings in nested_embeddings for embedding in embeddings
        ]

        for (event_id, text_batch), embeddings in zip(callback_payloads, nested_embeddings):
            dispatcher.event(EmbeddingEndEvent(chunks=text_batch, embeddings=embeddings))
            self.callback_manager.on_event_end(
                CBEventType.EMBEDDING,
                payload={
                    EventPayload.CHUNKS: text_batch,
                    EventPayload.EMBEDDINGS: embeddings,
                },
                event_id=event_id,
            )

        return result_embeddings

    def similarity(
        self,
        embedding1: Embedding,
        embedding2: Embedding,
        mode: SimilarityMode = SimilarityMode.DEFAULT,
    ) -> float:
        return similarity(embedding1=embedding1, embedding2=embedding2, mode=mode)

    def __call__(self, nodes: Sequence[BaseNode], **kwargs: Any) -> Sequence[BaseNode]:
        embeddings = self.get_text_embedding_batch(
            [node.get_content(metadata_mode=MetadataMode.EMBED) for node in nodes],
            **kwargs,
        )
        for node, embedding in zip(nodes, embeddings):
            node.embedding = embedding
        return nodes

    async def acall(self, nodes: Sequence[BaseNode], **kwargs: Any) -> Sequence[BaseNode]:
        embeddings = await self.aget_text_embedding_batch(
            [node.get_content(metadata_mode=MetadataMode.EMBED) for node in nodes],
            **kwargs,
        )
        for node, embedding in zip(nodes, embeddings):
            node.embedding = embedding
        return nodes


# ==========================================
# 5. Sparse BaseEmbedding Component
# ==========================================
class BaseSparseEmbedding(BaseModel):
    """Base class for sparse embeddings."""

    model_config = ConfigDict(
        protected_namespaces=("pydantic_model_",), arbitrary_types_allowed=True
    )
    model_name: str = Field(
        default="unknown", description="The name of the embedding model."
    )
    embed_batch_size: int = Field(
        default=DEFAULT_EMBED_BATCH_SIZE,
        description="The batch size for embedding calls.",
        gt=0,
        le=2048,
    )
    num_workers: Optional[int] = Field(
        default=None,
        description="The number of workers to use for async embedding calls.",
    )

    @classmethod
    def class_name(cls) -> str:
        return "BaseSparseEmbedding"

    @model_serializer(mode="wrap")
    def custom_model_dump(self, handler: Any) -> Dict[str, Any]:
        data = handler(self)
        data["class_name"] = self.class_name()
        data.pop("api_key", None)
        return data

    @abstractmethod
    def _get_query_embedding(self, query: str) -> SparseEmbedding:
        """Embed the input query synchronously."""

    @abstractmethod
    async def _aget_query_embedding(self, query: str) -> SparseEmbedding:
        """Embed the input query asynchronously."""

    @dispatcher.span
    def get_query_embedding(self, query: str) -> SparseEmbedding:
        model_dict = self.model_dump()
        dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
        query_embedding = self._get_query_embedding(query)
        dispatcher.event(SparseEmbeddingEndEvent(chunks=[query], embeddings=[query_embedding]))
        return query_embedding

    @dispatcher.span
    async def aget_query_embedding(self, query: str) -> SparseEmbedding:
        model_dict = self.model_dump()
        dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
        query_embedding = await self._aget_query_embedding(query)
        dispatcher.event(SparseEmbeddingEndEvent(chunks=[query], embeddings=[query_embedding]))
        return query_embedding

    def get_agg_embedding_from_queries(
        self,
        queries: List[str],
        agg_fn: Optional[Callable[..., SparseEmbedding]] = None,
    ) -> SparseEmbedding:
        query_embeddings = [self.get_query_embedding(query) for query in queries]
        agg_fn = agg_fn or sparse_mean_agg
        return agg_fn(query_embeddings)

    async def aget_agg_embedding_from_queries(
        self,
        queries: List[str],
        agg_fn: Optional[Callable[..., SparseEmbedding]] = None,
    ) -> SparseEmbedding:
        query_embeddings = [await self.aget_query_embedding(query) for query in queries]
        agg_fn = agg_fn or sparse_mean_agg
        return agg_fn(query_embeddings)

    @abstractmethod
    def _get_text_embedding(self, text: str) -> SparseEmbedding:
        """Embed the input text synchronously."""

    @abstractmethod
    async def _aget_text_embedding(self, text: str) -> SparseEmbedding:
        """Embed the input text asynchronously."""

    def _get_text_embeddings(self, texts: List[str]) -> List[SparseEmbedding]:
        return [self._get_text_embedding(text) for text in texts]

    async def _aget_text_embeddings(self, texts: List[str]) -> List[SparseEmbedding]:
        return await asyncio.gather(
            *[self._aget_text_embedding(text) for text in texts]
        )

    @dispatcher.span
    def get_text_embedding(self, text: str) -> SparseEmbedding:
        model_dict = self.model_dump()
        dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
        text_embedding = self._get_text_embedding(text)
        dispatcher.event(SparseEmbeddingEndEvent(chunks=[text], embeddings=[text_embedding]))
        return text_embedding

    @dispatcher.span
    async def aget_text_embedding(self, text: str) -> SparseEmbedding:
        model_dict = self.model_dump()
        dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
        text_embedding = await self._aget_text_embedding(text)
        dispatcher.event(SparseEmbeddingEndEvent(chunks=[text], embeddings=[text_embedding]))
        return text_embedding

    @dispatcher.span
    def get_text_embedding_batch(
        self,
        texts: List[str],
        show_progress: bool = False,
        **kwargs: Any,
    ) -> List[SparseEmbedding]:
        cur_batch: List[str] = []
        result_embeddings: List[SparseEmbedding] = []
        queue_with_progress = enumerate(
            get_tqdm_iterable(texts, show_progress, "Generating embeddings")
        )

        model_dict = self.model_dump()
        for idx, text in queue_with_progress:
            cur_batch.append(text)
            if idx == len(texts) - 1 or len(cur_batch) == self.embed_batch_size:
                dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
                embeddings = self._get_text_embeddings(cur_batch)
                result_embeddings.extend(embeddings)
                dispatcher.event(SparseEmbeddingEndEvent(chunks=cur_batch, embeddings=embeddings))
                cur_batch = []

        return result_embeddings

    @dispatcher.span
    async def aget_text_embedding_batch(
        self, texts: List[str], show_progress: bool = False
    ) -> List[SparseEmbedding]:
        num_workers = self.num_workers
        model_dict = self.model_dump()
        cur_batch: List[str] = []
        callback_payloads: List[List[str]] = []
        embeddings_coroutines: List[Coroutine] = []
        
        for idx, text in enumerate(texts):
            cur_batch.append(text)
            if idx == len(texts) - 1 or len(cur_batch) == self.embed_batch_size:
                dispatcher.event(SparseEmbeddingStartEvent(model_dict=model_dict))
                callback_payloads.append(cur_batch)
                embeddings_coroutines.append(self._aget_text_embeddings(cur_batch))
                cur_batch = []

        if num_workers and num_workers > 1:
            nested_embeddings = await run_jobs(
                embeddings_coroutines,
                show_progress=show_progress,
                workers=self.num_workers,
                desc="Generating embeddings",
            )
        else:
            if show_progress:
                try:
                    from tqdm.asyncio import tqdm_asyncio
                    nested_embeddings = await tqdm_asyncio.gather(
                        *embeddings_coroutines,
                        total=len(embeddings_coroutines),
                        desc="Generating embeddings",
                    )
                except ImportError:
                    nested_embeddings = await asyncio.gather(*embeddings_coroutines)
            else:
                nested_embeddings = await asyncio.gather(*embeddings_coroutines)

        result_embeddings = [
            embedding for embeddings in nested_embeddings for embedding in embeddings
        ]

        for text_batch, embeddings in zip(callback_payloads, nested_embeddings):
            dispatcher.event(SparseEmbeddingEndEvent(chunks=text_batch, embeddings=embeddings))

        return result_embeddings

    def similarity(
        self,
        embedding1: SparseEmbedding,
        embedding2: SparseEmbedding,
    ) -> float:
        return sparse_similarity(embedding1, embedding2)