# agent.anchor.model.types.param.embedding
## @lineage: bound.xor.model.types.param.embedding
## @lineage: eco.model.types.param.embedding
## @lineage: engine.model.types.param.embedding
## @lineage: bound.model.types.param.embedding
## @lineage: llm.types.param.embedding
## @lineage: eco.mesh.model.types.param.embedding
## @lineage: runtime.mesh.model.types.param.embedding
## @lineage: mesh.model.types.param.embedding
## @lineage: mesh.mapper.param.embedding
## @lineage: bound.mapper.param.embedding
from typing import Dict, List, Literal, Optional
from typing_extensions import TypedDict

class VectorStoreResultContent(TypedDict, total=False):
    text: Optional[str]
    type: Optional[str]


class VectorStoreSearchResult(TypedDict, total=False):
    score: Optional[float]
    content: Optional[List[VectorStoreResultContent]]
    file_id: Optional[str]
    filename: Optional[str]
    attributes: Optional[Dict]


class VectorStoreSearchResponse(TypedDict, total=False):
    object: Literal["vector_store.search_results.page"]
    search_query: Optional[str]
    data: Optional[List[VectorStoreSearchResult]]