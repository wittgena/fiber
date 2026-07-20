# xor.scope.mock.embedding
## @lineage: xphi.xor.mock.embedding
## @lineage: xphi.scope.mock.embedding
## @lineage: xphi.xor.tester.mock.embedding
## @lineage: anchor.tester.mock.embedding
## @lineage: anchor.surface.testing.mock.embedding
## @lineage: anchor.testing.mock.embedding
## @lineage: anchor.switch.mock.embedding
"""
@phase: Mock Generation Boundary (Embedding)
@desc: Simulates multi-dimensional spatial vectors for testing RAG or search workflows.
"""
from typing import List, Union
from bound.surface.legacy.types import EmbeddingResponse

def create_mock_embedding(
    input_data: Union[str, List[str]], 
    model: str = "mock-text-embedding-3-small",
    vector_dimension: int = 1536
) -> EmbeddingResponse:
    """
    벡터 검색 테스트를 위한 임베딩 응답 객체를 반환합니다.
    """
    import random
    
    inputs = [input_data] if isinstance(input_data, str) else input_data
    data_list = []
    
    for idx, _ in enumerate(inputs):
        # 테스트용 무작위 난수 벡터 생성
        mock_vector = [random.uniform(-0.1, 0.1) for _ in range(vector_dimension)]
        data_list.append({
            "object": "embedding",
            "index": idx,
            "embedding": mock_vector
        })
        
    return EmbeddingResponse(
        object="list",
        data=data_list,
        model=model,
        usage={"prompt_tokens": len(inputs) * 5, "total_tokens": len(inputs) * 5}
    )