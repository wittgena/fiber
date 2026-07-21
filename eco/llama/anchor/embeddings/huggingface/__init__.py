# eco.llama.anchor.embeddings.huggingface.__init__
## @lineage: adapter.llama.anchor.embeddings.huggingface.__init__
## @lineage: llama.anchor.embeddings.huggingface.__init__
## @lineage: anchor.inter.embeddings.huggingface.__init__
## @lineage: bound.inter.embeddings.huggingface.__init__
## @lineage: bound.adapter.llama.embeddings.huggingface.__init__
## @lineage: bound.channel.bridge.embeddings.huggingface.__init__
## @lineage: channel.bridge.embeddings.huggingface.__init__
## @lineage: bridge.llama.embeddings.huggingface.__init__
from llama_index.embeddings.huggingface.base import (
    HuggingFaceEmbedding,
    HuggingFaceInferenceAPIEmbedding,
    HuggingFaceInferenceAPIEmbeddings,
)

__all__ = [
    "HuggingFaceEmbedding",
    "HuggingFaceInferenceAPIEmbedding",
    "HuggingFaceInferenceAPIEmbeddings",
]
