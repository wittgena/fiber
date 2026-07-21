# adapter.mapper.inter.llm
## @lineage: bound.adapter.mapper.inter.llm
from typing import Dict, Any, Set, List, Optional
from dataclasses import dataclass, field
from adapter.mapper.inter.project import ProjectLayout

@dataclass
class LLMCapabilities:
    is_function_calling: bool = False
    is_openai_like: bool = False
    is_multimodal: bool = False
    supports_structured_outputs: bool = False

@dataclass
class LLMInfo:
    status: str
    type: str
    layout: Optional[ProjectLayout] = None
    tags: List[str] = field(default_factory=list)
    module: Optional[str] = None
    class_name: Optional[str] = None 
    lineage: List[str] = field(default_factory=list)
    accepted_kwargs: List[str] = field(default_factory=list)
    capabilities: Optional[LLMCapabilities] = None
    source_repo: Optional[str] = None