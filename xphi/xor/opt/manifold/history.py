# xphi.xor.opt.manifold.history
## @lineage: xphi.xor.manifold.history
from typing import Any
import pydantic

class History(pydantic.BaseModel):
    messages: list[dict[str, Any]]
    model_config = pydantic.ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        validate_assignment=True,
        extra="forbid",
    )
