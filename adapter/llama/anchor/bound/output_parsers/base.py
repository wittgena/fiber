# adapter.llama.anchor.bound.output_parsers.base
## @lineage: llama.anchor.bound.output_parsers.base
## @lineage: anchor.inter.bound.output_parsers.base
## @lineage: bound.adapter.llama.output_parsers.base
## @lineage: xphi.adapter.llama.output_parsers.base
## @lineage: bound.adapter.output_parsers.base
## @lineage: anchor.adapter.output_parsers.base
"""Base output parser class."""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class StructuredOutput:
    """Structured output class."""

    raw_output: str
    parsed_output: Optional[Any] = None


class OutputParserException(Exception):
    pass
