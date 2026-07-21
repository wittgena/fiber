# atoa.disc.event.conv.log
## @lineage: agent.disc.event.conv.log
## @lineage: agent.loop.event.conv.log
## @lineage: gov.gateway.io.event.conv.log
## @lineage: gov.medium.io.event.conv.log
## @lineage: gov.io.event.conv.log
## @lineage: bound.io.event.conv.log
## @lineage: langos.io.event.conv.log
## @lineage: ator.flow.event.conv.log
## @lineage: ator.event.conv.log
## @lineage: agent.event.conv.log
## @lineage: bound.event.conv.log
## @lineage: bound.event.llm.completion_log
from pydantic import Field
from eco.call.event.base import Event
from eco.call.event.types import SourceType

class LLMCompletionLogEvent(Event):
    source: SourceType = "environment"
    filename: str = Field(
        ...,
        description="The intended filename for this log (relative to log directory)",
    )
    log_data: str = Field(
        ...,
        description="The JSON-encoded log data to be written to the file",
    )
    model_name: str = Field(
        default="unknown",
        description="The model name for context",
    )
    usage_id: str = Field(
        default="default",
        description="The LLM usage_id that produced this log",
    )

    def __str__(self) -> str:
        return (
            f"LLMCompletionLog(usage_id={self.usage_id}, model={self.model_name}, "
            f"file={self.filename})"
        )
