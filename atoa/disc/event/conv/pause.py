# atoa.disc.event.conv.pause
## @lineage: agent.disc.event.conv.pause
## @lineage: agent.loop.event.conv.pause
## @lineage: gov.gateway.io.event.conv.pause
## @lineage: gov.medium.io.event.conv.pause
## @lineage: gov.io.event.conv.pause
## @lineage: bound.io.event.conv.pause
## @lineage: langos.io.event.conv.pause
## @lineage: ator.flow.event.conv.pause
## @lineage: ator.event.conv.pause
## @lineage: agent.event.conv.pause
## @lineage: bound.event.conv.pause
## @lineage: bound.event.user_action
from rich.text import Text
from eco.call.event.base import Event
from eco.call.event.types import SourceType

class PauseEvent(Event):
    source: SourceType = "user"

    @property
    def visualize(self) -> Text:
        content = Text()
        content.append("Conversation Paused", style="bold")
        return content

    def __str__(self) -> str:
        return f"{self.__class__.__name__} ({self.source}): Agent execution paused"
