# atoa.disc.event.conv.error
## @lineage: agent.disc.event.conv.error
## @lineage: agent.loop.event.conv.error
## @lineage: gov.gateway.io.event.conv.error
## @lineage: gov.medium.io.event.conv.error
## @lineage: gov.io.event.conv.error
## @lineage: bound.io.event.conv.error
## @lineage: langos.io.event.conv.error
## @lineage: ator.flow.event.conv.error
## @lineage: ator.event.conv.error
## @lineage: agent.event.conv.error
## @lineage: bound.event.conv.error
## @lineage: bridge.event.conv.error
## @lineage: foldbox.hands.subst.event.conv.error
## @lineage: foldbox.hands.core.event.conv.error
## @lineage: bridge.inter.event.conv.error
## @lineage: bridge.inter.event.conversation_error
from pydantic import Field
from rich.text import Text
from eco.call.event.base import Event

class ConversationErrorEvent(Event):
    code: str = Field(description="Code for the error - typically a type")
    detail: str = Field(description="Details about the error")

    @property
    def visualize(self) -> Text:
        content = Text()
        content.append("Conversation Error\n", style="bold")
        content.append("Code: ", style="bold")
        content.append(self.code)
        content.append("\n\nDetail:\n", style="bold")
        content.append(self.detail)
        return content
