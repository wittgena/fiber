# atoa.disc.event.conv.error
## @lineage: atoa.gov.disc.event.conv.error
## @lineage: agent.atoa.disc.event.conv.error
## @lineage: atoa.agent.disc.event.conv.error
from pydantic import Field
from rich.text import Text
from eco.fiber.event.base import Event

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