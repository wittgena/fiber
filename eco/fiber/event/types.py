# eco.fiber.event.types
## @lineage: agent.eco.event.types
## @lineage: eco.agent.event.types
## @lineage: eco.call.event.types
## @lineage: adapter.call.event.types
## @lineage: bound.adapter.call.event.types
## @lineage: agent.disc.event.types
## @lineage: gov.policy.event.types
from typing import Literal

EventType = Literal["action", "observation", "message", "system_prompt", "agent_error"]
SourceType = Literal["agent", "user", "environment", "hook", "watcher"]
EventID = str
ToolCallID = str
