# atoa.call.action.tool.browser

from collections.abc import Sequence
from typing import Self

from atoa.call.action.definition import ActionAnnotations, ActionDefinition
from atoa.call.action.executor import ActionExecutor
from atoa.call.action.tool.schema.browser import (
    BrowserAction,
    BrowserObservation,
    BrowserNavigateAction,
    BrowserGetContentAction,
)

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

BROWSER_NAVIGATE_DESCRIPTION = """Navigate to a URL in the browser.
This tool allows you to navigate to any web page. You can optionally open the URL in a new tab.
Parameters:
- url: The URL to navigate to (required)
- new_tab: Whether to open in a new tab (optional, default: False)
Examples:
- Navigate to Google: url="https://www.google.com"
"""

class BrowserNavigateTool(ActionDefinition[BrowserNavigateAction, BrowserObservation]):
    """Tool for browser navigation."""
    @classmethod
    def create(cls, executor: ActionExecutor[BrowserAction, BrowserObservation]) -> Sequence[Self]:
        return [
            cls(
                description=BROWSER_NAVIGATE_DESCRIPTION,
                action_type=BrowserNavigateAction,
                observation_type=BrowserObservation,
                annotations=ActionAnnotations(
                    title="browser_navigate",
                    readOnlyHint=False,
                    destructiveHint=False,
                    idempotentHint=False,
                    openWorldHint=True,
                ),
                executor=executor,
            )
        ]

BROWSER_GET_CONTENT_DESCRIPTION = """Extract the main content of the current page in clean markdown format. It has been filtered to remove noise and advertising content.
If the content was truncated and you need more information, use start_from_char parameter to continue from where truncation occurred.
"""

class BrowserGetContentTool(ActionDefinition[BrowserGetContentAction, BrowserObservation]):
    """Tool for getting page content in markdown."""
    @classmethod
    def create(cls, executor: ActionExecutor[BrowserAction, BrowserObservation]) -> Sequence[Self]:
        return [
            cls(
                description=BROWSER_GET_CONTENT_DESCRIPTION,
                action_type=BrowserGetContentAction,
                observation_type=BrowserObservation,
                annotations=ActionAnnotations(
                    title="browser_get_content",
                    readOnlyHint=True,
                    destructiveHint=False,
                    idempotentHint=True,
                    openWorldHint=True,
                ),
                executor=executor,
            )
        ]

class BrowserToolSet(ActionDefinition[BrowserAction, BrowserObservation]):
    @classmethod
    def create(
        cls,
        executor: ActionExecutor[BrowserAction, BrowserObservation]
    ) -> list[ActionDefinition[BrowserAction, BrowserObservation]]:
        """
        Create and return the list of browser tools using the provided executor.
        """
        # Each tool.create() returns a Sequence[Self], so we flatten the results
        tools: list[ActionDefinition[BrowserAction, BrowserObservation]] = []
        
        # Load the minimal Fetch & Extract tools
        for tool_class in [
            BrowserNavigateTool,
            BrowserGetContentTool,
        ]:
            tools.extend(tool_class.create(executor))
            
        return tools