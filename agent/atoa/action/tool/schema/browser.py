# agent.atoa.action.tool.schema.browser
## @lineage: atoa.agent.action.tool.schema.browser
## @lineage: atoa.call.action.tool.schema.browser
import base64
import hashlib
import os
from collections.abc import Sequence
from pathlib import Path

from pydantic import Field

from agent.eco.action.message import ImageContent, TextContent
from agent.atoa.disc.schema.action import Action, Observation
from agent.eco.residue.truncate import DEFAULT_TEXT_CONTENT_LIMIT, maybe_truncate

BROWSER_RECORDING_OUTPUT_DIR = os.path.join(".agent_tmp", "browser_observations")

BASE64_IMAGE_PREFIXES = {
    "/9j/": "image/jpeg",
    "iVBORw0KGgo": "image/png",
    "R0lGODlh": "image/gif",
    "UklGR": "image/webp",
}

def detect_image_mime_type(base64_data: str) -> str:
    for prefix, mime_type in BASE64_IMAGE_PREFIXES.items():
        if base64_data.startswith(prefix):
            return mime_type
    return "image/png"


# ============================================
# Observation (Result Schema)
# ============================================

class BrowserObservation(Observation):
    """Base observation for browser operations."""

    screenshot_data: str | None = Field(
        default=None, 
        description="Base64 screenshot data if available"
    )
    full_output_save_dir: str | None = Field(
        default=None,
        description="Directory where full output files are saved",
    )

    def _save_screenshot(self, base64_data: str, save_dir: str) -> str | None:
        try:
            save_dir_path = Path(save_dir)
            save_dir_path.mkdir(parents=True, exist_ok=True)

            mime_type = detect_image_mime_type(base64_data)
            ext = mime_type.split("/")[-1]
            if ext == "jpeg":
                ext = "jpg"

            # Generate hash for filename
            content_hash = hashlib.sha256(base64_data.encode("utf-8")).hexdigest()[:8]
            filename = f"browser_screenshot_{content_hash}.{ext}"
            file_path = save_dir_path / filename

            if not file_path.exists():
                image_data = base64.b64decode(base64_data)
                file_path.write_bytes(image_data)

            return str(file_path)
        except Exception:
            return None

    @property
    def to_llm_content(self) -> Sequence[TextContent | ImageContent]:
        llm_content: list[TextContent | ImageContent] = []

        # If is_error is true, prepend error message
        if self.is_error:
            llm_content.append(TextContent(text=self.ERROR_MESSAGE_HEADER))

        # Get text content and truncate if needed
        content_text = self.text
        if content_text:
            llm_content.append(
                TextContent(
                    text=maybe_truncate(
                        content=content_text,
                        truncate_after=DEFAULT_TEXT_CONTENT_LIMIT,
                        save_dir=self.full_output_save_dir,
                        tool_prefix="browser",
                    )
                )
            )

        if self.screenshot_data:
            mime_type = detect_image_mime_type(self.screenshot_data)

            # Save screenshot if directory is available
            if self.full_output_save_dir:
                saved_path = self._save_screenshot(
                    self.screenshot_data, self.full_output_save_dir
                )
                if saved_path:
                    llm_content.append(
                        TextContent(text=f"Screenshot saved to: {saved_path}")
                    )

            # Convert base64 to data URL format for ImageContent
            data_url = f"data:{mime_type};base64,{self.screenshot_data}"
            llm_content.append(ImageContent(image_urls=[data_url]))

        return llm_content


# ============================================
# Actions (Request Schemas)
# ============================================

class BrowserAction(Action):
    """Base Action schema for all browser operations."""
    pass


class BrowserNavigateAction(BrowserAction):
    """Schema for browser navigation."""
    url: str = Field(description="The URL to navigate to")
    new_tab: bool = Field(default=False, description="Whether to open in a new tab. Default: False")


class BrowserGetContentAction(BrowserAction):
    """Schema for getting page content in markdown."""
    extract_links: bool = Field(
        default=False,
        description="Whether to include links in the content (default: False)",
    )
    start_from_char: int = Field(
        default=0,
        ge=0,
        description="Character index to start from in the page content (default: 0)",
    )