# eco.gov.tool.browser.server
## @lineage: atoa.gov.tool.browser.server
## @lineage: agent.gov.tool.browser.server
## @lineage: gov.sandbox.engine.tool.browser.server
## @lineage: agent.engine.tool.browser.server
import re
from typing import Tuple, Dict, Optional
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)


def extract_clean_markdown(html_content: str, extract_links: bool = False) -> Tuple[str, Dict[str, int]]:
    try:
        original_html_chars = len(html_content)
        soup = BeautifulSoup(html_content, "html.parser")
        for tag in soup(["script", "style", "noscript", "meta", "header", "footer", "nav", "aside", "svg"]):
            tag.decompose()

        clean_html = str(soup)
        strip_tags = None if extract_links else ['a']
        raw_markdown = md(clean_html, strip=strip_tags, heading_style="ATX")
        
        initial_markdown_chars = len(raw_markdown)
        filtered_markdown = re.sub(r'\n{3,}', '\n\n', raw_markdown).strip()
        final_filtered_chars = len(filtered_markdown)
        
        content_stats = {
            "original_html_chars": original_html_chars,
            "initial_markdown_chars": initial_markdown_chars,
            "filtered_chars_removed": initial_markdown_chars - final_filtered_chars,
            "final_filtered_chars": final_filtered_chars,
        }

        return filtered_markdown, content_stats
    except Exception as e:
        log.error(f"Markdown extraction failed: {e}")
        return "", {
            "original_html_chars": 0, 
            "initial_markdown_chars": 0, 
            "filtered_chars_removed": 0, 
            "final_filtered_chars": 0
        }


class BrowserServer:
    def __init__(self, session_timeout_minutes: int = 10):
        self._session_timeout_ms = session_timeout_minutes * 60 * 1000
        self._inject_scripts: list[str] = []
        
        # Playwright internals
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

    def set_inject_scripts(self, scripts: list[str]) -> None:
        self._inject_scripts = scripts

    async def _init_browser_session(self, headless: bool = True, executable_path: str = None, **kwargs) -> None:
        if self._playwright is not None:
            return  # Already initialized

        try:
            self._playwright = await async_playwright().start()
            launch_args = {
                "headless": headless,
                "args": ["--disable-blink-features=AutomationControlled"]
            }
            if executable_path:
                launch_args["executable_path"] = executable_path
            
            # Handle root/sandbox configurations passed from executor
            if not kwargs.get("chromium_sandbox", True):
                launch_args["args"].append("--no-sandbox")

            self._browser = await self._playwright.chromium.launch(**launch_args)
            self._context = await self._browser.new_context(
                viewport={"width": 1280, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            self._context.set_default_timeout(self._session_timeout_ms)
            
            # Open an initial page
            self._page = await self._context.new_page()
            log.info(f"Playwright browser session initialized (Headless: {headless})")
        except Exception as e:
            # 에러 발생 시 띄우다 만 브라우저 및 Playwright 인스턴스 강제 정리 (좀비 프로세스 방지)
            log.exception(f"Failed to initialize Playwright browser: {e}")
            await self._close_browser()
            raise

    async def _inject_scripts_to_session(self) -> None:
        if not self._context or not self._inject_scripts:
            return

        try:
            for script in self._inject_scripts:
                # Playwright's native way to inject scripts on every new document
                await self._context.add_init_script(script)
            log.info(f"Registered {len(self._inject_scripts)} init script(s) for the browser context")
        except Exception as e:
            log.warning(f"Failed to register init scripts: {e}")

    async def _navigate(self, url: str, new_tab: bool = False) -> str:
        if not self._context:
            return "Error: Browser context not initialized."

        try:
            # 새 탭을 열어야 하거나 페이지가 닫혀있다면
            if new_tab or not self._page or self._page.is_closed():
                old_page = self._page
                self._page = await self._context.new_page()
                # 기존 탭이 살아있다면 리소스 정리를 위해 닫아줌 (메모리 누수 방지)
                if old_page and not old_page.is_closed():
                    await old_page.close()
            
            # 최신 SPA 사이트 대응을 위해 networkidle 사용
            await self._page.goto(url, wait_until="networkidle", timeout=30000)
            return f"Successfully navigated to {url}"
        except Exception as e:
            log.error(f"Navigation to {url} failed: {e}")
            return f"Failed to navigate to {url}: {e}"

    async def _get_content(self, extract_links: bool = False, start_from_char: int = 0) -> str:
        MAX_CHAR_LIMIT = 30000

        if not self._page or self._page.is_closed():
            return "Error: No active page available."

        try:
            html_content = await self._page.content()
            content, content_stats = extract_clean_markdown(
                html_content=html_content, extract_links=extract_links
            )
        except Exception as e:
            log.exception("Error extracting page content", exc_info=e)
            return f"Could not extract clean markdown: {type(e).__name__}"

        final_filtered_length = content_stats["final_filtered_chars"]

        if start_from_char > 0:
            if start_from_char >= len(content):
                return f"start_from_char ({start_from_char}) exceeds content length ({len(content)})."
            content = content[start_from_char:]
            content_stats["started_from_char"] = start_from_char

        # Smart truncation with context preservation
        truncated = False
        if len(content) > MAX_CHAR_LIMIT:
            truncate_at = MAX_CHAR_LIMIT
            paragraph_break = content.rfind("\n\n", MAX_CHAR_LIMIT - 500, MAX_CHAR_LIMIT)
            if paragraph_break > 0:
                truncate_at = paragraph_break
            else:
                sentence_break = content.rfind(".", MAX_CHAR_LIMIT - 200, MAX_CHAR_LIMIT)
                if sentence_break > 0:
                    truncate_at = sentence_break + 1

            content = content[:truncate_at]
            truncated = True
            content_stats["next_start_char"] = (start_from_char or 0) + truncate_at

        # Build stats summary
        stats_summary = (
            f"Content processed: {content_stats['original_html_chars']:,} HTML chars "
            f"→ {final_filtered_length:,} filtered markdown chars"
        )
        if start_from_char > 0:
            stats_summary += f" (started from char {start_from_char:,})"
        if truncated:
            stats_summary += f" → {len(content):,} final chars (truncated, use start_from_char={content_stats['next_start_char']} to continue)"
        elif content_stats["filtered_chars_removed"] > 0:
            stats_summary += f" (filtered {content_stats['filtered_chars_removed']:,} chars of noise)"

        current_url = self._page.url

        return f"""<url>
{current_url}
</url>
<content>
<content_stats>
{stats_summary}
</content_stats>

<webpage_content>
{content}
</webpage_content>
</content>"""

    async def _close_browser(self) -> str:
        """Close the browser session and clean up Playwright resources."""
        try:
            if self._context:
                await self._context.close()
                self._context = None
            if self._browser:
                await self._browser.close()
                self._browser = None
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            
            log.info("Playwright browser session closed gracefully.")
            return "Browser closed successfully."
        except Exception as e:
            log.warning(f"Error during browser closure: {e}")
            return f"Error closing browser: {e}"

    async def _close_all_sessions(self) -> str:
        """Alias for compatibility with executor cleanup logic."""
        return await self._close_browser()