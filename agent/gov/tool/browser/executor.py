# agent.gov.tool.browser.executor
## @lineage: atoa.gov.tool.browser.executor
## @lineage: gov.sandbox.engine.tool.browser.executor
## @lineage: agent.engine.tool.browser.executor
from __future__ import annotations

import builtins
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

from func_timeout import func_timeout, FunctionTimedOut

from agent.atoa.action.executor import ActionExecutor
from agent.atoa.action.tool.schema.browser import (
    BrowserAction,
    BrowserGetContentAction,
    BrowserNavigateAction,
    BrowserObservation,
)
from agent.atoa.command import sanitized_env
from agent.executor.base import AsyncExecutor
from watcher.plane.emitter import get_logger
from agent.gov.tool.browser.server import BrowserServer 

if TYPE_CHECKING:
    from agent.atoa.disc.base.conv import ToolExecutionContextProtocol

logger = get_logger(__name__)
DEFAULT_BROWSER_ACTION_TIMEOUT_SECONDS = 300.0


class ToolTimeoutError(Exception):
    pass


def run_with_timeout(func, timeout, *args, **kwargs):
    try:
        return func_timeout(timeout, func, args=args, kwargs=kwargs)
    except FunctionTimedOut:
        raise ToolTimeoutError(f"Operation timed out after {timeout} seconds")


def _format_browser_operation_error(
    error: BaseException, timeout_seconds: float | None = None
) -> str:
    if error_detail := str(error).strip():
        pass
    elif isinstance(error, builtins.TimeoutError):
        error_detail = (
            f"Operation timed out after {int(timeout_seconds)} seconds"
            if timeout_seconds is not None
            else "Operation timed out"
        )
    else:
        error_detail = error.__class__.__name__
    return f"Browser operation failed: {error_detail}"


def _install_chromium() -> bool:
    """Attempt to install Chromium via uvx playwright install."""
    try:
        if not shutil.which("uvx"):
            logger.warning("uvx not found - cannot auto-install Chromium")
            return False

        logger.info("Attempting to install Chromium via uvx...")
        result = subprocess.run(
            ["uvx", "playwright", "install", "chromium", "--with-deps", "--no-shell"],
            capture_output=True,
            text=True,
            timeout=300,
            env=sanitized_env(),
        )

        if result.returncode == 0:
            logger.info("Chromium installation completed successfully")
            return True
        else:
            logger.error(f"Chromium installation failed: {result.stderr}")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        logger.error(f"Error during Chromium installation: {e}")
        return False


def _get_chromium_error_message() -> str:
    return (
        "Chromium is required for browser operations but is not installed.\n\n"
        "To install Chromium, run one of the following commands:\n"
        "  1. Using uvx (recommended): uvx playwright install chromium --with-deps --no-shell\n"
        "  2. Using pip: pip install playwright && playwright install chromium\n"
        "  3. Using system package manager:\n"
        "     - Ubuntu/Debian: sudo apt install chromium-browser\n"
        "     - macOS: brew install chromium\n"
        "     - Windows: winget install Chromium.Chromium\n\n"
        "After installation, restart your application to use the browser tool."
    )


def check_chromium_available() -> str | None:
    """Check standard paths and Playwright caches for Chromium."""
    standard_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for install_path in standard_paths:
        p = Path(install_path)
        if p.exists():
            return str(p)

    playwright_cache_candidates = [
        Path.home() / ".cache" / "ms-playwright",
        Path.home() / "Library" / "Caches" / "ms-playwright",
    ]

    for playwright_cache in playwright_cache_candidates:
        if playwright_cache.exists():
            chromium_dirs = list(playwright_cache.glob("chromium-*"))
            for chromium_dir in chromium_dirs:
                possible_paths = [
                    chromium_dir / "chrome-linux" / "chrome",
                    chromium_dir / "chrome-linux64" / "chrome",
                    chromium_dir / "chrome-mac" / "Chromium.app" / "Contents" / "MacOS" / "Chromium",
                    chromium_dir / "chrome-mac-arm64" / "Google Chrome for Testing.app" / "Contents" / "MacOS" / "Google Chrome for Testing",
                    chromium_dir / "chrome-mac" / "Google Chrome for Testing.app" / "Contents" / "MacOS" / "Google Chrome for Testing",
                ]
                for p in possible_paths:
                    if p.exists():
                        return str(p)

    for binary in ("google-chrome", "chrome", "chromium", "chromium-browser"):
        if path := shutil.which(binary):
            return path

    return None


def _ensure_chromium_available() -> str:
    """Ensure Chromium is available, attempt install if missing, or raise an error."""
    # 1. 1차 확인
    if path := check_chromium_available():
        logger.info(f"Chromium is available for browser operations at {path}")
        return path

    # 2. 없다면 설치 시도 (Dead Code 살리기)
    logger.info("Chromium not found. Attempting auto-installation...")
    if _install_chromium():
        if path := check_chromium_available():
            logger.info(f"Chromium successfully auto-installed at {path}")
            return path
            
    # 3. 설치도 실패했거나 uvx가 없으면 에러 발생
    raise Exception(_get_chromium_error_message())


# ============================================
# Executor 클래스
# ============================================

class BrowserExecutor(ActionExecutor[BrowserAction, BrowserObservation]):
    """Executor that wraps browser-use MCP server for integration."""

    _server: BrowserServer
    _config: dict[str, Any]
    _initialized: bool
    _async_executor: AsyncExecutor
    _cleanup_initiated: bool
    _action_timeout_seconds: float
    full_output_save_dir: str | None

    def __init__(
        self,
        headless: bool = True,
        allowed_domains: list[str] | None = None,
        session_timeout_minutes: int = 30,
        init_timeout_seconds: int = 30,
        action_timeout_seconds: float = DEFAULT_BROWSER_ACTION_TIMEOUT_SECONDS,
        full_output_save_dir: str | None = None,
        inject_scripts: list[str] | None = None,
        **config,
    ):
        if action_timeout_seconds <= 0:
            raise ValueError("action_timeout_seconds must be greater than 0")

        self.full_output_save_dir = full_output_save_dir
        self._initialized = False
        self._async_executor = AsyncExecutor()
        self._cleanup_initiated = False
        self._action_timeout_seconds = action_timeout_seconds

        def _init_server():
            nonlocal headless
            executable_path = _ensure_chromium_available()
            
            self._server = BrowserServer(
                session_timeout_minutes=session_timeout_minutes,
            )
            
            if os.getenv("OH_ENABLE_VNC", "false").lower() in {"true", "1", "yes"}:
                headless = False
                logger.info("VNC is enabled - running browser in non-headless mode")

            if inject_scripts:
                self._server.set_inject_scripts(inject_scripts)

            running_as_root = os.getuid() == 0
            if running_as_root:
                logger.warning("Running as root - disabling Chromium sandbox.")

            self._config = {
                "headless": headless,
                "allowed_domains": allowed_domains or [],
                "executable_path": executable_path,
                "chromium_sandbox": not running_as_root,
                **config,
            }

        try:
            # 타임아웃을 적용하여 초기화 수행
            run_with_timeout(_init_server, init_timeout_seconds)
        except ToolTimeoutError:
            raise Exception(f"Browser tool initialization timed out after {init_timeout_seconds}s")

    def __call__(
        self,
        action: BrowserAction,
        conversation: ToolExecutionContextProtocol | None = None,
    ):
        try:
            return self._async_executor.run_async(
                self._execute_action,
                action,
                timeout=self._action_timeout_seconds,
            )
        except builtins.TimeoutError as error:
            return BrowserObservation.from_text(
                text=_format_browser_operation_error(
                    error, timeout_seconds=self._action_timeout_seconds
                ),
                is_error=True,
                full_output_save_dir=self.full_output_save_dir,
            )

    async def _execute_action(self, action: BrowserAction) -> BrowserObservation:
        # 중복된 내부 import 제거됨 (상단에서 import 완료)
        try:
            result = ""
            if isinstance(action, BrowserNavigateAction):
                result = await self.navigate(action.url, action.new_tab)
            elif isinstance(action, BrowserGetContentAction):
                result = await self.get_content(
                    action.extract_links, action.start_from_char
                )
            else:
                error_msg = f"Unsupported action type: {type(action)}"
                return BrowserObservation.from_text(
                    text=error_msg,
                    is_error=True,
                    full_output_save_dir=self.full_output_save_dir,
                )

            return BrowserObservation.from_text(
                text=result,
                is_error=False,
                full_output_save_dir=self.full_output_save_dir,
            )
        except Exception as error:
            error_msg = _format_browser_operation_error(error)
            logger.error(error_msg, exc_info=True)
            return BrowserObservation.from_text(
                text=error_msg,
                is_error=True,
                full_output_save_dir=self.full_output_save_dir,
            )

    async def _ensure_initialized(self):
        """Ensure browser session is initialized."""
        if not self._initialized:
            await self._server._init_browser_session(**self._config)
            await self._server._inject_scripts_to_session()
            self._initialized = True

    async def navigate(self, url: str, new_tab: bool = False) -> str:
        """Navigate to a URL."""
        await self._ensure_initialized()
        return await self._server._navigate(url, new_tab)

    async def get_content(self, extract_links: bool, start_from_char: int) -> str:
        """Extract page content, optionally with links."""
        await self._ensure_initialized()
        return await self._server._get_content(
            extract_links=extract_links, start_from_char=start_from_char
        )

    async def close_browser(self) -> str:
        """Close the browser session."""
        if self._initialized:
            result = await self._server._close_browser()
            self._initialized = False
            return result
        return "No browser session to close"

    async def cleanup(self):
        """Cleanup browser resources."""
        try:
            if hasattr(self._server, "_close_all_sessions"):
                await self._server._close_all_sessions()
            else:
                await self.close_browser()
        except Exception as e:
            logger.warning(f"Error during browser cleanup: {e}")

    def close(self):
        """Close the browser executor and cleanup resources."""
        if self._cleanup_initiated:
            return
        self._cleanup_initiated = True
        try:
            self._async_executor.run_async(self.cleanup, timeout=30.0)
        except Exception as e:
            logger.warning(f"Error during browser cleanup: {e}")
        finally:
            self._async_executor.close()

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self.close()
        except Exception:
            pass