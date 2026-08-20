# bound.xor.bridge.mark.truncate
## @lineage: bound.eco.xor.bridge.mark.truncate
## @lineage: eco.bound.xor.bridge.mark.truncate
## @lineage: engine.xor.bridge.mark.truncate
## @lineage: xor.bridge.mark.truncate
## @lineage: xor.mark.truncate
## @lineage: arch.xor.bridge.mark.truncate
## @lineage: arch.xor.xe.truncate
## @lineage: watcher.xe.residue.truncate
## @lineage: atoa.mesh.residue.truncate
import hashlib
from pathlib import Path
from watcher.plane.emitter import get_logger

logger = get_logger(__name__)

DEFAULT_TEXT_CONTENT_LIMIT = 50_000
DEFAULT_TRUNCATE_NOTICE = (
    "<response clipped><NOTE>Due to the max output limit, only part of the full "
    "response has been shown to you.</NOTE>"
)

DEFAULT_TRUNCATE_NOTICE_WITH_PERSIST = (
    "<response clipped><NOTE>Due to the max output limit, only part of the full "
    "response has been shown to you. The complete output has been saved to "
    "{file_path} - you can use other tools to view the full content (truncated "
    "part starts around line {line_num}).</NOTE>"
)

def _save_full_content(content: str, save_dir: str, tool_prefix: str) -> str | None:
    """Save full content to the specified directory and return the file path."""

    save_dir_path = Path(save_dir)
    save_dir_path.mkdir(parents=True, exist_ok=True)
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:8]
    filename = f"{tool_prefix}_output_{content_hash}.txt"
    file_path = save_dir_path / filename
    if not file_path.exists():
        try:
            file_path.write_text(content, encoding="utf-8")
        except Exception as e:
            logger.debug(f"Failed to save full content to {file_path}: {e}")
            return None

    return str(file_path)


def maybe_truncate(
    content: str,
    truncate_after: int | None = None,
    truncate_notice: str = DEFAULT_TRUNCATE_NOTICE,
    save_dir: str | None = None,
    tool_prefix: str = "output",
) -> str:
    """Truncate the middle of content if it exceeds the specified length"""
    if not truncate_after or len(content) <= truncate_after or truncate_after < 0:
        return content

    if len(truncate_notice) >= truncate_after:
        return truncate_notice[:truncate_after]

    available_chars = truncate_after - len(truncate_notice)
    proposed_head = available_chars // 2 + (available_chars % 2)
    final_notice = truncate_notice
    if save_dir:
        saved_file_path = _save_full_content(content, save_dir, tool_prefix)
        if saved_file_path:
            head_content_lines = len(content[:proposed_head].splitlines())
            final_notice = DEFAULT_TRUNCATE_NOTICE_WITH_PERSIST.format(
                file_path=saved_file_path,
                line_num=head_content_lines + 1,  # +1 to indicate next line
            )

    if len(final_notice) >= truncate_after:
        return final_notice[:truncate_after]

    remaining = truncate_after - len(final_notice)
    head_chars = min(proposed_head, remaining)
    tail_chars = remaining - head_chars
    return (
        content[:head_chars]
        + final_notice
        + (content[-tail_chars:] if tail_chars > 0 else "")
    )
