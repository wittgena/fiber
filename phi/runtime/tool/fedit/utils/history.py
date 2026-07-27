# phi.runtime.tool.fedit.utils.history
## @lineage: swarm.mesh.tool.fedit.utils.history
## @lineage: mesh.tool.fedit.utils.history
## @lineage: gov.tool.fedit.utils.history
## @lineage: eco.gov.tool.fedit.utils.history
## @lineage: atoa.gov.tool.fedit.utils.history
## @lineage: agent.gov.tool.fedit.utils.history
## @lineage: gov.sandbox.engine.tool.fedit.utils.history
## @lineage: agent.engine.tool.fedit.utils.history
## @lineage: sandbox.tool.fedit.utils.history
## @lineage: ops.xor.tool.fedit.utils.history
## @lineage: meta.xor.tool.fedit.utils.history
## @lineage: gov.engine.tool.fedit.utils.history
## @lineage: gov.engine.executor.tool.fedit.utils.history
## @lineage: gov.executor.tool.fedit.utils.history
## @lineage: gov.executor.fedit.utils.history
## @lineage: gov.sandbox.executor.fedit.utils.history
## @lineage: gov.sphere.sandbox.executor.fedit.utils.history
## @lineage: gov.medium.tool.fedit.utils.history
## @lineage: gov.bridge.tool.fedit.utils.history
## @lineage: meta.ops.tool.fedit.utils.history
## @lineage: meta.loop.tool.fedit.utils.history
## @lineage: workspace.tool.fedit.utils.history
## @lineage: xyz.workspace.tool.fedit.utils.history
## @lineage: bridge.tool.fedit.utils.history
## @lineage: foldbox.tool.fedit.utils.history
## @lineage: ator.tool.fedit.utils.history
## @lineage: ator.tools.fedit.utils.history
## @lineage: foldbox.flow.tools.fedit.utils.history
## @lineage: foldbox.tools.fedit.utils.history
## @lineage: agent.tools.fedit.utils.history
## @lineage: loop.tools.fedit.utils.history
## @lineage: tools.fedit.utils.history
## @lineage: tools.file_editor.utils.history
## @lineage: bridge.quarantine.tools.file_editor.utils.history
"""History management for file edits with disk-based storage and memory constraints."""

import logging
import tempfile
from pathlib import Path

from phi.runtime.tool.fedit.utils.file_cache import FileCache


class FileHistoryManager:
    """Manages file edit history with disk-based storage and memory constraints."""

    max_history_per_file: int
    cache: FileCache
    logger: logging.Logger

    def __init__(self, max_history_per_file: int = 5, history_dir: Path | None = None):
        """Initialize the history manager.

        Args:
            max_history_per_file: Maximum number of history entries to keep per
                file (default: 5)
            history_dir: Directory to store history files. If None, uses a temp
                directory

        Notes:
            - Each file's history is limited to the last N entries to conserve
              memory
            - The file cache is limited to prevent excessive disk usage
            - Older entries are automatically removed when limits are exceeded
        """
        self.max_history_per_file = max_history_per_file
        if history_dir is None:
            history_dir = Path(tempfile.mkdtemp(prefix="oh_editor_history_"))
        self.cache = FileCache(str(history_dir))
        self.logger = logging.getLogger(__name__)

    def _get_metadata_key(self, file_path: Path) -> str:
        return f"{file_path}.metadata"

    def _get_history_key(self, file_path: Path, counter: int) -> str:
        return f"{file_path}.{counter}"

    def add_history(self, file_path: Path, content: str):
        """Add a new history entry for a file."""
        metadata_key = self._get_metadata_key(file_path)
        metadata = self.cache.get(metadata_key, {"entries": [], "counter": 0})
        counter = metadata["counter"]

        # Add new entry
        history_key = self._get_history_key(file_path, counter)
        self.cache.set(history_key, content)

        metadata["entries"].append(counter)
        metadata["counter"] += 1

        # Keep only last N entries
        while len(metadata["entries"]) > self.max_history_per_file:
            old_counter = metadata["entries"].pop(0)
            old_history_key = self._get_history_key(file_path, old_counter)
            self.cache.delete(old_history_key)

        self.cache.set(metadata_key, metadata)

    def pop_last_history(self, file_path: Path) -> str | None:
        """Pop and return the most recent history entry for a file."""
        metadata_key = self._get_metadata_key(file_path)
        metadata = self.cache.get(metadata_key, {"entries": [], "counter": 0})
        entries = metadata["entries"]

        if not entries:
            return None

        # Pop and remove the last entry
        last_counter = entries.pop()
        history_key = self._get_history_key(file_path, last_counter)
        content = self.cache.get(history_key)

        if content is None:
            self.logger.warning(f"History entry not found for {file_path}")
        else:
            # Remove the entry from the cache
            self.cache.delete(history_key)

        # Update metadata
        metadata["entries"] = entries
        self.cache.set(metadata_key, metadata)

        return content

    def get_metadata(self, file_path: Path):
        """Get metadata for a file (for testing purposes)."""
        metadata_key = self._get_metadata_key(file_path)
        metadata = self.cache.get(metadata_key, {"entries": [], "counter": 0})
        return metadata  # Return the actual metadata, not a copy

    def clear_history(self, file_path: Path):
        """Clear history for a given file."""
        metadata_key = self._get_metadata_key(file_path)
        metadata = self.cache.get(metadata_key, {"entries": [], "counter": 0})

        # Delete all history entries
        for counter in metadata["entries"]:
            history_key = self._get_history_key(file_path, counter)
            self.cache.delete(history_key)

        # Clear metadata
        self.cache.set(metadata_key, {"entries": [], "counter": 0})

    def get_all_history(self, file_path: Path) -> list[str]:
        """Get all history entries for a file."""
        metadata_key = self._get_metadata_key(file_path)
        metadata = self.cache.get(metadata_key, {"entries": [], "counter": 0})
        entries = metadata["entries"]

        history = []
        for counter in entries:
            history_key = self._get_history_key(file_path, counter)
            content = self.cache.get(history_key)
            if content is not None:
                history.append(content)

        return history
