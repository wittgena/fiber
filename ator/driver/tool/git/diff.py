# ator.driver.tool.git.diff
## @lineage: driver.tool.git.diff
## @lineage: ator.bound.tool.git.diff
## @lineage: eco.tool.git.diff
## @lineage: engine.tool.git.diff
## @lineage: phi.tool.git.diff
## @lineage: phi.runtime.tool.git.diff
## @lineage: swarm.mesh.tool.git.diff
## @lineage: mesh.tool.git.diff
## @lineage: gov.tool.git.diff
## @lineage: eco.gov.tool.git.diff
## @lineage: atoa.gov.tool.git.diff
## @lineage: agent.gov.tool.git.diff
## @lineage: gov.sandbox.engine.tool.git.diff
## @lineage: agent.engine.tool.git.diff
## @lineage: sandbox.tool.git.diff
## @lineage: ops.xor.tool.git.diff
## @lineage: meta.xor.tool.git.diff
## @lineage: gov.engine.tool.git.diff
## @lineage: gov.engine.executor.tool.git.diff
## @lineage: gov.executor.tool.git.diff
## @lineage: gov.executor.git.diff
import json
import logging
import os
import sys
from pathlib import Path

from ator.driver.tool.git.exceptions import (
    GitCommandError,
    GitPathError,
    GitRepositoryError,
)
from engine.xor.bridge.tool.git import GitDiff
from ator.driver.tool.git.utils import (
    get_valid_ref,
    run_git_command,
    validate_git_repository,
)


logger = logging.getLogger(__name__)


MAX_FILE_SIZE_FOR_GIT_DIFF = 1024 * 1024  # 1 Mb


def get_closest_git_repo(path: Path) -> Path | None:
    current_path = path.resolve()
    while True:
        git_path = current_path / ".git"
        if git_path.exists():  # Could be file (worktree) or directory
            logger.debug(f"Found git repository at: {current_path}")
            return current_path

        parent = current_path.parent
        if parent == current_path:  # Reached filesystem root
            logger.debug(f"No git repository found for path: {path}")
            return None
        current_path = parent


def get_git_diff(relative_file_path: str | Path) -> GitDiff:
    path = Path(os.getcwd(), relative_file_path).resolve()
    if not path.exists():
        raise GitPathError(f"File does not exist: {path}")

    try:
        file_size = os.path.getsize(path)
        if file_size > MAX_FILE_SIZE_FOR_GIT_DIFF:
            raise GitPathError(
                f"File too large for git diff: {file_size} bytes "
                f"(max: {MAX_FILE_SIZE_FOR_GIT_DIFF} bytes)"
            )
    except OSError as e:
        raise GitPathError(f"Cannot access file: {path}") from e

    # Find git repository
    closest_git_repo = get_closest_git_repo(path)
    if not closest_git_repo:
        raise GitRepositoryError(f"File is not in a git repository: {path}")

    # Validate the git repository
    validated_repo = validate_git_repository(closest_git_repo)

    current_rev = get_valid_ref(validated_repo)
    if not current_rev:
        logger.warning(f"No valid git reference found for {validated_repo}")
        return GitDiff(modified="", original="")

    # Get the relative path from the git repo root
    try:
        relative_path_from_repo = path.relative_to(validated_repo)
    except ValueError as e:
        raise GitPathError(f"File is not within git repository: {path}") from e

    # Get old content (from the ref)
    try:
        original = run_git_command(
            ["git", "show", f"{current_rev}:{relative_path_from_repo}"], validated_repo
        )
    except GitCommandError:
        logger.debug(f"No old content found for {path} at ref {current_rev}")
        original = ""

    # Get new content (current file)
    try:
        with open(path, encoding="utf-8") as f:
            modified = "\n".join(f.read().splitlines())
    except (OSError, UnicodeDecodeError) as e:
        logger.error(f"Failed to read file {path}: {e}")
        modified = ""

    logger.info(f"Generated git diff for {path}")
    return GitDiff(
        modified=modified,
        original=original,
    )


if __name__ == "__main__":
    diff = get_git_diff(sys.argv[-1])
    print(json.dumps(diff))
