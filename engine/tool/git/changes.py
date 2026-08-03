# engine.tool.git.changes
## @lineage: phi.tool.git.changes
## @lineage: phi.runtime.tool.git.changes
## @lineage: swarm.mesh.tool.git.changes
## @lineage: mesh.tool.git.changes
## @lineage: gov.tool.git.changes
## @lineage: eco.gov.tool.git.changes
## @lineage: atoa.gov.tool.git.changes
## @lineage: agent.gov.tool.git.changes
## @lineage: gov.sandbox.engine.tool.git.changes
## @lineage: agent.engine.tool.git.changes
## @lineage: sandbox.tool.git.changes
## @lineage: ops.xor.tool.git.changes
## @lineage: meta.xor.tool.git.changes
## @lineage: gov.engine.tool.git.changes
## @lineage: gov.engine.executor.tool.git.changes
## @lineage: gov.executor.tool.git.changes
## @lineage: gov.executor.git.changes
import glob
import json
import logging
import os
from pathlib import Path
from engine.tool.git.exceptions import GitCommandError
from arch.xor.bridge.tool.git import GitChange, GitChangeStatus
from engine.tool.git.utils import get_valid_ref, run_git_command, validate_git_repository
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

def _map_git_status_to_enum(status: str) -> GitChangeStatus:
    """Map git status codes to GitChangeStatus enum values."""
    status_mapping = {
        "M": GitChangeStatus.UPDATED,
        "A": GitChangeStatus.ADDED,
        "D": GitChangeStatus.DELETED,
        "U": GitChangeStatus.UPDATED,  # Unmerged files are treated as updated
    }
    if status not in status_mapping:
        raise ValueError(f"Unknown git status: {status}")
    return status_mapping[status]


def get_changes_in_repo(repo_dir: str | Path) -> list[GitChange]:
    validated_repo = validate_git_repository(repo_dir)
    ref = get_valid_ref(validated_repo)
    if not ref:
        log.warning(f"No valid git reference found for {validated_repo}")
        return []

    # Get changed files using secure git command
    try:
        changed_files_output = run_git_command(
            ["git", "--no-pager", "diff", "--name-status", ref], validated_repo
        )
        changed_files = (
            changed_files_output.splitlines() if changed_files_output else []
        )
    except GitCommandError as e:
        log.error(f"Failed to get git diff for {validated_repo}: {e}")
        raise
    changes = []
    for line in changed_files:
        if not line.strip():
            log.warning("Empty line in git diff output, skipping")
            continue

        parts = line.split()
        if len(parts) < 2:
            log.error(f"Unexpected git diff line format: {line}")
            raise GitCommandError(
                message=f"Unexpected git diff output format: {line}",
                command=["git", "diff", "--name-status"],
                exit_code=0,
                stderr="Invalid output format",
            )

        status = parts[0].strip()

        # Handle rename operations (status starts with 'R' followed
        # by similarity percentage)
        if status.startswith("R") and len(parts) == 3:
            # Rename: convert to delete (old path) + add (new path)
            old_path = parts[1].strip()
            new_path = parts[2].strip()
            changes.append(
                GitChange(
                    status=GitChangeStatus.DELETED,
                    path=Path(old_path),
                )
            )
            changes.append(
                GitChange(
                    status=GitChangeStatus.ADDED,
                    path=Path(new_path),
                )
            )
            log.debug(f"Found git rename: {old_path} -> {new_path}")
            continue

        # Handle copy operations (status starts with 'C' followed by
        # similarity percentage)
        elif status.startswith("C") and len(parts) == 3:
            # Copy: only add the new path (original remains)
            new_path = parts[2].strip()
            changes.append(
                GitChange(
                    status=GitChangeStatus.ADDED,
                    path=Path(new_path),
                )
            )
            log.debug(f"Found git copy: -> {new_path}")
            continue

        # Handle regular operations (M, A, D, etc.)
        elif len(parts) == 2:
            path = parts[1].strip()
        else:
            log.error(f"Unexpected git diff line format: {line}")
            raise GitCommandError(
                message=f"Unexpected git diff output format: {line}",
                command=["git", "diff", "--name-status"],
                exit_code=0,
                stderr="Invalid output format",
            )

        if status == "??":
            status = "A"
        elif status == "*":
            status = "M"

        # Check for valid single-character status codes
        if status in {"M", "A", "D", "U"}:
            try:
                changes.append(
                    GitChange(
                        status=_map_git_status_to_enum(status),
                        path=Path(path),
                    )
                )
                log.debug(f"Found git change: {status} {path}")
            except ValueError as e:
                log.error(f"Unknown git status '{status}' for file {path}")
                raise GitCommandError(
                    message=f"Unknown git status: {status}",
                    command=["git", "diff", "--name-status"],
                    exit_code=0,
                    stderr=f"Unknown status code: {status}",
                ) from e
        else:
            log.error(f"Unexpected git status '{status}' for file {path}")
            raise GitCommandError(
                message=f"Unexpected git status: {status}",
                command=["git", "diff", "--name-status"],
                exit_code=0,
                stderr=f"Unexpected status code: {status}",
            )

    # Get untracked files
    try:
        untracked_output = run_git_command(
            ["git", "--no-pager", "ls-files", "--others", "--exclude-standard"],
            validated_repo,
        )
        untracked_files = untracked_output.splitlines() if untracked_output else []
    except GitCommandError as e:
        log.error(f"Failed to get untracked files for {validated_repo}: {e}")
        untracked_files = []
    for path in untracked_files:
        if path.strip():
            changes.append(
                GitChange(
                    status=GitChangeStatus.ADDED,
                    path=Path(path.strip()),
                )
            )
            log.debug(f"Found untracked file: {path}")

    log.info(f"Found {len(changes)} total git changes in {validated_repo}")
    return changes


def get_git_changes(cwd: str | Path) -> list[GitChange]:
    git_dirs = {
        os.path.dirname(f)[2:]
        for f in glob.glob("./*/.git", root_dir=cwd, recursive=True)
    }

    changes = get_changes_in_repo(cwd)
    changes = [
        change
        for change in changes
        if next(
            iter(
                git_dir for git_dir in git_dirs if str(change.path).startswith(git_dir)
            ),
            None,
        )
        is None
    ]

    # Add changes from git directories
    for git_dir in git_dirs:
        git_dir_changes = get_changes_in_repo(str(Path(cwd, git_dir)))
        for change in git_dir_changes:
            # Create a new GitChange with the updated path
            updated_change = GitChange(
                status=change.status,
                path=Path(git_dir) / change.path,
            )
            changes.append(updated_change)

    changes.sort(key=lambda change: str(change.path))

    return changes


if __name__ == "__main__":
    try:
        changes = get_git_changes(os.getcwd())
        # Convert GitChange objects to dictionaries for JSON serialization
        changes_dict = [
            {
                "status": change.status.value,
                "path": str(change.path),
            }
            for change in changes
        ]
        print(json.dumps(changes_dict))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
