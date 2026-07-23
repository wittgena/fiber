# atoa.gov.tool.git.utils
## @lineage: gov.sandbox.engine.tool.git.utils
## @lineage: agent.engine.tool.git.utils
## @lineage: sandbox.tool.git.utils
## @lineage: ops.xor.tool.git.utils
## @lineage: meta.xor.tool.git.utils
## @lineage: gov.engine.tool.git.utils
## @lineage: gov.engine.executor.tool.git.utils
## @lineage: gov.executor.tool.git.utils
import re
import shlex
import subprocess
from pathlib import Path

from atoa.gov.tool.git.exceptions import GitCommandError, GitRepositoryError
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

GIT_EMPTY_TREE_HASH = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

def run_git_command(
    args: list[str],
    cwd: str | Path | None = None,
    timeout: int = 30,
) -> str:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )

        if result.returncode != 0:
            cmd_str = shlex.join(args)
            error_msg = f"Git command failed: {cmd_str}"
            log.error(
                f"{error_msg}. Exit code: {result.returncode}. Stderr: {result.stderr}"
            )
            raise GitCommandError(
                message=error_msg,
                command=args,
                exit_code=result.returncode,
                stderr=result.stderr.strip(),
            )

        log.debug(f"Git command succeeded: {shlex.join(args)}")
        return result.stdout.strip()

    except subprocess.TimeoutExpired as e:
        cmd_str = shlex.join(args)
        error_msg = f"Git command timed out: {cmd_str}"
        log.error(error_msg)
        raise GitCommandError(
            message=error_msg,
            command=args,
            exit_code=-1,
            stderr="Command timed out",
        ) from e
    except FileNotFoundError as e:
        error_msg = "Git command not found. Is git installed?"
        log.error(error_msg)
        raise GitCommandError(
            message=error_msg,
            command=args,
            exit_code=-1,
            stderr="Git executable not found",
        ) from e


def _repo_has_commits(repo_dir: str | Path) -> bool:
    try:
        count = run_git_command(
            ["git", "--no-pager", "rev-list", "--count", "--all"], repo_dir
        )
        return count.strip() != "0"
    except GitCommandError:
        log.debug("Could not check commit count")
        return False


def get_valid_ref(repo_dir: str | Path) -> str | None:
    refs_to_try = []
    if not _repo_has_commits(repo_dir):
        log.debug("Repository has no commits yet, using empty tree reference")
        return GIT_EMPTY_TREE_HASH

    # Try current branch's origin
    try:
        current_branch = run_git_command(
            ["git", "--no-pager", "rev-parse", "--abbrev-ref", "HEAD"], repo_dir
        )
        if current_branch and current_branch != "HEAD":  # Not in detached HEAD state
            refs_to_try.append(f"origin/{current_branch}")
            log.debug(f"Added current branch reference: origin/{current_branch}")
    except GitCommandError:
        log.debug("Could not get current branch name")

    # Try to get default branch from remote
    try:
        remote_info = run_git_command(
            ["git", "--no-pager", "remote", "show", "origin"], repo_dir
        )
        for line in remote_info.splitlines():
            if "HEAD branch:" in line:
                default_branch = line.split(":")[-1].strip()
                if default_branch:
                    refs_to_try.append(f"origin/{default_branch}")
                    log.debug(
                        f"Added default branch reference: origin/{default_branch}"
                    )

                    # Also try merge base with default branch
                    try:
                        merge_base = run_git_command(
                            [
                                "git",
                                "--no-pager",
                                "merge-base",
                                "HEAD",
                                f"origin/{default_branch}",
                            ],
                            repo_dir,
                        )
                        if merge_base:
                            refs_to_try.append(merge_base)
                            log.debug(f"Added merge base reference: {merge_base}")
                    except GitCommandError:
                        log.debug("Could not get merge base")
                break
    except GitCommandError:
        log.debug("Could not get remote information")

    # Find the first valid reference
    for ref in refs_to_try:
        try:
            result = run_git_command(
                ["git", "--no-pager", "rev-parse", "--verify", ref], repo_dir
            )
            if result:
                log.debug(f"Using valid reference: {ref} -> {result}")
                return result
        except GitCommandError:
            log.debug(f"Reference not valid: {ref}")
            continue

    # Fallback to empty tree hash (always valid, no verification needed)
    log.debug(f"Using empty tree reference: {GIT_EMPTY_TREE_HASH}")
    return GIT_EMPTY_TREE_HASH


def validate_git_repository(repo_dir: str | Path) -> Path:
    repo_path = Path(repo_dir).resolve()
    if not repo_path.exists():
        raise GitRepositoryError(f"Directory does not exist: {repo_path}")

    if not repo_path.is_dir():
        raise GitRepositoryError(f"Path is not a directory: {repo_path}")

    # Check if it's a git repository by looking for .git directory or file
    git_dir = repo_path / ".git"
    if not git_dir.exists():
        # Maybe we're in a subdirectory, try to find the git root
        try:
            run_git_command(["git", "rev-parse", "--git-dir"], repo_path)
        except GitCommandError as e:
            raise GitRepositoryError(f"Not a git repository: {repo_path}") from e

    return repo_path

def is_git_url(source: str) -> bool:
    if source.startswith(("https://", "http://")):
        return True

    if re.match(r"^[\w.-]+@[\w.-]+:", source):
        return True

    if source.startswith("git://"):
        return True

    if source.startswith("file://"):
        return True

    return False


def normalize_git_url(url: str) -> str:
    if url.startswith(("https://", "http://")) and not url.endswith(".git"):
        url = url.rstrip("/")
        url = f"{url}.git"
    return url


def extract_repo_name(source: str) -> str:
    name = source
    for prefix in ("github:", "https://", "http://", "git://", "file://"):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            break

    # Handle SSH format: user@host:path -> path
    if "@" in name and ":" in name and "/" not in name.split(":")[0]:
        name = name.split(":", 1)[1]

    # Remove .git suffix and get last path component
    name = name.rstrip("/").removesuffix(".git")
    name = name.rsplit("/", 1)[-1]

    # Sanitize: keep alphanumeric, dash, underscore only
    name = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")

    return name[:32] if name else "repo"
