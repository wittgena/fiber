# phi.tool.git.repo
## @lineage: phi.runtime.tool.git.repo
## @lineage: swarm.mesh.tool.git.repo
## @lineage: mesh.tool.git.repo
## @lineage: gov.tool.git.repo
## @lineage: eco.gov.tool.git.repo
## @lineage: atoa.gov.tool.git.repo
## @lineage: agent.gov.tool.git.repo
## @lineage: gov.sandbox.engine.tool.git.repo
## @lineage: agent.engine.tool.git.repo
## @lineage: sandbox.tool.git.repo
## @lineage: ops.xor.tool.git.repo
## @lineage: meta.xor.tool.git.repo
## @lineage: gov.engine.tool.git.repo
## @lineage: gov.engine.executor.tool.git.repo
## @lineage: gov.executor.tool.git.repo
## @lineage: gov.executor.git.repo
from __future__ import annotations
import shutil
from pathlib import Path
from filelock import FileLock, Timeout
from phi.tool.git.exceptions import GitCommandError
from phi.tool.git.utils import run_git_command
from watcher.plane.emitter import get_emitter

logger = get_emitter(__name__)
DEFAULT_LOCK_TIMEOUT = 30

class GitHelper:
    def clone(
        self,
        url: str,
        dest: Path,
        depth: int | None = 1,
        branch: str | None = None,
        timeout: int = 120,
    ) -> None:
        cmd = ["git", "clone"]
        if depth is not None:
            cmd.extend(["--depth", str(depth)])

        if branch:
            cmd.extend(["--branch", branch])

        cmd.extend([url, str(dest)])

        run_git_command(cmd, timeout=timeout)

    def fetch(
        self,
        repo_path: Path,
        remote: str = "origin",
        ref: str | None = None,
        timeout: int = 60,
    ) -> None:
        cmd = ["git", "fetch", remote]
        if ref:
            cmd.append(ref)

        run_git_command(cmd, cwd=repo_path, timeout=timeout)

    def checkout(self, repo_path: Path, ref: str, timeout: int = 30) -> None:
        run_git_command(["git", "checkout", ref], cwd=repo_path, timeout=timeout)

    def reset_hard(self, repo_path: Path, ref: str, timeout: int = 30) -> None:
        run_git_command(["git", "reset", "--hard", ref], cwd=repo_path, timeout=timeout)

    def get_current_branch(self, repo_path: Path, timeout: int = 10) -> str | None:
        branch = run_git_command(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_path,
            timeout=timeout,
        )
        return None if branch == "HEAD" else branch

    def get_default_branch(self, repo_path: Path, timeout: int = 10) -> str | None:
        try:
            # origin/HEAD is a symbolic ref pointing to the default branch
            ref = run_git_command(
                ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
                cwd=repo_path,
                timeout=timeout,
            )
            # Output is like "refs/remotes/origin/main" - extract branch name
            prefix = "refs/remotes/origin/"
            if ref.startswith(prefix):
                return ref[len(prefix) :]
            return None
        except GitCommandError:
            # origin/HEAD may not be set (e.g., bare clone, or never configured)
            return None

    def get_head_commit(self, repo_path: Path, timeout: int = 10) -> str:
        return run_git_command(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            timeout=timeout,
        )


def try_cached_clone_or_update(
    url: str,
    repo_path: Path,
    ref: str | None = None,
    update: bool = True,
    git_helper: GitHelper | None = None,
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
) -> Path | None:
    git = git_helper if git_helper is not None else GitHelper()
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = repo_path.with_suffix(".lock")
    lock = FileLock(lock_path)

    try:
        with lock.acquire(timeout=lock_timeout):
            return _do_clone_or_update(url, repo_path, ref, update, git)
    except Timeout:
        logger.warning(
            f"Timed out waiting for lock on {repo_path} after {lock_timeout}s"
        )
        return None
    except GitCommandError as e:
        logger.warning(f"Git operation failed: {e}")
        return None
    except Exception as e:
        logger.warning(f"Error managing repository: {str(e)}")
        return None


def _do_clone_or_update(
    url: str,
    repo_path: Path,
    ref: str | None,
    update: bool,
    git: GitHelper,
) -> Path:
    if repo_path.exists() and (repo_path / ".git").exists():
        if update:
            logger.debug(f"Updating repository at {repo_path}")
            _update_repository(repo_path, ref, git)
        elif ref:
            logger.debug(f"Checking out ref {ref} at {repo_path}")
            _checkout_ref(repo_path, ref, git)
        else:
            logger.debug(f"Using cached repository at {repo_path}")
    else:
        logger.info(f"Cloning repository from {url}")
        _clone_repository(url, repo_path, ref, git)

    return repo_path


def _clone_repository(
    url: str,
    dest: Path,
    branch: str | None,
    git: GitHelper,
) -> None:
    if dest.exists():
        shutil.rmtree(dest)

    git.clone(url, dest, depth=1, branch=branch)
    logger.debug(f"Repository cloned to {dest}")


def _update_repository(
    repo_path: Path,
    ref: str | None,
    git: GitHelper,
) -> None:
    if not _try_fetch(repo_path, git):
        return

    if ref:
        _try_checkout_and_reset(repo_path, ref, git)
        return

    current_branch = git.get_current_branch(repo_path)
    if current_branch:
        _try_reset_to_origin(repo_path, current_branch, git)
        return

    _recover_from_detached_head(repo_path, git)


def _try_fetch(repo_path: Path, git: GitHelper) -> bool:
    try:
        git.fetch(repo_path)
        return True
    except GitCommandError as e:
        logger.warning(f"Failed to fetch updates: {e}. Using cached version.")
        return False


def _try_checkout_and_reset(repo_path: Path, ref: str, git: GitHelper) -> None:
    try:
        _checkout_ref(repo_path, ref, git)
        logger.debug(f"Repository updated to {ref}")
    except GitCommandError as e:
        logger.warning(f"Failed to checkout {ref}: {e}. Using cached version.")


def _try_reset_to_origin(repo_path: Path, branch: str, git: GitHelper) -> None:
    try:
        git.reset_hard(repo_path, f"origin/{branch}")
        logger.debug("Repository updated successfully")
    except GitCommandError as e:
        logger.warning(
            f"Failed to reset to origin/{branch}: {e}. Using cached version."
        )


def _recover_from_detached_head(repo_path: Path, git: GitHelper) -> None:
    default_branch = git.get_default_branch(repo_path)
    if not default_branch:
        logger.warning(
            "Repository is in detached HEAD state and default branch could not be "
            "determined. Specify a ref explicitly to update, or the cached version "
            "will be used as-is."
        )
        return

    logger.debug(
        f"Repository in detached HEAD state, "
        f"checking out default branch: {default_branch}"
    )

    try:
        git.checkout(repo_path, default_branch)
        git.reset_hard(repo_path, f"origin/{default_branch}")
        logger.debug(f"Repository updated to default branch: {default_branch}")
    except GitCommandError as e:
        logger.warning(
            f"Failed to checkout default branch {default_branch}: {e}. "
            "Using cached version."
        )


def _checkout_ref(repo_path: Path, ref: str, git: GitHelper) -> None:
    logger.debug(f"Checking out ref: {ref}")
    git.checkout(repo_path, ref)
    current_branch = git.get_current_branch(repo_path)
    if current_branch is None:
        logger.debug(f"Checked out {ref} (detached HEAD - tag or commit)")
        return

    try:
        git.reset_hard(repo_path, f"origin/{current_branch}")
        logger.debug(f"Branch {current_branch} reset to origin/{current_branch}")
    except GitCommandError:
        # Branch may not exist on origin (e.g., local-only branch)
        logger.debug(
            f"Could not reset to origin/{current_branch} "
            f"(branch may not exist on remote)"
        )
