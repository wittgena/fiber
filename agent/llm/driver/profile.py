# agent.llm.driver.profile
## @lineage: ator.driver.profile
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Final
from filelock import FileLock, Timeout
from watcher.plane.emitter import get_logger
from kernel.bind.resolver import resolve_path

if TYPE_CHECKING:
    from agent.llm.driver.model import LLMModel
    from agent.llm.driver.factory import DriverFactory

_DEFAULT_PROFILE_DIR: Final[Path] = resolve_path("io") / "profiles"
_LOCK_TIMEOUT_SECONDS: Final[float] = 30.0

logger = get_logger(__name__)

class LLMProfileStore:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else _DEFAULT_PROFILE_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._file_lock = FileLock(self.base_dir / ".profiles.lock")

    @contextmanager
    def _acquire_lock(self, timeout: float = _LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
        try:
            with self._file_lock.acquire(timeout=timeout):
                yield
        except Timeout:
            logger.error(f"[Profile Store] Failed to acquire lock within {timeout}s")
            raise TimeoutError(
                f"Profile store lock acquisition timed out after {timeout}s"
            )

    def list(self) -> list[str]:
        with self._acquire_lock():
            return [p.name for p in self.base_dir.glob("*.json")]

    def _get_profile_path(self, name: str) -> Path:
        clean_name = name.removesuffix(".json")
        if (
            not clean_name
            or "/" in clean_name
            or "\\" in clean_name
            or clean_name.startswith(".")
        ):
            raise ValueError(
                f"Invalid profile name: {name!r}. "
                "Profile names must be simple filenames without path separators."
            )

        return self.base_dir / f"{clean_name}.json"

    def save(self, name: str, llm: "Driver", include_secrets: bool = False) -> None:
        profile_path = self._get_profile_path(name)
        with self._acquire_lock():
            if profile_path.exists():
                logger.info(
                    f"[Profile Store] Profile `{name}` already exists. Overwriting."
                )

            profile_json = llm.model_dump_json(
                exclude_none=True,
                indent=2,
                context={"expose_secrets": include_secrets},
            )
            with tempfile.NamedTemporaryFile(
                mode="w", dir=self.base_dir, suffix=".tmp", delete=False
            ) as tmp:
                tmp.write(profile_json)
                tmp_path = Path(tmp.name)

            Path.replace(tmp_path, profile_path)
            logger.info(f"[Profile Store] Saved profile `{name}` at {profile_path}")

    def load(self, name: str) -> "Driver":
        profile_path = self._get_profile_path(name)
        with self._acquire_lock():
            if not profile_path.exists():
                existing = [p.name for p in self.base_dir.glob("*.json")]
                raise FileNotFoundError(
                    f"Profile `{name}` not found. "
                    f"Available profiles: {', '.join(existing) or 'none'}"
                )

            try:
                from agent.llm.driver.model import LLMModel
                llm_instance = DriverFactory.load_from_json(str(profile_path))
            except Exception as e:
                # Re-raise as ValueError for clearer error handling
                raise ValueError(f"Failed to load profile `{name}`: {e}") from e

            logger.info(f"[Profile Store] Loaded profile `{name}` from {profile_path}")
            return llm_instance

    def delete(self, name: str) -> None:
        profile_path = self._get_profile_path(name)
        with self._acquire_lock():
            if not profile_path.exists():
                logger.info(f"[Profile Store] Profile `{name}` not found. Skipping.")
                return

            profile_path.unlink()
            logger.info(f"[Profile Store] Deleted profile `{name}`")
