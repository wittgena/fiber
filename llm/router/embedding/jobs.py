# fiber.llm.router.embedding.jobs
import asyncio
import base64
import concurrent.futures
import contextvars
import json
import os
import re
from binascii import Error as BinasciiError
from functools import partial
from io import BytesIO
from pathlib import Path
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
    TYPE_CHECKING,
)
from urllib.parse import urlparse

import platformdirs
import requests

from xphi.watcher.observer.span import observe
from xphi.watcher.plane.emitter import get_emitter

if TYPE_CHECKING:
    from fiber.llm.types.inter.block import ContentBlock, TextBlock

log = get_emitter(__name__, phase="EMBEDDING_JOBS")

T = TypeVar("T")
DEFAULT_NUM_WORKERS = 4

@observe(name="llm.embedding.run_jobs", phase="EMBEDDING_JOBS")
async def run_jobs(
    jobs: List[Coroutine[Any, Any, T]],
    show_progress: bool = False,
    workers: int = DEFAULT_NUM_WORKERS,
    desc: Optional[str] = None,
) -> List[T]:
    log.info(
        f"Starting {len(jobs)} parallel jobs", 
        context={"workers": workers, "desc": desc}
    )

    semaphore = asyncio.Semaphore(workers)
    @observe(name="llm.embedding.worker")
    async def worker(job: Coroutine) -> Any:
        async with semaphore:
            return await job

    pool_jobs = [worker(job) for job in jobs]

    if show_progress:
        from tqdm.asyncio import tqdm_asyncio
        results = await tqdm_asyncio.gather(*pool_jobs, desc=desc)
    else:
        results = await asyncio.gather(*pool_jobs)

    log.debug("All parallel jobs completed successfully.")
    return results