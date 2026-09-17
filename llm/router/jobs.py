# fiber.llm.router.jobs
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

from fiber.llm.router.dispatcher import dispatcher

if TYPE_CHECKING:
    from fiber.llm.types.llm.block import ContentBlock, TextBlock


T = TypeVar("T")
DEFAULT_NUM_WORKERS = 4

@dispatcher.span
async def run_jobs(
    jobs: List[Coroutine[Any, Any, T]],
    show_progress: bool = False,
    workers: int = DEFAULT_NUM_WORKERS,
    desc: Optional[str] = None,
) -> List[T]:
    semaphore = asyncio.Semaphore(workers)

    @dispatcher.span
    async def worker(job: Coroutine) -> Any:
        async with semaphore:
            return await job

    pool_jobs = [worker(job) for job in jobs]

    if show_progress:
        from tqdm.asyncio import tqdm_asyncio
        results = await tqdm_asyncio.gather(*pool_jobs, desc=desc)
    else:
        results = await asyncio.gather(*pool_jobs)

    return results