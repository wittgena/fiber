# anchor.phase.executor.legacy
## @lineage: anchor.executor.legacy
## @lineage: bound.surface.legacy.client.executor
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 100
executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
