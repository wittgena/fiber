# bound.surface.legacy.client.executor
## @lineage: bound.transport.client.executor
## @lineage: bound.bridge.channel.task.executor
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 100
executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
