# bound.channel.action.task.executor
## @lineage: bound.channel.client.action.task.executor
## @lineage: anchor.channel.client.action.task.executor
## @lineage: anchor.channel.action.task.executor
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 100
executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
