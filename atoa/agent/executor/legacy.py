# atoa.agent.executor.legacy
## @lineage: agent.executor.legacy
## @lineage: atoa.executor.legacy
## @lineage: bound.executor.legacy
## @lineage: xor.executor.legacy
## @lineage: xphi.xor.executor.legacy
## @lineage: anchor.phase.executor.legacy
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = 100
executor = ThreadPoolExecutor(max_workers=MAX_THREADS)
