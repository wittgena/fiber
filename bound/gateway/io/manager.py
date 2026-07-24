# bound.gateway.io.manager
## @lineage: bound.gateway.bridge.io.manager
## @lineage: bound.gateway.stream.bridge.io.manager
## @lineage: gateway.stream.bridge.io.manager
## @lineage: atoa.gov.context.io.manager
import asyncio
import logging
from typing import Dict, Optional

from bound.xor.secure.secret.validator import Cipher
from atoa.gov.store.log import LogStore, BASE_STATE, EVENTS_DIR

from arch.xor.store.file import LocalFileStore
from arch.xor.store.memfile import InMemoryFileStore
from arch.xor.store.file import FileStore

log = logging.getLogger(__name__)

class WritePayload:
    """큐에 담을 순수 데이터 구조체 (Private 변수 없음)"""
    def __init__(self, conv_id: str, path: str, data: str, is_append: bool = False):
        self.conv_id = conv_id
        self.path = path
        self.data = data
        self.is_append = is_append

class IOManager:
    """I/O 자원 생성, 읽기, 쓰기를 중앙 전담하는 매니저"""
    instance = None

    def __init__(self):
        # 비동기 처리를 위한 큐 및 워커
        self.queue = asyncio.Queue()
        self.worker_task = None
        self.running = False
        
        # 매니저가 통제하는 자원들 (State는 이를 몰라도 됨)
        self.stores: Dict[str, FileStore] = {}
        self.log_stores: Dict[str, LogStore] = {}
        self.ciphers: Dict[str, Cipher] = {}

    @classmethod
    def get_instance(cls):
        if cls.instance is None:
            cls.instance = cls()
        return cls.instance

    async def start(self):
        if self.running:
            return
        self.running = True
        self.worker_task = asyncio.create_task(self.process_queue())
        log.info("IOManager started.")

    async def stop(self):
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
            log.info("IOManager stopped.")

    def register_conversation(
        self, 
        conv_id: str, 
        persistence_dir: str | None, 
        cache_limit: int = 500, 
        cipher: Cipher | None = None
    ):
        """대화 ID를 기준으로 필요한 Store를 생성하고 매니저에 등록"""
        if persistence_dir:
            store = LocalFileStore(persistence_dir, cache_limit_size=cache_limit)
        else:
            store = InMemoryFileStore()

        self.stores[conv_id] = store
        self.log_stores[conv_id] = LogStore(store, dir_path=EVENTS_DIR)
        
        if cipher:
            self.ciphers[conv_id] = cipher

    def get_log_store(self, conv_id: str) -> Optional[LogStore]:
        return self.log_stores.get(conv_id)

    def get_cipher(self, conv_id: str) -> Optional[Cipher]:
        return self.ciphers.get(conv_id)

    def read_base_state(self, conv_id: str) -> Optional[str]:
        """동기식 읽기 (앱 초기화 및 복원 시 사용)"""
        store = self.stores.get(conv_id)
        if not store:
            return None
        try:
            return store.read(BASE_STATE)
        except FileNotFoundError:
            return None

    def save_base_state(self, conv_id: str, payload: str):
        """State 객체가 상태 저장 시 호출하는 논블로킹 진입점"""
        task = WritePayload(conv_id=conv_id, path=BASE_STATE, data=payload, is_append=False)
        self.enqueue(task)

    def append_event(self, conv_id: str, file_name: str, payload: str):
        """Event 추가 시 호출하는 논블로킹 진입점"""
        path = f"{EVENTS_DIR}/{file_name}"
        task = WritePayload(conv_id=conv_id, path=path, data=payload, is_append=True)
        self.enqueue(task)

    def enqueue(self, task: WritePayload):
        """이벤트 루프 확인 후 안전하게 큐에 밀어넣음 (Lock 불필요)"""
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon_threadsafe(self.queue.put_nowait, task)
        except RuntimeError:
            self.execute_task(task)

    async def process_queue(self):
        """단일 백그라운드 워커: 큐에서 꺼내어 순차적으로 디스크 기록"""
        while self.running:
            try:
                task = await self.queue.get()
                # I/O 블로킹 방지를 위한 스레드 위임
                await asyncio.to_thread(self.execute_task, task)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"IO task failed for conv {task.conv_id}: {e}")

    def execute_task(self, task: WritePayload):
        """실제 Store를 이용해 디스크에 기록하는 로직"""
        store = self.stores.get(task.conv_id)
        if not store:
            log.warning(f"Store not found for conv {task.conv_id}, dropping IO task.")
            return

        if task.is_append:
            # FileStore 구현체에 append()가 있다고 가정, 없다면 read + write로 폴백 가능
            if hasattr(store, 'append'):
                store.append(task.path, task.data)
            else:
                try:
                    existing = store.read(task.path)
                except FileNotFoundError:
                    existing = ""
                store.write(task.path, existing + task.data)
        else:
            store.write(task.path, task.data)