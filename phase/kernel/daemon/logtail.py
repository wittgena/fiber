# phase.kernel.daemon.logtail
## @lineage: dphi.node.daemon.logtail
## @lineage: phase.node.daemon.logtail
## @lineage: ops.daemon.logtail
## @lineage: meta.ops.daemon.logtail
import os
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, List
from collections import defaultdict
from datetime import datetime, timezone

from xphi.kernel.daemon.bootstrap import AbstractDaemon
from xphi.kernel.bind.resolver import resolve_path
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("observer.logtail", phase="ops")
IO_ROOT = resolve_path("io")
WORKSPACE_ROOT = resolve_path("workspace")

class CursorManager:
    """@desc: Persists byte offsets to prevent state amnesia across daemon restarts."""
    def __init__(self, cursor_file: Path):
        self.cursor_file = cursor_file
        self.cursors: Dict[str, int] = self._load()

    def _load(self) -> Dict[str, int]:
        if self.cursor_file.exists():
            with open(self.cursor_file, "r") as f:
                return json.load(f)
        return {}

    def save(self, file_path: str, position: int):
        self.cursors[file_path] = position
        with open(self.cursor_file, "w") as f:
            json.dump(self.cursors, f)

    def get(self, file_path: str) -> int:
        return self.cursors.get(file_path, 0)


class CursorBuilder:
    def __init__(self, batch_size: int = 100, flush_interval: int = 5):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: List[Dict[str, Any]] = []
        self._last_flush = time.time()

    async def add_event(self, event: Dict[str, Any]):
        self._buffer.append(event)
        
        # 버퍼가 차거나 일정 시간이 지나면 AI Context로 플러시
        if len(self._buffer) >= self.batch_size or (time.time() - self._last_flush) > self.flush_interval:
            if self._buffer:
                await self._flush_context()

    async def _flush_context(self):
        """AI에 전달하기 적합한 형태로 이벤트를 재구성 (Contextualization)"""
        flow_groups = defaultdict(list)
        anomalies = []
        
        for event in self._buffer:
            # 1. Flow ID 기준으로 묶기 (작업의 서사 생성)
            flow_id = event.get("context", {}).get("flow_id", "unknown_flow")
            flow_groups[flow_id].append({
                "time": event.get("@timestamp"),
                "level": event.get("level"),
                "source": event.get("source_id"),
                "message": event.get("message")
            })
            
            # 2. 에러 및 폭주(Burst) 이벤트 따로 추출 (AI의 핵심 분석 대상)
            if event.get("level") in ["ERROR", "CRIT"] or event.get("context", {}).get("is_bursting"):
                anomalies.append(event)

        # AI에게 던져줄 최종 구조화된 프롬프트 컨텍스트
        structured_context = {
            "window_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_events": len(self._buffer),
            "flow_stories": dict(flow_groups),
            "critical_anomalies": anomalies
        }

        # [TODO] 향후 이 structured_context를 AI Agent(LLM) 파이프라인이나 메시지 큐로 전달
        log.signal(f"Generated AI Context Batch: {len(flow_groups)} flows, {len(anomalies)} anomalies.")
        
        # 버퍼 초기화
        self._buffer.clear()
        self._last_flush = time.time()


class IOTailDaemon(AbstractDaemon):
    """
    @desc: Dynamically discovers and tails NDJSON files in the 'io' directory.
    """
    def __init__(self):
        super().__init__("ops.iotail")
        self.io_dir = IO_ROOT
        self.io_dir.mkdir(parents=True, exist_ok=True)
        
        self.cursor_mgr = CursorManager(WORKSPACE_ROOT / "io_cursors.json")
        self.builder = CursorBuilder(batch_size=50, flush_interval=10)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._active_tails: Dict[str, asyncio.Task] = {}

    async def _tail_file(self, file_path: Path):
        log.info(f"[IO Monitor] Tailing attached to: {file_path.name}")
        path_str = str(file_path)
        
        while self.running:
            if not file_path.exists():
                await asyncio.sleep(5)
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                f.seek(self.cursor_mgr.get(path_str))
                
                while self.running:
                    line = f.readline()
                    if not line:
                        await asyncio.sleep(0.5)
                        if f.tell() > os.path.getsize(file_path): 
                            f.seek(0)
                        continue
                    
                    line = line.strip()
                    if line:
                        try:
                            # NDJSON 고속 파싱
                            parsed_json = json.loads(line)
                            await self.queue.put((parsed_json, f.tell(), path_str))
                        except json.JSONDecodeError:
                            # 불완전한 라인(디스크 쓰기 도중 읽음 등) 무시
                            pass

    async def _directory_scanner(self):
        """주기적으로 io 디렉토리를 스캔하여 새로운 .jsonl 파일을 추적 파이프라인에 추가합니다."""
        while self.running:
            for file_path in self.io_dir.glob("*.jsonl"):
                path_str = str(file_path)
                if path_str not in self._active_tails:
                    task = asyncio.create_task(self._tail_file(file_path))
                    self._active_tails[path_str] = task
            await asyncio.sleep(10) # 10초마다 디렉토리 스캔

    async def _consume_queue(self):
        """큐에서 이벤트를 꺼내 AI Aggregator로 넘기고 커서를 저장합니다."""
        while self.running:
            try:
                event_data, position, file_path = await self.queue.get()
                
                # AI 컨텍스트 빌더에 이벤트 전달
                await self.builder.add_event(event_data)
                
                # 상태 저장
                self.cursor_mgr.save(file_path, position)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[Consumer Error] {e}")
                await asyncio.sleep(1)

    async def run(self):
        self.log.info(f"[{self.name}] Activated. Monitoring '{self.io_dir}' for NDJSON streams.")
        scanner_task = asyncio.create_task(self._directory_scanner())
        consumer_task = asyncio.create_task(self._consume_queue())
        await asyncio.gather(scanner_task, consumer_task)