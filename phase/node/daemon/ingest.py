# phase.node.daemon.ingest
import asyncio
import json
import re
import _thread
from pathlib import Path
from typing import Dict, Any, List, Set, Union
import httpx
import asyncio
from typing import Dict, Any, Optional, Tuple, List

from arch.contract.interface import IEventBus
from arch.contract.event.psi import PsiEvent
from kernel.daemon.bootstrap import AbstractDaemon
from kernel.bind.resolver import resolve_path
from watcher.plane.emitter import get_emitter

log = get_emitter('resolver.issue')

class GitHubProber:
    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Surgent-Prober/2.1",
            "Accept": "application/vnd.github.v3+json"
        }
        self.client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        """@desc: Context manager for async client initialization."""
        self.client = httpx.AsyncClient(timeout=self.timeout, headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    async def search_issues(self, query: str) -> Tuple[int, List[Dict[str, Any]]]:
        """
        @desc: Executes the search query against GitHub API.
        @return: Tuple of (HTTP_Status_Code, List_of_Items)
        """
        if not self.client:
            raise RuntimeError("Prober must be used within an async context manager.")
            
        target_url = f"https://api.github.com/search/issues?q={query}&sort=updated&order=desc"
        response = await self.client.get(target_url)
        
        items = response.json().get("items", []) if response.status_code == 200 else []
        return response.status_code, items

class IssueStorage:
    def __init__(self):
        self.workspace = resolve_path("workspace") / "issue"
        self.registry_file = self.workspace / "registry.jsonl"
        self.report_dir = self.workspace / "report"
        
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def fetch_registry(self, target_id: str = None) -> Union[Set[str], Dict[str, Any]]:
        if not self.registry_file.exists():
            return {} if target_id else set()

        processed_ids = set()
        with open(self.registry_file, "r", encoding="utf-8") as f:
            for line in (l.strip() for l in f if l.strip()):
                try:
                    data = json.loads(line)
                    if target_id and data.get("issue_id") == target_id:
                        return data
                    if "issue_id" in data:
                        processed_ids.add(data["issue_id"])
                except json.JSONDecodeError:
                    continue
        return {} if target_id else processed_ids

    def upsert_registry(self, record: Dict[str, Any]) -> None:
        if not self.registry_file.exists():
            with open(self.registry_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            return

        updated = False
        lines = []
        with open(self.registry_file, "r", encoding="utf-8") as f:
            for line in (l.strip() for l in f if l.strip()):
                try:
                    data = json.loads(line)
                    if data.get("issue_id") == record.get("issue_id"):
                        lines.append(json.dumps(record, ensure_ascii=False) + "\n")
                        updated = True
                    else:
                        lines.append(line + "\n")
                except json.JSONDecodeError:
                    lines.append(line + "\n")

        if not updated:
            lines.append(json.dumps(record, ensure_ascii=False) + "\n")

        with open(self.registry_file, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def generate_report(self, item: Dict[str, Any]) -> None:
        issue_id = item.get('issue_id', 'unknown_issue')
        target_file = self.report_dir / f"{issue_id}.md"
        summary = item.get("summary", {})
        
        with open(target_file, "w", encoding="utf-8") as f:
            f.write(f"# GitHub Issue Analysis: {issue_id}\n\n")
            f.write(f"## {item.get('title')}\n- **URL**: {item.get('url')}\n- **Tag**: {item.get('tag')}\n\n")
            
            if "error" in summary:
                f.write(f"> **분석 실패**: {summary.get('error')}\n\n")
            else:
                f.write(f"### 💬 대화 요약\n{summary.get('translated_conversations', '대화 없음')}\n\n")
                f.write("### 🔍 분석 요약\n")
                f.write(f"- **증상**: {summary.get('symptom', 'N/A')}\n")
                f.write(f"- **원인**: {summary.get('cause', 'N/A')}\n")
                f.write(f"- **기술 스택**: {summary.get('tech_stack', 'N/A')}\n\n")
            
            f.write(f"---\n## 📎 원본 대화 내역\n```text\n{item.get('raw_conversations', 'N/A')}\n```\n")
        log.info(f"  └─ Report generated at: {target_file}")


class IssueAnalyzer:
    def __init__(self, engine_factory: callable):
        self.engine_factory = engine_factory
        self.prober = GitHubProber()
        self._llm_buffer = {"text": "", "count": 0}

    def _rupture_callback(self, event):
        if event.source != "agent": return
        self._llm_buffer["count"] += 1
        
        if self._llm_buffer["count"] >= 1000:
            log.error("## @fatal: Max token limit reached.")
            _thread.interrupt_main()
            return

        text = event.content.strip()
        if len(text) > 30 and "}" in text:
            self._llm_buffer["text"] = text
            _thread.interrupt_main()

    async def analyze(self, target_data: Dict[str, Any]) -> Dict[str, Any]:
        url = target_data.get("url")
        if not url: return target_data

        log.info("  └─ Fetching raw conversations via GitHubProber...")
        target_data["raw_conversations"] = await self.prober.fetch_conversations(url)

        prompt = (
            f"[Target Title]: {target_data.get('title')}\n[URL]: {url}\n"
            f"[Conversations]:\n{target_data['raw_conversations']}\n\n"
            f"Output STRICTLY in JSON format with keys (translated_conversations, symptom, cause, tech_stack) in Korean."
        )

        log.info("  └─ Requesting topological analysis from LLM engine...")
        self._llm_buffer = {"text": "", "count": 0}
        engine = self.engine_factory("analyzer")
        try:
            response = engine.ask(prompt, callback=self._rupture_callback)
            if not self._llm_buffer["text"] and response:
                self._llm_buffer["text"] = str(response)
        except KeyboardInterrupt:
            log.info("  └─ Analysis phase severed by Rupture (JSON completed early).")

        raw_text = self._llm_buffer["text"]
        try:
            match = re.search(r'\{.*\}', raw_text, re.DOTALL)
            target_data["summary"] = json.loads(match.group(0)) if match else {"error": "JSON pattern not found."}
        except Exception as e:
            target_data["summary"] = {"error": str(e)}
        return target_data


class IssueResolverDaemon(AbstractDaemon):
    def __init__(self, bus: IEventBus, engine_factory: callable):
        super().__init__("daemon.issue_resolver")
        self.bus = bus
        self.storage = IssueStorage()
        self.analyzer = IssueAnalyzer(engine_factory)
        
        self.bus.subscribe(self.name, self._handle_ingest)

    async def _handle_ingest(self, event: PsiEvent) -> None:
        carrier = event.carrier
        if getattr(carrier, 'kind', None) != "bounty" or getattr(carrier, 'tag', None) != "acquired":
            return
            
        log.info(f"\n## @internal.field: 수신: {carrier.tag} (Tick: {event.tick})")
        processed_ids = self.storage.fetch_registry()
        
        for i, target in enumerate(carrier.payload, 1):
            issue_id = target.get('issue_id')
            if not issue_id or issue_id in processed_ids:
                continue
                
            record = {
                "issue_id": issue_id,
                "tag": target.get('tag', 'unknown'),
                "title": target.get('title'),
                "url": target.get('url'),
                "anomaly": target.get('anomaly', False)
            }
            
            self.storage.upsert_registry(record)
            indicator = "[ANOMALY]" if record['anomaly'] else f"[{record['tag']}]"
            log.info(f"    {i}. {indicator} [{target.get('potential', 'N/A')}] {record['title']}")

    async def execute_deep_scan(self, target_id: str):
        log.info(f"## @daemon: surgent deep_scan (Target ID: {target_id})")
        target_data = self.storage.fetch_registry(target_id=target_id)
        if not target_data:
            log.warning(f"Target ID '{target_id}' not found in registry.")
            return

        analyzed_data = await self.analyzer.analyze(target_data)
        self.storage.upsert_registry(analyzed_data)
        self.storage.generate_report(analyzed_data)
        log.info("=" * 60)
        log.info("Analyzer complete. Registry updated and Report generated.")

    async def run(self):
        log.info(f"{self.name} 가동. Event Bus 구독 대기 중...")
        while self.running:
            await asyncio.sleep(1.0)