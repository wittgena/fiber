# fiber.dphi.worker.search.archive
import os
import sys
import json
import time
import asyncio
import re
import glob
import contextlib
from datetime import datetime, timedelta
from typing import Dict, Any

import httpx
import duckdb

from xphi.arch.contract.protocol.agent import AsyncAgentProtocol

class SearchTelemetry:
    """데이터 처리량 및 구간별 소요 시간을 구조적으로 추적하는 계측기"""
    def __init__(self):
        self.scanned_mb: float = 0.0
        self.events_parsed: int = 0
        self.timing_metrics: Dict[str, float] = {}

    def measure_storage(self, file_pattern: str):
        """지정된 패턴의 파일 총 용량을 MB 단위로 계산하여 기록합니다."""
        files = glob.glob(file_pattern)
        total_bytes = sum(os.path.getsize(f) for f in files)
        self.scanned_mb = round(total_bytes / (1024 * 1024), 2)

    @contextlib.contextmanager
    def time_block(self, metric_name: str):
        """특정 코드 블록의 실행 시간을 측정하는 컨텍스트 매니저"""
        start_time = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - start_time
            self.timing_metrics[metric_name] = round(elapsed, 4)

    def record_event_count(self, count: int):
        self.events_parsed = count

    def to_dict(self) -> Dict[str, Any]:
        """E2E나 외부 에이전트가 소비하기 쉬운 딕셔너리 형태로 반환합니다."""
        return {
            "scanned_file_mb": self.scanned_mb,
            "total_events_parsed": self.events_parsed,
            **self.timing_metrics
        }


# =====================================================================
# Main Worker Class
# =====================================================================
class ArchiveSearchWorker(AsyncAgentProtocol):
    def __init__(self):
        super().__init__(agent_name="search.archive.worker")
        
        self.log.info("Initializing DuckDB In-Memory Engine & Filters...")
        
        self.data_dir = './github_data'
        self.data_path = f'{self.data_dir}/*.json.gz'
        self.target_repos = ['BerriAI/litellm', 'langchain-ai/langchain', 'microsoft/autogen']
        
        self.blacklist = ["#nft", "airdrop", "tutorial", "how to build"]
        self.regex_cost = re.compile(r"(\$|€|£|¥)[0-9,]+(\.[0-9]{2})?|[0-9,]+(k|m)?\s*(tokens|calls)\s*(wasted|drained|burned|cost|hit)", re.IGNORECASE)
        self.regex_error = re.compile(r"[a-zA-Z]*(Error|Exception|LimitExceeded|Timeout|OOM)[a-zA-Z]*|infinite loop|stuck|recursion", re.IGNORECASE)
        
        self.domain_keywords = {
            "Domain_1_Control_Failure": ["agent", "litellm", "infinite loop", "runaway", "budget", "limit"],
            "Domain_2_Security_Audit": ["firewall", "data leak", "audit", "compliance", "enterprise"],
            "Domain_3_M2M_Payment": ["http 402", "macaroon", "capability token", "payment"],
            "Domain_4_Tool_Call_Lockin": ["malformed json", "parse error", "migration", "lock-in"]
        }
        
        self.log.info("DuckDB Search Worker successfully mounted. Ready for Intents.")

    async def _ensure_archive_data(self):
        os.makedirs(self.data_dir, exist_ok=True)
        if glob.glob(self.data_path):
            return

        self.log.warning("⚠️ No local archive data found. Initiating auto-download of real data for testing...")
        
        target_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        target_file = f"{target_date}-0.json.gz"
        url = f"https://data.gharchive.org/{target_file}"
        save_path = os.path.join(self.data_dir, target_file)

        self.log.info(f"⬇️ Downloading actual GHArchive data from: {url}")
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                async with client.stream("GET", url) as response:
                    if response.status_code != 200:
                        raise FileNotFoundError(f"Download rejected by GHArchive. HTTP {response.status_code}")
                    
                    with open(save_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                            
            self.log.info(f"✅ Real data downloaded successfully: {save_path}")
        except Exception as e:
            self.log.error(f"Failed to auto-download archive data: {e}")
            raise RuntimeError(f"Data fetching failed: {e}")

    async def handle_tools_list(self, req_id: Any):
        tools = [{
            "name": "search_market_evidence",
            "description": "Scan GitHub Archive using DuckDB to find real-world agentic infrastructure pain points and vulnerabilities.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "domain": {"type": "string", "enum": list(self.domain_keywords.keys())},
                    "limit": {"type": "integer", "default": 20}
                },
                "required": ["domain"]
            }
        }]
        await self.send_response(req_id, {"tools": tools})

    async def handle_tools_call(self, req_id: Any, tool_name: str, arguments: Dict[str, Any], meta: Dict[str, Any]):
        if tool_name == "search_market_evidence":
            await self._execute_duckdb_search(req_id, arguments)
        else:
            await self.send_error(req_id, code=-32601, message=f"Method not found: Unknown tool '{tool_name}'")

    async def _execute_duckdb_search(self, req_id: Any, arguments: Dict[str, Any]):
        domain = arguments.get("domain")
        limit = arguments.get("limit", 20)
        
        if domain not in self.domain_keywords:
            await self.send_error(req_id, code=-32602, message=f"Invalid domain. Choose from: {list(self.domain_keywords.keys())}")
            return

        self.log.info(f"Executing DuckDB Search -> Domain: {domain}, Limit: {limit}")
        
        try:
            await self._ensure_archive_data()
            result_payload = await asyncio.to_thread(self._sync_duckdb_scan_and_filter, domain, limit)
            
            await self.send_response(req_id, {
                "content": [{"type": "text", "text": json.dumps(result_payload)}],
                "isError": False
            })
            
        except Exception as e:
            self.log.error(f"DuckDB Search Execution Failed: {str(e)}", exc_info=True)
            await self.send_error(req_id, code=-32000, message=f"Search Execution Failed: {str(e)}")

    def _sync_duckdb_scan_and_filter(self, target_domain: str, limit: int) -> Dict[str, Any]:
        """
        동기 블록: 데이터 추출 및 정규식 필터링
        SearchTelemetry 클래스를 활용하여 각 구간을 우아하게 계측합니다.
        """
        telemetry = SearchTelemetry()
        telemetry.measure_storage(self.data_path)

        repos_sql_str = ", ".join([f"'{r}'" for r in self.target_repos])
        query = f"""
        SELECT 
            type,
            repo.name AS repo_name,
            JSON_EXTRACT_STRING(payload, '$.issue.html_url') AS thread_id,
            JSON_EXTRACT_STRING(payload, '$.issue.user.login') AS issue_author,
            JSON_EXTRACT_STRING(payload, '$.issue.body') AS issue_body,
            JSON_EXTRACT_STRING(payload, '$.comment.user.login') AS comment_author,
            JSON_EXTRACT_STRING(payload, '$.comment.body') AS comment_body
        FROM read_json_auto('{self.data_path}', ignore_errors=true)
        WHERE repo.name IN ({repos_sql_str})
          AND type IN ('IssuesEvent', 'IssueCommentEvent')
        """
        
        # [계측 1] DuckDB SQL 쿼리 구간
        with telemetry.time_block("duckdb_sql_time_sec"):
            raw_events = duckdb.query(query).df().to_dict('records')
            telemetry.record_event_count(len(raw_events))
        
        # [계측 2] 파이썬 스레드 병합 및 정규식 필터링 구간
        with telemetry.time_block("python_regex_time_sec"):
            threads = {}
            for event in raw_events:
                t_id = event['thread_id']
                if not t_id: continue
                
                if t_id not in threads:
                    threads[t_id] = {
                        "thread_id": t_id,
                        "repo": event['repo_name'],
                        "parent_post": {"author": event['issue_author'] or "unknown", "text": event['issue_body'] or ""},
                        "comments": []
                    }
                if event['type'] == 'IssueCommentEvent' and event['comment_body']:
                    threads[t_id]['comments'].append({"author": event['comment_author'] or "unknown", "text": event['comment_body']})

            final_output = []
            target_keywords = self.domain_keywords[target_domain]
            
            for t_id, thread in threads.items():
                if len(final_output) >= limit:
                    break
                    
                parent_text = thread['parent_post']['text']
                comments = thread['comments']
                full_text = parent_text + " " + " ".join([c['text'] for c in comments])
                full_text_lower = full_text.lower()
                
                if len(full_text) < 150: continue
                if not any(kw in full_text_lower for kw in target_keywords): continue
                    
                has_blacklist = any(b in full_text_lower for b in self.blacklist)
                cost_evidence = self.regex_cost.findall(full_text)
                error_evidence = self.regex_error.findall(full_text)
                has_evidence = bool(cost_evidence or error_evidence)
                
                if has_blacklist and not has_evidence: continue
                    
                relevant_comments = []
                for c in comments:
                    c_cost = self.regex_cost.search(c['text'])
                    c_err = self.regex_error.search(c['text'])
                    if c_cost or c_err:
                        relevant_comments.append({
                            "author": c['author'], "text": c['text'],
                            "evidence": {
                                "financial_loss": c_cost.group(0) if c_cost else None,
                                "error_type": c_err.group(0) if c_err else None
                            }
                        })
                
                final_output.append({
                    "thread_id": thread['thread_id'], "repo": thread['repo'],
                    "target_pain_point": target_domain,
                    "parent_post": {"author": thread['parent_post']['author'], "text": parent_text[:300] + "..."},
                    "relevant_comments": relevant_comments,
                    "is_override_applied": (has_blacklist and has_evidence),
                    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                })
            
        return {
            "query_domain": target_domain, 
            "extracted_threads_count": len(final_output),
            "telemetry": telemetry.to_dict(),  # 분리된 객체의 결과를 단일 라인으로 주입
            "results": final_output
        }

def main():
    server = ArchiveSearchWorker()
    try:
        asyncio.run(server.serve_forever_async())
    except KeyboardInterrupt:
        server.log.info("DuckDB Search Worker shutting down by interrupt.")
        sys.exit(0)

if __name__ == "__main__":
    main()