# anchor.cli.adapter.analyzer.llm
import sys
import json
import asyncio
import argparse
from pathlib import Path
from dataclasses import asdict

from anchor.cli.adapter.scan.llm import LLMScanner
from xphi.analyzer.repo import InterAnalyzer

from arch.contract.registry.unified import contract
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

log = get_emitter("analyzer.llm", phase="SYSTEM")

class LLMAnalyzer:
    """@desc: Core business logic engine focusing on a single target integration."""
    def __init__(self, target: str):
        self.target = target
        self.timeout = 300

    async def _pipe(self) -> dict:
        """@desc: Asynchronous execution of the scan -> analyze pipeline for one target."""
        log.info(f"[*] [Phase 1] Initializing Scout (Scanner) to locate target: [{self.target}]")
        scanner = LLMScanner()
        scan_results = scanner.scan()
        
        if self.target not in scan_results:
            log.error(f"[-] Target integration '{self.target}' not found in the monorepo catalog.")
            return {}

        info = scan_results[self.target]
        layout = info.get("layout")
        
        if not layout or not layout.get("root_dir"):
            log.error(f"[-] Target [{self.target}] does not have a valid layout structure. Halting.")
            return {}
            
        repo_dir = layout["root_dir"]
        log.info(f"\n---> [Phase 2] Analyzing [{self.target}] at {Path(repo_dir).name}...")
        
        try:
            workflow = InterAnalyzer(timeout=self.timeout)
            manifest = await workflow.run(repo_dir=repo_dir)
            
            if manifest:
                log.info(f"[+] Successfully extracted manifest for [{self.target}]")
                return asdict(manifest)
            else:
                log.warning(f"[-] Analysis failed for [{self.target}]: No manifest generated.")
                return {}
                
        except Exception as e:
            log.error(f"[!] Critical error while analyzing [{self.target}]: {e}")
            return {}

    def execute(self) -> None:
        """@desc: Synchronous entry method for the adapter to trigger."""
        try:
            final_manifest = asyncio.run(self._pipe())
            if not final_manifest:
                sys.exit(1)
            
            json_output = json.dumps(final_manifest, indent=4, ensure_ascii=False)
            log.info(f"\n[ANALYSIS MANIFEST RESULT: {self.target}]\n{json_output}")
        except Exception as e:
            log.error(f"[ERROR] Orchestrator execution failed: {e}")
            sys.exit(1)

def entry_task(args):
    parser = argparse.ArgumentParser(description="Brane LlamaIndex Single Target Orchestrator")
    parser.add_argument(
        "--target", 
        type=str, 
        required=True, 
        help="Specific LLM integration target to analyze (e.g., 'anthropic', 'openai')"
    )
    parsed_args = parser.parse_args(args)
    orchestrator = LLMAnalyzer(target=parsed_args.target)
    return CliTaskAdapter(orchestrator.execute)

@contract.cli(
    name="analyzer.llm", 
    args=["--target"],
    tags=["llama", "analyzer", "manifest"],
    entry="entry_task" 
)
def main(args=None):
    if args is not None:
        return entry_task(args)
    
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli("analyzer.llm", entry_task, __file__)

if __name__ == "__main__":
    main()