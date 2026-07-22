# bound.xor.analyzer.repo
## @lineage: xor.analyzer.repo
## @lineage: xor.opt.analyzer.repo
## @lineage: xphi.analyzer.repo
from pathlib import Path
from typing import Dict, Any

from arch.xor.workflow import Workflow, Event, StartEvent, StopEvent, ErrorEvent, step
from gateway.adapter.mapper.inter.project import IntegrationManifest, ProjectLayout
from bound.xor.analyzer.inter import (
    PyProjectParser, 
    MakefileParser, 
    ReadmeParser, 
    SourceCodeParser,
    WebDependencyFetcher
)

from watcher.plane.emitter import get_emitter

log = get_emitter("analyzer.repo", phase="SYSTEM")

class AnalysisContext:
    """@desc: Context object securely propagating state throughout the pipeline's lifecycle"""
    def __init__(self, repo_dir: str | Path):
        self.repo_dir = Path(repo_dir).resolve()
        if not self.repo_dir.exists() or not self.repo_dir.is_dir():
            raise FileNotFoundError(f"Target repository directory not found: {self.repo_dir}")
        
        self.layout = ProjectLayout.resolve(self.repo_dir)
        self.manifest = IntegrationManifest(repo_name=self.repo_dir.name)
        self.scratchpad: Dict[str, Any] = {}

class SetupCompletedEvent(Event): ctx: AnalysisContext
class PyProjectParsedEvent(Event): ctx: AnalysisContext
class MakefileParsedEvent(Event): ctx: AnalysisContext
class ReadmeParsedEvent(Event): ctx: AnalysisContext
class AstParsedEvent(Event): ctx: AnalysisContext

class InterAnalyzer(Workflow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pyproject_parser = PyProjectParser()
        self.makefile_parser = MakefileParser()
        self.readme_parser = ReadmeParser()
        self.source_parser = SourceCodeParser()
        self.web_fetcher = WebDependencyFetcher()  # Added

    class Meta:
        trans_rules = {"error": ErrorEvent}
        flow = [
            "setup", 
            "extract_pyproject", 
            "extract_makefile", 
            "extract_readme", 
            "extract_source_ast",
            "extract_web_dependencies"
        ]

    @step
    async def setup(self, ev: StartEvent) -> SetupCompletedEvent | ErrorEvent:
        try:
            repo_dir = getattr(ev, "repo_dir", ".")
            ctx = AnalysisContext(repo_dir)
            log.info(f"## STARTING INTEGRATION ANALYSIS PIPELINE: [{ctx.repo_dir.name}]")
            return SetupCompletedEvent(ctx=ctx)
        except Exception as e:
            return ErrorEvent(msg=str(e), status="error")

    @step
    async def extract_pyproject(self, ev: SetupCompletedEvent) -> PyProjectParsedEvent | ErrorEvent:
        ctx = ev.ctx
        try:
            if not ctx.layout.pyproject_path:
                log.warning("[-] pyproject.toml not found. Skipping this stage.")
            else:
                target_path = Path(ctx.layout.pyproject_path)
                ctx.manifest.project_meta = self.pyproject_parser.parse(target_path)
                log.info(f"[+] PyProject extraction completed from: {target_path.relative_to(ctx.repo_dir)}")
                
            return PyProjectParsedEvent(ctx=ctx)
        except Exception as e:
            return ErrorEvent(msg=f"Failed to parse pyproject.toml: {e}", status="error")

    @step
    async def extract_makefile(self, ev: PyProjectParsedEvent) -> MakefileParsedEvent:
        ctx = ev.ctx
        if not ctx.layout.makefile_path:
            log.warning("[-] Makefile not found. Skipping this stage.")
            ctx.manifest.workflow = []
        else:
            commands = self.makefile_parser.parse(Path(ctx.layout.makefile_path))
            ctx.manifest.workflow = commands
            log.info(f"[+] Makefile extraction completed. Parsed {len(commands)} commands.")
            
        return MakefileParsedEvent(ctx=ctx)

    @step
    async def extract_readme(self, ev: MakefileParsedEvent) -> ReadmeParsedEvent:
        ctx = ev.ctx
        if not ctx.layout.readme_path:
            log.warning("[-] README.md not found. Skipping this stage.")
            ctx.scratchpad["readme_snippets"] = []
        else:
            snippets = self.readme_parser.parse(Path(ctx.layout.readme_path))
            ctx.scratchpad["readme_snippets"] = snippets
            log.info(f"[+] Readme snippet extraction completed. Cached {len(snippets)} snippets.")
            
        return ReadmeParsedEvent(ctx=ctx)

    @step
    async def extract_source_ast(self, ev: ReadmeParsedEvent) -> AstParsedEvent | ErrorEvent:
        ctx = ev.ctx
        
        if not ctx.layout.base_py_locations:
            log.warning("[-] Core source file (base.py) not found. Structural analysis skipped.")
            ## Proceed to the next stage (web dependency scan) without error even if the file is missing
            return AstParsedEvent(ctx=ctx)

        try:
            target_file = Path(ctx.layout.base_py_locations[0])
            module_meta = self.source_parser.parse(target_file)
            module_meta.snippets = ctx.scratchpad.get("readme_snippets", [])
            
            module_key = target_file.parent.name
            ctx.manifest.modules[module_key] = module_meta
            
            log.info(f"[+] Source AST extraction completed successfully for [{module_key}].")
            return AstParsedEvent(ctx=ctx)
        except Exception as e:
            return ErrorEvent(msg=f"Source AST parsing failed: {e}", status="error")

    @step
    async def extract_web_dependencies(self, ev: AstParsedEvent) -> StopEvent | ErrorEvent:
        """@desc: Newly added web dependency analysis step"""
        ctx = ev.ctx
        repo_name = ctx.repo_dir.name
        
        try:
            ## Parsing based on LlamaIndex naming convention (llama-index-{category}-{name})
            parts = repo_name.replace("llama-index-", "").split("-")
            if len(parts) >= 2:
                category = parts[0]
                integration_name = "-".join(parts[1:])
            else:
                category = "llms" # Default fallback
                integration_name = repo_name

            log.info(f"[*] Fetching web dependencies for {category} / {integration_name}...")
            
            deps = self.web_fetcher.fetch(category=category, integration_name=integration_name)
            ctx.manifest.external_dependencies = deps.get("external", {})
            ctx.manifest.upstream_internal_packages = deps.get("internal_raw", [])
            
            log.info(f"[+] Web dependency extraction completed (Found {len(ctx.manifest.external_dependencies)} external deps).")
            log.info("## PIPELINE EXECUTION SUCCESS")
            
            ## Final termination of the pipeline
            return StopEvent(result=ctx.manifest)
            
        except Exception as e:
            return ErrorEvent(msg=f"Web dependency fetching failed: {e}", status="error")

    @step
    async def handle_error(self, ev: ErrorEvent) -> StopEvent:
        log.error(f">>> [IntegrationAnalyzer] Pipeline halted due to error: {ev.msg}")
        return StopEvent(result=None)