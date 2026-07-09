# xphi.analyzer.parser.snippet
## @lineage: bound.surface.analyzer.parser.snippet
## @lineage: anchor.cli.analyzer.parser.snippet
"""
@desc: Automatically injects dynamic CLI documentation and source code snippets into Markdown files to keep documentation synchronized with the codebase.
@flow: Initialize Manager -> Parse README -> Regex Match Snippet/CLI Blocks -> Extract & Format Code/Docs -> Write to File (or Check)
"""
import re
import importlib
from pathlib import Path
from typing import Callable
import typer
from typer.main import get_command
from watcher.plane.emitter import get_emitter

log = get_emitter("parser.snippet")

class CliDocProcessor:
    """@desc: Extracts and formats Typer CLI reference documentation."""
    
    def __init__(self) -> None:
        self.pattern = re.compile(
            r"^(\s*)\n(.*?)^\1", 
            flags=re.MULTILINE | re.DOTALL
        )

    def process(self, content: str) -> str:
        """@desc: Replaces matched blocks with generated CLI documentation."""
        return self.pattern.sub(self._process_cli_block, content)

    def _process_cli_block(self, match: re.Match[str]) -> str:
        indent = match.group(1)
        target = match.group(2)
        
        if ":" in target:
            module_path, app_name = target.split(":", 1)
        else:
            module_path, app_name = target, "app"

        generated_md = self._extract_typer_docs(module_path, app_name)
        indented_md = generated_md.replace("\n", f"\n{indent}")
        
        return f"{indent}\n{indent}{indented_md}\n{indent}"

    def _extract_typer_docs(self, module_path: str, app_obj_name: str = "app") -> str:
        try:
            module = importlib.import_module(module_path)
            app = getattr(module, app_obj_name)
            
            if not isinstance(app, typer.Typer):
                log.error(f"Object {app_obj_name} in {module_path} is not a Typer instance.")
                return ""

            click_app = get_command(app)
            md_lines = [f"### CLI Reference: `{module_path}`\n"]
            
            for cmd_name, cmd_obj in click_app.commands.items():
                help_text = cmd_obj.help or cmd_obj.get_short_help_str() or "No description provided."
                md_lines.append(f"#### `mcp {cmd_name}`")
                md_lines.append(f"{help_text.strip()}\n")
                
                if cmd_obj.params:
                    md_lines.append("| Parameter | Type | Required | Description |")
                    md_lines.append("|---|---|---|---|")
                    for param in cmd_obj.params:
                        p_name = getattr(param, "name", "")
                        p_type = param.type.name if hasattr(param, "type") else "Any"
                        p_req = "✅" if param.required else "❌"
                        p_help = getattr(param, "help", "") or ""
                        
                        if hasattr(param, "opts") and param.opts:
                            p_name = ", ".join(param.opts)
                            
                        md_lines.append(f"| `{p_name}` | {p_type} | {p_req} | {p_help} |")
                    md_lines.append("\n")

            return "\n".join(md_lines).strip()

        except Exception as e:
            log.error(f"Failed to extract CLI docs from {module_path}: {e}")
            return f"> ⚠️ **Warning:** Could not generate CLI documentation for `{module_path}`."


class CodeSnippetProcessor:
    """@desc: Extracts raw source code and formats it into Markdown snippets."""
    
    def __init__(self, base_url: str | None = None, check_mode: bool = False) -> None:
        self.base_url = base_url
        self.check_mode = check_mode
        self.pattern = re.compile(
            r"^(\s*)\n(.*?)^\1", 
            flags=re.MULTILINE | re.DOTALL
        )

    def process(self, content: str) -> str:
        """@desc: Replaces matched blocks with raw source code."""
        return self.pattern.sub(self._process_snippet_block, content)

    def _process_snippet_block(self, match: re.Match[str]) -> str:
        full_match = match.group(0)
        indent = match.group(1)
        file_path = match.group(2)

        try:
            file = Path(file_path)
            if not file.exists():
                log.warning(f"Warning: File not found: {file_path}")
                return full_match

            code = file.read_text(encoding="utf-8").rstrip()
            indented_code = code.replace("\n", f"\n{indent}")
            
            if self.base_url:
                source_link = f"{self.base_url.rstrip('/')}/{file_path}"
                link_md = f"_Full example: [{file_path}]({source_link})_"
            else:
                link_md = f"_Full example: `{file_path}`_"
            
            replacement = (
                f"{indent}\n"
                f"{indent}```python\n"
                f"{indent}{indented_code}\n"
                f"{indent}```\n\n"
                f"{indent}{link_md}\n"
                f"{indent}"
            )

            if self.check_mode:
                existing_content = match.group(3) if len(match.groups()) >= 3 else ""
                if self._is_snippet_unchanged(existing_content, code, indent):
                    return full_match

            return replacement

        except Exception as e:
            log.error(f"Error processing {file_path}: {e}")
            return full_match

    def _is_snippet_unchanged(self, existing_content: str, new_code: str, indent: str) -> bool:
        if not existing_content:
            return False
            
        existing_lines = existing_content.strip().split("\n")
        code_lines = []
        in_code = False
        
        for line in existing_lines:
            stripped = line.strip()
            if stripped == "```python":
                in_code = True
            elif stripped == "```":
                break
            elif in_code:
                code_lines.append(line)
                
            existing_code = "\n".join(code_lines).strip()
            expected_code = new_code.replace("\n", f"\n{indent}").strip()
            
        return existing_code == expected_code