# atoa.agent.mock.icl
## @lineage: atoa.call.mock.icl
## @lineage: agent.call.mock.icl
## @lineage: agent.llm.call.mock.icl
## @lineage: gov.llm.call.mock.icl
## @lineage: gov.llm.mock.icl
## @lineage: gov.sandbox.mock.icl
import sys
import runpy
from typing import Final
from eco.tenant.switch.params import ChatCompletionToolParam
from phase.bind.resolver import resolve_path 

RES_ROOT = resolve_path("res")
TEX_FILE = RES_ROOT / "tex.py"

TEX = runpy.run_path(str(TEX_FILE))["TEX"]
TOOL_KEY_MAP = {
    "terminal": "terminal",
    "file_editor": "file_editor",
    "browser": "browser",
    "finish": "finish",
    "edit_file": "edit_file",
    "task_tracker": "task_tracker"
}

def _refine_prompt(prompt: str) -> str:
    return prompt.replace("bash", "powershell") if sys.platform == "win32" else prompt

def get_tool_icl(tools: list[ChatCompletionToolParam]) -> str:
    """Generate an in-context learning example based on available tools."""
    avail = {
        TOOL_KEY_MAP[name] 
        for t in tools 
        if t.get("type") == "function" and (name := t["function"]["name"]) in TOOL_KEY_MAP
    }
    if not avail:
        return ""

    parts = [
        "Here's a running example of how to perform a task with the provided tools.\n\n",
        "--------------------- START OF EXAMPLE ---------------------\n\n",
        "USER: Create a list of numbers from 1 to 10, and display them in a web page at port 5000.\n\n"
    ]

    # Build example based on available tools
    if "terminal" in avail: parts.append(TEX["bash"]["check_dir"])
    if "file_editor" in avail: parts.append(TEX["file_editor"]["create_file"])
    elif "edit_file" in avail: parts.append(TEX["edit_file"]["create_file"])
    if "terminal" in avail: parts.append(TEX["bash"]["run_server"])
    if "browser" in avail: parts.append(TEX["browser"]["view_page"])
    if "terminal" in avail: parts.append(TEX["bash"]["kill_server"])
    if "file_editor" in avail: parts.append(TEX["file_editor"]["edit_file"])
    elif "edit_file" in avail: parts.append(TEX["edit_file"]["edit_file"])
    if "terminal" in avail: parts.append(TEX["bash"]["run_server_again"])
    if "finish" in avail: parts.append(TEX["finish"]["example"])
    if "task_tracker" in avail:
        parts.append(TEX["task_tracker"]["view"])
        parts.append(TEX["task_tracker"]["plan"])

    parts.append("\n--------------------- END OF EXAMPLE ---------------------\n\n")
    parts.append("Do NOT assume the environment is the same as in the example above.\n\n")
    parts.append("--------------------- NEW TASK DESCRIPTION ---------------------\n")
    return _refine_prompt("".join(parts).lstrip())