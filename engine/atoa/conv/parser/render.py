# engine.atoa.conv.parser.render
## @lineage: engine.protocol.atoa.conv.parser.render
## @lineage: phi.agent.atoa.conv.parser.render
## @lineage: agent.atoa.conv.parser.render
## @lineage: atoa.agent.parser.render
## @lineage: phi.agent.parser.render
## @lineage: bound.parser.conv.render
## @lineage: gov.atoa.parser.action
from __future__ import annotations
import os
import sys
import json
import re
from typing import Any
from pydantic import ValidationError
from functools import lru_cache
from jinja2 import (
    BaseLoader,
    Environment,
    FileSystemBytecodeCache,
    Template,
    TemplateNotFound,
)
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class FlexibleFileSystemLoader(BaseLoader):
    def __init__(self, searchpath: str):
        self.searchpath = os.path.abspath(searchpath)

    def get_source(self, environment, template):
        if os.path.isabs(template):
            path = template
        else:
            path = os.path.join(self.searchpath, template)

        if not os.path.exists(path):
            raise TemplateNotFound(template)

        mtime = os.path.getmtime(path)
        with open(path, encoding="utf-8") as f:
            source = f.read()

        def uptodate():
            try:
                return os.path.getmtime(path) == mtime
            except OSError:
                return False

        return source, path, uptodate

def refine(text: str) -> str:
    if sys.platform == "win32":
        text = re.sub(r"\bterminal\b", "execute_powershell", text, flags=re.IGNORECASE)
        text = re.sub(
            r"(?<!execute_)(?<!_)\bbash\b", "powershell", text, flags=re.IGNORECASE
        )
    return text


@lru_cache(maxsize=64)
def _get_env(prompt_dir: str) -> Environment:
    if not prompt_dir:
        raise ValueError("prompt_dir is required")
    cache_folder = os.path.join(os.path.expanduser("~"), ".surgent", "cache", "jinja")
    os.makedirs(cache_folder, exist_ok=True)
    bcc = FileSystemBytecodeCache(directory=cache_folder)
    env = Environment(
        loader=FlexibleFileSystemLoader(prompt_dir),
        bytecode_cache=bcc,
        autoescape=False,
    )
    env.filters["refine"] = refine
    return env


@lru_cache(maxsize=256)
def _get_template(prompt_dir: str, template_name: str) -> Template:
    env = _get_env(prompt_dir)
    try:
        return env.get_template(template_name)
    except Exception:
        raise FileNotFoundError(
            f"Prompt file {os.path.join(prompt_dir, template_name)} not found"
        )

def render_template(prompt_dir: str, template_name: str, **ctx) -> str:
    if os.path.isabs(template_name):
        if not os.path.isfile(template_name):
            raise FileNotFoundError(f"Prompt file {template_name} not found")
        actual_dir = os.path.dirname(template_name)
        actual_filename = os.path.basename(template_name)
        tpl = _get_template(actual_dir, actual_filename)
    else:
        tpl = _get_template(prompt_dir, template_name)
    return refine(tpl.render(**ctx).strip())
