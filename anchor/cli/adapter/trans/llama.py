# anchor.cli.adapter.trans.llama
import os
import sys
import shutil
import tempfile
import argparse
import json
import threading
import subprocess
import urllib.request
from urllib.error import URLError, HTTPError
from pathlib import Path

import anchor.inter as inter_path
import anchor.inter.bound as llama_bound
import xphi.loop as xphi_loop
import xphi.loop.flow as xphi_flow

from xphi.analyzer.parser.github import GitHubExtractor

from anchor.cli.runner import IsoRunner
from anchor.registry.resolver.ext import ExtResolver
from arch.contract.registry.unified import contract, registry
from phase.bind.resolver import find_current_self, get_invoker
from phase.runtime.cli.executor import CliTaskAdapter, parse_local, dispatch_cli
from watcher.plane.emitter import get_emitter

_invoker_full, MODULE_NAMESPACE = get_invoker(Path(__file__))
log = get_emitter(MODULE_NAMESPACE, phase="SYSTEM")

SELF_ROOT = find_current_self()
TARGET_REPO = "brane"
DEST_PATH = inter_path.__name__

LLAMA_BOUND_PATH = llama_bound.__name__
LOOP_PATH = xphi_loop.__name__
FLOW_PATH = xphi_flow.__name__

IMPORT_ALIGN_MAP = [
    ("llama_index.core.readers", f"{FLOW_PATH}.reader"),
    ("llama_index.core.embeddings", f"{FLOW_PATH}.embedding"),
    ("llama_index.core.llms", f"{FLOW_PATH}.llm"),
    ("llama_index.core.program", f"{LOOP_PATH}.prog"),
    ("llama_index.core.instrumentation", f"{LOOP_PATH}.inst"),
    ("llama_index.core.callbacks", f"{LOOP_PATH}.callback" ),
    ("llama_index.core", LLAMA_BOUND_PATH),
]

DEFAULT_TARGET_TAG = ExtResolver.RULES["constants"]["tag"]

class LlamaTransductor:
    """Main pipeline orchestrating the extraction and dependency mutation of integration modules"""
    def __init__(self, category: str, name: str, tag: str = DEFAULT_TARGET_TAG):
        self.category = category
        self.name = name
        self.tag = tag
        
        ## Calculate local destination path following Python package naming conventions
        category_pkg = category.replace("-", "_")
        name_pkg = name.replace("-", "_")
        
        new_path_parts = DEST_PATH.split(".")
        self.dest_dir = SELF_ROOT / TARGET_REPO / Path(*new_path_parts) / category_pkg / name_pkg
        
        ## Initialize extractor using ExtResolver logic seamlessly
        self.extractor = GitHubExtractor(category, name, self.dest_dir, self.tag)

    def mutate_dependencies(self):
        """Sequential dependency substitution and path alignment within an isolated environment"""
        log.info("\n## @mutate: Isolated Subprocess Execution")
        log.signal("[TASK] Executing 'align.imports' sequentially...")
        
        for old_pkg, new_pkg in IMPORT_ALIGN_MAP:
            log.info(f"  -> Aligning import: '{old_pkg}' to '{new_pkg}'")
            IsoRunner.execute_subtask(
                command_name="align.imports", 
                args=["--local", "--old", old_pkg, "--new", new_pkg, "--repo", TARGET_REPO]
            )
        
        log.signal("[TASK] Executing 'align.path' sequentially...")
        IsoRunner.execute_subtask(
            command_name="align.path", 
            args=["--local", "--repo", TARGET_REPO]
        )
        log.info("\n[SUCCESS] All isolated mutation processes completed.")

    def run(self):
        """Main synchronized execution pipeline"""
        if not (SELF_ROOT / TARGET_REPO).is_dir():
            log.error(f"[ERROR] Cannot find repository '{TARGET_REPO}'. Make sure it is at the top of '.self/'.")
            sys.exit(1)
            
        try:
            log.info(f"## @extract: '{self.category}/{self.name}' [Tag: {self.tag}]")
            self.extractor.fetch()
            self.mutate_dependencies()
        except Exception as e:
            log.error(f"\n[CRITICAL ERROR] Pipeline halted: {e}")
            raise


def entry_task(args):
    parser = argparse.ArgumentParser(description="Brane Integration Extraction Transductor")
    parser.add_argument("--category", required=True, help="Integration category (e.g., readers, llms)")
    parser.add_argument("--name", required=True, help="Integration name (e.g., database, openai)")
    parser.add_argument("--tag", default=DEFAULT_TARGET_TAG, help=f"Target GitHub tag or branch (default: {DEFAULT_TARGET_TAG})")
    parsed_args = parser.parse_args(args)

    runner = LlamaTransductor(category=parsed_args.category, name=parsed_args.name, tag=parsed_args.tag)
    return CliTaskAdapter(runner.run)

@contract.cli(
    name=MODULE_NAMESPACE,
    args=["--category", "--name", "--tag"],
    tags=["llama", "trans"],
    entry="entry_task" 
)
def main(args=None):
    if args is not None:
        return entry_task(args)
        
    bound_args, remain = parse_local(sys.argv[1:])
    if bound_args.local:
        entry_task(remain).run()
    else:
        dispatch_cli(MODULE_NAMESPACE, entry_task, __file__)

if __name__ == "__main__":
    main()