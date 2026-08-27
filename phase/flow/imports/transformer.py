# fiber.phase.flow.imports.transformer
## @lineage: phase.flow.imports.transformer
## @lineage: nexus.flow.imports.transformer
## @lineage: meta.flow.imports.transformer
## @lineage: meta.flow.trans.imports.transformer
## @lineage: meta.trans.imports.transformer
## @lineage: meta.model.trans.imports.transformer
## @lineage: arch.model.trans.imports.transformer
## @lineage: arch.contract.trans.imports.transformer
## @lineage: arch.gov.trans.imports.transformer
## @lineage: arch.topos.trans.imports.transformer
## @lineage: arch.topos.node.trans.imports.transformer
## @lineage: arch.topos.bound.trans.imports.transformer
## @lineage: arch.bound.trans.imports.transformer
## @lineage: arch.topic.imports.transformer
import os
import sys
import difflib
import argparse
import libcst as cst
from typing import List, Dict, Any, Tuple, Callable
from pathlib import Path

def node_to_str(node: cst.CSTNode) -> str:
    return cst.Module([]).code_for_node(node)

class ImportTransformer(cst.CSTTransformer):
    def __init__(self, old: str, new: str):
        self.old = old
        self.new = new

    def _should_replace(self, module_name: str) -> bool:
        return module_name == self.old or module_name.startswith(self.old + ".")

    def _replace(self, module_name: str) -> str:
        return self.new + module_name[len(self.old):]

    def leave_ImportFrom(self, original_node, updated_node):
        if original_node.module is None:
            return updated_node

        module_name = node_to_str(original_node.module)
        if self._should_replace(module_name):
            new_module = self._replace(module_name)
            return updated_node.with_changes(
                module=cst.parse_expression(new_module)
            )
        return updated_node

    def leave_Import(self, original_node, updated_node):
        new_names = []
        for alias in original_node.names:
            name_str = node_to_str(alias.name)
            if self._should_replace(name_str):
                new_name = self._replace(name_str)
                new_alias = alias.with_changes(name=cst.parse_expression(new_name))
                new_names.append(new_alias)
            else:
                new_names.append(alias)
        return updated_node.with_changes(names=new_names)

class RelativeImportTransformer(cst.CSTTransformer):
    def __init__(self, current_package: str):
        """:param current_package: 현재 파일이 속한 패키지의 절대 경로 (예: "title.a.b")"""
        self.current_package = current_package

    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.CSTNode:
        if not original_node.relative:
            return updated_node

        if not self.current_package:
            return updated_node

        dots = len(original_node.relative)
        parts = self.current_package.split(".")
        if dots > len(parts) + 1:
            return updated_node

        slice_idx = len(parts) - (dots - 1)
        base_pkg_parts = parts[:slice_idx]
        module_str = node_to_str(original_node.module) if original_node.module else ""
        if module_str:
            base_pkg_parts.append(module_str)

        absolute_module_name = ".".join(base_pkg_parts)
        return updated_node.with_changes(
            relative=[],
            module=cst.parse_expression(absolute_module_name) if absolute_module_name else None
        )