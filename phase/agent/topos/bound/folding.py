# phase.agent.topos.bound.folding
## @lineage: agent.nexus.topos.bound.folding
## @lineage: nexus.agent.topos.bound.folding
## @lineage: meta.agent.topos.bound.folding
## @lineage: topos.bound.folding
## @lineage: kernel.bind.folding
import contextlib
import types
import inspect
import functools
import logging
from typing import Dict, Any
from fiber.phase.agent.topos.bound.transcript.spec import TranscriptSpec

log = logging.getLogger("bind.folding")

class FoldingBound:
    @staticmethod
    def _synthesize_tension_wrapper(func: callable, global_limit: int, node_rule: Dict[str, Any]):
        limit = node_rule.get("retry", global_limit)
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                re_entries = 0
                while re_entries < limit:
                    try:
                        return await func(self, *args, **kwargs)
                    except Exception as e:
                        re_entries += 1
                        log.warning(f"[Tension Spike] '{func.__name__}' Re-entry {re_entries}/{limit}. Cause: {e}")
                        if re_entries >= limit:
                            log.error(f"[Fatal Rupture] Bound collapsed at '{func.__name__}'.")
                            raise
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                re_entries = 0
                while re_entries < limit:
                    try:
                        return func(self, *args, **kwargs)
                    except Exception as e:
                        re_entries += 1
                        log.warning(f"[Tension Spike] '{func.__name__}' Re-entry {re_entries}/{limit}. Cause: {e}")
                        if re_entries >= limit:
                            log.error(f"[Fatal Rupture] Bound collapsed at '{func.__name__}'.")
                            raise
            return sync_wrapper

    @classmethod
    def fold_onto(cls, target_obj: Any, re_entry_limit: int, topology: Dict[str, Any] = None) -> tuple[Any, type]:
        """[Topological Folding] 객체의 위상을 분석하여 각 메서드에 지능형 보호막"""
        original_class = target_obj.__class__
        mutated_attrs = {}
        topology = topology or {}

        # 1. Topology에서 tension_map을 추출 (노드 ID가 무엇이든 유연하게 탐색)
        global_tension_map = {}
        for node_id, node_data in topology.items():
            node_spec = node_data.get("spec", {})
            if "tension_map" in node_spec:
                global_tension_map.update(node_spec["tension_map"])

        ## 클래스 내부의 메서드들을 순회하며 전사된 규칙과 대조
        for name, attr in original_class.__dict__.items():
            if isinstance(attr, types.FunctionType) and not name.startswith("__"):
                
                # 2. 메서드명(name)을 tension_map에서 바로 찾음
                node_rule = global_tension_map.get(name, {})
                
                log.debug(f"[Folding] Mapping rule to '{name}': {node_rule}")
                mutated_attrs[name] = cls._synthesize_tension_wrapper(attr, re_entry_limit, node_rule)

        ## 런타임에 변조된 클래스 생성 및 적용
        MutatedClass = type(f"FoldingBound{original_class.__name__}", (original_class,), mutated_attrs)
        target_obj.__class__ = MutatedClass
        return target_obj, original_class

@contextlib.contextmanager
def folding(*targets: Any, re_entry_limit: int = 3):
    restoration_stack = []
    mutated_targets = []
    transcript = TranscriptSpec(base_node=None)

    try:
        for target in targets:
            log.info(f"[Folding] Analyzing topology for {target.__class__.__name__}...")
            
            ## 전사(Transcription) 단계: 주석으로부터 위상 추출
            topology = {}
            if target.__doc__:
                try:
                    topology = transcript._reflect_source(target.__doc__, is_file=False)
                    log.info(f"  -> Topology materialized from docstring.")
                except Exception as e:
                    log.error(f"  -> Transcription failed: {e}. Falling back to default bound.")

            ## 접합(Folding) 단계: 추출된 topology 정보를 바탕으로 객체 변조
            mutated, original_class = FoldingBound.fold_onto(target, re_entry_limit, topology)
            mutated_targets.append(mutated)
            restoration_stack.append((mutated, original_class))
        yield tuple(mutated_targets) if len(mutated_targets) > 1 else mutated_targets[0]
    finally:
        for target, original_class in reversed(restoration_stack):
            target.__class__ = original_class
            log.info(f"[Unfolding] Bound dissolved for {original_class.__name__}.")
