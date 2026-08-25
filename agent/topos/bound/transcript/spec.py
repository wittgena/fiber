# agent.topos.bound.transcript.spec
## @lineage: phase.agent.topos.bound.transcript.spec
## @lineage: agent.nexus.topos.bound.transcript.spec
## @lineage: nexus.agent.topos.bound.transcript.spec
## @lineage: meta.agent.topos.bound.transcript.spec
## @lineage: topos.bound.transcript.spec
## @lineage: kernel.bind.transcript.spec
import re
import yaml
import os
from typing import Any, Dict, List, Optional

from xphi.arch.contract.registry.unified import contract
from xphi.arch.xor.parser.lang.md import MdAstParser
from xphi.arch.xor.parser.block.extractor import BlockExtractor
from fiber.agent.topos.bound.transcript.phi import TranscriptBase
from xphi.watcher.plane.emitter import get_logger

log = get_logger("transcript.spec")

@contract.ator("transcript.spec")
class TranscriptSpec(TranscriptBase):
    """@flow: Ψ(Markdown Document) → Φ(Extracted Topology) → Φ_materialized transformer"""
    RE_DEFINE_ATOR = re.compile(r'@define\.ator\(\s*["\']([^"\']+)["\']\s*\)')
    RE_DEFINE_REGIME = re.compile(r'@define\.regime\(\s*["\']([^"\']+)["\']\s*\)')
    RE_PROJECT_NODE = re.compile(r'@(project|contract)\.(regime|ator)\(\s*["\']([^"\']+)["\']\s*\)')
    RE_FLOW_MARKER = re.compile(r'@(phase\.flow|flow)')
    RE_SAFE_PROJECT = re.compile(
        r'@?(?:project\.|contract\.)?(ator|regime)(?:\s*\(\s*["\']?(.*?)["\']?\s*\))?',
        re.IGNORECASE
    )

    def __init__(self, base_node: Any):
        super().__init__(base_node)
        self.role = "spec_transcript"
        self.parser_cls = MdAstParser
        self.extractor = BlockExtractor()

    def _register_dynamic_ator(self, name: str, code: str, lang: str):
        log.info(f"    [Define] Registering dynamic ator: {name} (lang: {lang})")
        # TODO: 실제 구현 시 registry.register_dynamic_component 호출
        pass

    def _reflect_source(self, source: str, is_file: bool = True) -> Dict[str, Any]:
        md_text = self._load_source(source, is_file)
        
        # MdAstParser를 통한 AST 파싱 및 블록 추출
        doc = self.parser_cls(md_text, is_file=False).parse()
        blocks = self.extractor.extract(doc)
        log.info(f"  [Trace] Extracted {len(blocks)} blocks. Detailed Manifest:")
        for i, b in enumerate(blocks):
            b_type = getattr(b, 'type', getattr(b, 'block_type', 'UNKNOWN'))
            # 첫 줄 혹은 30자만 추출하여 가독성 확보
            snippet = str(getattr(b, 'content', getattr(b, 'text', ''))).replace('\n', '\\n')[:40]
            log.info(f"    Block[{i:02d}]: type='{b_type}' | content='{snippet}...'")
        
        defined_regimes: Dict[str, List[Any]] = {}
        xphi_topology: Dict[str, Dict[str, Any]] = {}

        self._hoist_definitions(blocks, defined_regimes)
        self._link_topos(blocks, defined_regimes, xphi_topology)
        return self._bind_sequence(xphi_topology, source)

    def _load_source(self, source: str, is_file: bool) -> str:
        if not is_file:
            return source
        if not os.path.exists(source):
            raise FileNotFoundError(f"[Φ:fracture] File not found: {source}")
        with open(source, 'r', encoding='utf-8') as f:
            return f.read()

    def _hoist_definitions(self, blocks: List[Any], defined_regimes: Dict[str, List[Any]]):
        ctx_mode, ctx_target = None, None
        for b in blocks:
            b_type = getattr(b, 'type', '').lower()
            content = getattr(b, 'content', getattr(b, 'text', ''))

            if b_type in ("heading", "header"):
                if match := self.RE_DEFINE_ATOR.search(content):
                    ctx_mode, ctx_target = "ator", match.group(1)
                elif match := self.RE_DEFINE_REGIME.search(content):
                    ctx_mode, ctx_target = "regime", match.group(1)
                    defined_regimes[ctx_target] = []
                else:
                    ctx_mode, ctx_target = None, None
            
            elif b_type in ("code", "python", "yaml", "json", "bash"):
                if ctx_mode == "ator":
                    self._register_dynamic_ator(ctx_target, content, b_type)
                    ctx_mode = None
                elif ctx_mode == "regime":
                    defined_regimes[ctx_target].append(b)

    def _link_topos(self, blocks: List[Any], defined_regimes: Dict[str, List], topology: Dict[str, Any]):
        ctx_node = None
        ctx_sub_mode = None
        pending_type = None  # 타입만 발견되고 ID를 기다리는 상태

        for i, b in enumerate(blocks):
            raw_type = getattr(b, 'block_type', getattr(b, 'type', ''))
            b_type = str(raw_type).lower()
            content = getattr(b, 'content', getattr(b, 'text', ''))
            if not content: continue
            content_stripped = content.strip()

            # 1. Heading 처리
            if b_type in ("heading", "header", "h1", "h2", "h3"):
                if match := self.RE_SAFE_PROJECT.search(content_stripped):
                    node_kind = match.group(1).lower()
                    node_id = match.group(2) # 괄호가 없으면 None이 됨
                    
                    if node_id:
                        # [DSL 모드] ### @ator("manifold_kernel") 형태
                        ctx_node = node_id
                        ctx_sub_mode = node_kind
                        topology[ctx_node] = {
                            "type": "ator", # Registry SSOT 정렬 (regime도 ator로 생성)
                            "spec": {"role": node_id, "flow": {}}
                        }
                        log.info(f"    [Block:{i:02d}] DISCOVERED (DSL): {ctx_node}")
                    else:
                        # [단순 모드] ### @ator 형태 -> 다음 YAML의 'role'을 기다림
                        pending_type = "ator"
                        ctx_node = None
                        log.info(f"    [Block:{i:02d}] PENDING TYPE: {node_kind}")
                
                elif "@flow" in content_stripped or "@phase.flow" in content_stripped:
                    ctx_sub_mode = "flow"

            # 2. 데이터 처리 (YAML)
            elif b_type in ("yaml", "json") and (ctx_node or pending_type):
                try:
                    parsed_data = yaml.safe_load(content_stripped)
                    if not isinstance(parsed_data, dict): continue

                    # PENDING 상태라면 YAML 내부의 role을 ID로 사용하여 노드 생성
                    if ctx_node is None and "role" in parsed_data:
                        ctx_node = parsed_data["role"]
                        topology[ctx_node] = {
                            "type": pending_type,
                            "spec": {"role": ctx_node, "flow": {}}
                        }
                        log.info(f"    [Block:{i:02d}] MATERIALIZED from YAML: {ctx_node}")

                    if ctx_node:
                        target_spec = topology[ctx_node]["spec"]
                        if ctx_sub_mode == "flow":
                            target_spec["flow"].update(parsed_data)
                        else:
                            target_spec.update(parsed_data)
                            if "role" not in target_spec: target_spec["role"] = ctx_node
                except Exception as e:
                    log.error(f"    [Block:{i:02d}] BIND ERROR: {e}")

    def _bind_sequence(self, topology: Dict[str, Any], source: str) -> Dict[str, Any]:
        nodes = list(topology.keys())
        if not nodes:
            raise ValueError(f"No valid XPHI topology found in: {source}")

        for i, node_id in enumerate(nodes):
            spec = topology[node_id]["spec"]
            # flow 내부에 next가 명시되지 않은 경우 순차 바인딩
            if "next" not in spec["flow"]:
                next_node = nodes[i + 1] if i < len(nodes) - 1 else "UGA"
                spec["flow"]["next"] = next_node
            
            # 최상위 spec에도 next 동기화 (접근 편의성)
            spec["next"] = spec["flow"]["next"]

        log.info(f"  [Trace] Topology Built: {nodes}")
        return topology