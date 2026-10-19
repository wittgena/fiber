# fiber.infra.mcp.mapper.config
## @lineage: fiber.gateway.mcp.mapper.config
import os
import sys
import copy
import shlex
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

import orjson
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("mcp.mapper.config")


@dataclass(frozen=True)
class MappedMcpEntry:
    target_id: str
    exec_cmd: str
    env: Dict[str, str] = field(default_factory=dict)
    source_agent: str = "custom"


DEFAULT_MCP_MAPPER_RULES: Dict[str, Any] = {
    "root_keys": ["mcpServers", "servers", "context_servers"],
    "ignore_rules": {
        "targets": ["fiber-gateway", "fiber_gateway"],
        "cmd_substrings": ["/v1/mcp-gateway"],
    },
    "extraction_rules": {
        "url": ["url", "serverUrl", "uri", "transport.url"],
        "command": ["command.path", "command.command", "command", "transport.command"],
        "args": ["command.args", "args", "transport.args"],
        "env": ["command.env", "env", "transport.env"],
        "disabled": ["disabled"],
    },
    "agent_profiles": {
        "fiber": {
            "markers": ["fiber-mcp.json", ".anchor"],
            "paths": {"*": ["{cwd}/fiber-mcp.json", "{home}/.anchor/mcp.json"]},
        },
        "claude-code": {
            "markers": [".mcp.json", ".claude.json"],
            "paths": {"*": ["{cwd}/.mcp.json", "{home}/.claude.json"]},
        },
        "claude-desktop": {
            "markers": ["claude_desktop_config.json"],
            "paths": {
                "darwin": ["{home}/Library/Application Support/Claude/claude_desktop_config.json"],
                "win32": ["{appdata}/Claude/claude_desktop_config.json"],
                "linux": ["{home}/.config/Claude/claude_desktop_config.json"],
            },
        },
        "cursor": {
            "markers": [".cursor"],
            "paths": {"*": ["{cwd}/.cursor/mcp.json", "{home}/.cursor/mcp.json"]},
        },
        "windsurf": {
            "markers": ["windsurf", "mcp_config.json"],
            "paths": {"*": ["{home}/.codeium/windsurf/mcp_config.json"]},
        },
        "vscode": {
            "root_key": "servers",
            "markers": [".vscode"],
            "paths": {
                "darwin": ["{cwd}/.vscode/mcp.json", "{home}/Library/Application Support/Code/User/mcp.json"],
                "win32": ["{cwd}/.vscode/mcp.json", "{appdata}/Code/User/mcp.json"],
                "linux": ["{cwd}/.vscode/mcp.json", "{home}/.config/Code/User/mcp.json"],
            },
        },
        "zed": {
            "root_key": "context_servers",
            "markers": [".zed"],
            "paths": {
                "darwin": ["{cwd}/.zed/settings.json", "{home}/.config/zed/settings.json"],
                "win32": ["{cwd}/.zed/settings.json", "{appdata}/Zed/settings.json"],
                "linux": ["{cwd}/.zed/settings.json", "{home}/.config/zed/settings.json"],
            },
        },
    },
}


class McpConfigMapper:
    _rules: Optional[Dict[str, Any]] = None

    @classmethod
    def get_rules(cls) -> Dict[str, Any]:
        if cls._rules is not None:
            return cls._rules

        rules = copy.deepcopy(DEFAULT_MCP_MAPPER_RULES)
        custom_path = os.environ.get("FIBER_MCP_MAPPER_RULES_PATH")
        if custom_path:
            p = Path(custom_path)
            if p.is_file():
                try:
                    ext = orjson.loads(p.read_bytes())
                    if isinstance(ext, dict):
                        cls._deep_merge(rules, ext)
                        log.info(f"[ConfigMapper] Merged external mapper rules from {p}")
                except Exception as e:
                    log.warning(f"[ConfigMapper] Ignored invalid rules file {p}: {e}")
        cls._rules = rules
        return cls._rules

    @classmethod
    def _deep_merge(cls, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                cls._deep_merge(base[k], v)
            else:
                base[k] = v

    @staticmethod
    def _traverse(data: Dict[str, Any], paths: List[str], expected_type: type) -> Any:
        for path_expr in paths:
            curr: Any = data
            for part in path_expr.split("."):
                if isinstance(curr, dict) and part in curr:
                    curr = curr[part]
                else:
                    curr = None
                    break
            if curr is not None and isinstance(curr, expected_type):
                return curr
        return None

    @classmethod
    def _is_ignored(cls, target_id: str, exec_cmd: str) -> bool:
        ignore = cls.get_rules().get("ignore_rules", {})
        if target_id.lower() in {str(t).lower() for t in ignore.get("targets", [])}:
            return True
        return any(sub in exec_cmd for sub in ignore.get("cmd_substrings", []))

    @classmethod
    def _resolve_candidate_paths(cls) -> Dict[str, List[Path]]:
        rules = cls.get_rules()
        plat = "darwin" if sys.platform == "darwin" else ("win32" if sys.platform == "win32" else "linux")
        home = Path.home()
        tokens = {
            "home": str(home),
            "cwd": str(Path.cwd()),
            "appdata": os.environ.get("APPDATA", str(home / "AppData" / "Roaming")),
        }

        resolved: Dict[str, List[Path]] = {}
        for agent, profile in rules.get("agent_profiles", {}).items():
            path_map = profile.get("paths", {})
            raw_list = path_map.get(plat, []) + path_map.get("*", [])
            resolved[agent] = [Path(tmpl.format(**tokens)) for tmpl in raw_list]
        return resolved

    @classmethod
    def discover_config_files(cls, agent: Optional[str] = None) -> List[Tuple[str, Path]]:
        candidates = cls._resolve_candidate_paths()
        target_agents = [agent.lower()] if agent else list(candidates.keys())
        seen_paths: set[Path] = set()
        found: List[Tuple[str, Path]] = []

        for ag in target_agents:
            for path in candidates.get(ag, []):
                if path.is_file():
                    resolved_p = path.resolve()
                    if resolved_p not in seen_paths:
                        seen_paths.add(resolved_p)
                        found.append((ag, path))
        return found

    @classmethod
    def parse_file(cls, config_path: Path, agent_hint: Optional[str] = None) -> List[MappedMcpEntry]:
        try:
            raw_data = orjson.loads(config_path.read_bytes())
        except Exception as e:
            log.error(f"[ConfigMapper] Failed to parse JSON from {config_path}: {e}")
            return []

        if not isinstance(raw_data, dict):
            return []

        source = agent_hint or cls._detect_agent_format(config_path, raw_data)
        servers_map = cls._extract_servers_map(raw_data)

        return [
            mapped
            for target_id, spec in servers_map.items()
            if isinstance(spec, dict) and (mapped := cls._normalize_entry(str(target_id), spec, source))
        ]

    @classmethod
    def scan_and_parse(cls, agent: Optional[str] = None) -> List[MappedMcpEntry]:
        merged: Dict[str, MappedMcpEntry] = {}
        for ag_name, path in cls.discover_config_files(agent):
            for entry in cls.parse_file(path, agent_hint=ag_name):
                if entry.target_id not in merged:
                    merged[entry.target_id] = entry
                    log.info(f"[ConfigMapper] Discovered [{entry.target_id}] from {ag_name} ({path})")
        return list(merged.values())

    @classmethod
    def _detect_agent_format(cls, path: Path, data: Dict[str, Any]) -> str:
        profiles = cls.get_rules().get("agent_profiles", {})
        path_tokens = {p.lower() for p in path.parts} | {path.name.lower()}

        for agent, prof in profiles.items():
            root_key = prof.get("root_key")
            if root_key and root_key in data:
                return agent
            if any(m.lower() in path_tokens for m in prof.get("markers", [])):
                return agent
        return "standard-mcp"

    @classmethod
    def _extract_servers_map(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        for key in cls.get_rules().get("root_keys", []):
            if isinstance(data.get(key), dict):
                return data[key]
        return data

    @classmethod
    def _normalize_entry(cls, target_id: str, spec: Dict[str, Any], source: str) -> Optional[MappedMcpEntry]:
        ext = cls.get_rules()["extraction_rules"]

        if cls._traverse(spec, ext.get("disabled", ["disabled"]), bool) is True:
            return None

        raw_env = cls._traverse(spec, ext["env"], dict) or {}
        env_map = {str(k): str(v) for k, v in raw_env.items()}

        url_val = cls._traverse(spec, ext["url"], str)
        cmd_bin = cls._traverse(spec, ext["command"], str)

        if url_val:
            exec_cmd = url_val.strip()
        elif cmd_bin:
            raw_args = cls._traverse(spec, ext["args"], list) or []
            exec_cmd = shlex.join([cmd_bin, *(str(a) for a in raw_args)])
        else:
            return None

        if cls._is_ignored(target_id, exec_cmd):
            log.debug(f"[ConfigMapper] Skipped self-referential gateway entry: [{target_id}]")
            return None

        return MappedMcpEntry(
            target_id=target_id,
            exec_cmd=exec_cmd,
            env=env_map,
            source_agent=source,
        )