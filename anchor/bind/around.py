# anchor.bind.around
import os
import sys
import shutil
import json
import site
import logging
import subprocess
import importlib.util
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("around")

CURRENT_SCRIPT = Path(__file__).absolute()
CURRENT_DIR = CURRENT_SCRIPT.parent
PTH_FILENAME = "brane.pth"
CORES = ["brane", "surgent", "nexus", "theoria"]

## Default minimum skeleton to prevent resolver crashes if bound.json doesn't exist
DEFAULT_BOUND_SKELETON = {
    "name": "bound",
    "version": "1.5.1",
    "identity": {
        "manifold_id": 1,
        "vertex_id": 5
    },
    "substitution": {
        "io": "anchor/io",
        "memory": "anchor/memory",
        "workspace": "anchor/ext/workspace",
        "xor": "anchor/io/xor",
        "contract": "anchor/io/contract"
    },
    "paths": {
        "brane": "brane",
        "nexus": "nexus",
        "io": ":io:",
        "ext": ":anchor:/ext",
        "memory": ":anchor:/memory",
        "ledger": ":anchor:/ledger",
        "log": ":io:/log",
        "ailog": ":io:/ailog",
        "sandbox": ":workspace:/sandbox",
        "surface": ":io:/surface",
        "contract": ":contract:",
        "spec": ":contract:/spec",
        "scheme": ":contract:/scheme",
        "registry": ":contract:/registry",
        "xor": ":xor:",
        "lib": ":contract:/lib",
        "workspace": ":workspace:",
        "template": ":workspace:/template",
        "code": ":xor:/code",
        "res": ":io:/res"
    },
    "channels": {
        "namespaces": [
            "theoria", "brane", "nexus", "ext", "surgent", "psi", "delta", "xor", "watcher"
        ],
        "xphi": {
            "pattern": "^(theoria|psi|delta|meta|xphi|ion|ex|xe|xor|loop|field|watcher):"
        }
    }
}


def ignore_hidden(dir, files):
    """@helper: Exclude hidden files and directories from the copy target."""
    return [f for f in files if f.startswith('.')]

def determine_execution_mode() -> str:
    """
    @helper: Determine if the script is running from source (DEV) or as an installed package (USER).
    @desc: Checks if the script resides inside 'site-packages' or 'dist-packages'.
    """
    if any(part in ["site-packages", "dist-packages"] for part in CURRENT_SCRIPT.parts):
        return "USER"
    return "DEV"


def resolve_workspace_root() -> Path:
    """
    @helper: Dynamically resolve the workspace root (e.g., 'self/') without relying on cwd.
    @desc: Traverses upwards looking for a known core directory (e.g., 'brane' or 'theoria') to identify the root.
    """
    current = CURRENT_DIR
    while current.parent != current:
        if current.name in CORES:
            return current.parent
        current = current.parent
    
    # Fallback to current working directory if core marker is not found
    return Path.cwd()


def replicate_and_relaunch(workspace_root: Path, anchor_dir: Path) -> None:
    """
    @flow: Replicate the script to the root anchor directory and relaunch (DEV Mode Only).
    @desc: Resolves the hardcoded 'meta' dependency. Copies the logic into the workspace 'anchor' dynamically.
    """
    if os.getenv("PYTH_REPLICATED") == "1":
        return

    target_dir = anchor_dir / CURRENT_DIR.name
    
    # Skip if already running from the replicated target path
    if CURRENT_SCRIPT.is_relative_to(anchor_dir):
        log.info("[Phase: Skip Copy] Executed from target 'anchor' directly.")
        return

    log.info(f"[Phase: Copy] Replicating: {CURRENT_DIR} -> {target_dir}")
    try:
        shutil.copytree(CURRENT_DIR, target_dir, dirs_exist_ok=True, ignore=ignore_hidden, symlinks=True)
    except Exception as e:
        log.error(f"[Error] Unexpected error during copy: {e}")
        sys.exit(1)

    log.info(f"[Phase: Relaunch] Executing relaunched script from {target_dir}...\n")
    os.environ["PYTH_REPLICATED"] = "1"
    new_script = target_dir / CURRENT_SCRIPT.name
    os.execvp(sys.executable, [sys.executable, str(new_script)] + sys.argv[1:])


def discover_repos(base_dir: Path, max_depth: int = 1) -> list[Path]:
    """@flow: Scan surrounding directories to discover local Git repositories (DEV Mode)."""
    found = []
    exclude = {'.git', 'node_modules', 'venv', '__pycache__', 'build', '.idea', '.vscode'}
    
    def _scan(current: Path, depth: int):
        if depth > max_depth:
            return
        try:
            for child in current.iterdir():
                if child.is_dir() and child.name not in exclude:
                    if (child / '.git').exists():
                        found.append(child)
                    else:
                        _scan(child, depth + 1)
        except PermissionError:
            pass

    _scan(base_dir, 0)
    return sorted(list(set(found)))


def discover_user_repos() -> list[Path]:
    """@flow: Resolve installed package paths using importlib metadata (USER Mode)."""
    found = []
    for core in CORES:
        spec = importlib.util.find_spec(core)
        if spec and spec.submodule_search_locations:
            pkg_path = Path(spec.submodule_search_locations[0])
            found.append(pkg_path)
    return sorted(list(set(found)))


def update_bound_config(anchor_dir: Path, repos: list[Path]) -> None:
    """
    @task: Update the bound.json file dynamically.
    @desc: Creates a default skeleton if missing, and updates the 'around' topology with privilege info.
    """
    bound_path = anchor_dir / "bound.json"
    anchor_dir.mkdir(parents=True, exist_ok=True)
    
    config = {}
    if bound_path.exists():
        try:
            with open(bound_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
        except json.JSONDecodeError:
            log.warning(f"[Warning] Failed to parse {bound_path}, overwriting with default skeleton.")
            config = DEFAULT_BOUND_SKELETON.copy()
    else:
        log.info("[Phase: Sync] bound.json not found. Bootstrapping with default skeleton.")
        config = DEFAULT_BOUND_SKELETON.copy()

    new_around_map = {}
    for p in repos:
        repo_name = p.name
        is_core = repo_name in CORES
        
        # Persist data in a dictionary structure
        new_around_map[repo_name] = {
            "path": str(p.resolve()),
            "is_core": is_core,
            "allow_side_effects": is_core  # Core repositories are exempt from AST side-effect checks
        }
        
    config["around"] = new_around_map
    
    try:
        with open(bound_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        log.info(f"[Phase: Sync] Updated 'around' topology (Dict Mode) in: {bound_path}")
    except Exception as e:
        log.warning(f"[Warning] Failed to update bound.json: {e}")


def project_dev_mode(workspace_root: Path, anchor_dir: Path) -> list[Path]:
    """@install: Discover local repos, project paths into site-packages, and bind (DEV Mode)."""
    log.info(f"[Phase: Discovery] Scanning workspace around: {workspace_root} (DEV Mode)")
    repos = discover_repos(workspace_root, max_depth=2)
    
    if not repos:
        log.error("[Error] No valid local repositories found.")
        return []
    
    update_bound_config(anchor_dir, repos)
    
    # Write .pth file (based on path list)
    pth_content = "\n".join(str(p.resolve()) for p in repos)
    sp_paths = site.getsitepackages()
    site_packages = Path(sp_paths[0])
    pth_path = site_packages / PTH_FILENAME
    
    try:
        pth_path.write_text(pth_content, encoding="utf-8")
        log.info(f"[Phase: Bootstrap] {len(repos)} topos projected to: {pth_path}")
    except PermissionError:
        log.error("[Error] Permission denied: Run with elevated privileges (sudo/admin) to link .pth file.")
        sys.exit(1)

    for r in repos:
        is_core = "*" if r.name in CORES else " "
        log.info(f"  + [{is_core}] {r.name}")
        
    return repos


def project_user_mode(anchor_dir: Path) -> list[Path]:
    """@install: Discover installed packages and setup user-local workspace (USER Mode)."""
    log.info(f"[Phase: Discovery] Resolving installed packages (USER Mode)")
    repos = discover_user_repos()
    
    if not repos:
        log.warning("[Warning] No core packages found in the python environment.")
    
    update_bound_config(anchor_dir, repos)
    log.info(f"[Phase: Bootstrap] User environment configured at: {anchor_dir}")
    
    for r in repos:
        is_core = "*" if r.name in CORES else " "
        log.info(f"  + [{is_core}] {r.name}")
        
    return repos


def verify_projection(repos: list[Path], root_context: Path) -> None:
    """@helper: Verify if the projected paths exist within sys.path."""
    log.info("\n[Phase: Verification] Projected sys.path (filtered):")
    
    resolved_paths = [str(p.resolve()) for p in repos]
    script = f"""
import sys
valid_starts = {resolved_paths + [str(root_context)]}
for p in sys.path:
    if any(p.startswith(v) for v in valid_starts):
        print(p)
"""
    subprocess.run([sys.executable, "-c", script], check=True)


if __name__ == "__main__":
    MODE = determine_execution_mode()
    
    if MODE == "DEV":
        # Local source environment workflow
        WORKSPACE_ROOT = resolve_workspace_root()
        ANCHOR_DIR = WORKSPACE_ROOT / "anchor"
        
        replicate_and_relaunch(WORKSPACE_ROOT, ANCHOR_DIR)
        
        found_repos = project_dev_mode(WORKSPACE_ROOT, ANCHOR_DIR)
        if found_repos:
            verify_projection(found_repos, WORKSPACE_ROOT)
            
    else:
        # Installed package user environment workflow
        USER_ROOT = Path.cwd()
        ANCHOR_DIR = USER_ROOT / ".anchor"
        
        found_repos = project_user_mode(ANCHOR_DIR)
        if found_repos:
            verify_projection(found_repos, USER_ROOT)