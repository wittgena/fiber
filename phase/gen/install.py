# fiber.phase.gen.install
import sys
import os
import stat
from pathlib import Path
from xphi.node.space.bind.resolver import resolve_path

# Configuration Constants
GITHUB_REPO = "wittgena/fiber"
PYTHON_VERSION = "3.12"
PROJECT_NAME_TITLE = "Fiber"
PROJECT_NAME_LOWER = "fiber"

FIBER_ROOT = resolve_path("fiber")
INSTALL_TEMPLATE = """#!/usr/bin/env bash
# ==============================================================================
# {{PROJECT_NAME_TITLE}} AI Runtime Harness (Linux / systemd)
#
# Steps:
# - Detect or install the 'uv' package manager
# - Install {{PROJECT_NAME_TITLE}} in an isolated uv tool environment
# - Bootstrap workspace anchor (bind.around) & Zero-Env keys (sign init)
# - Register user-space systemd service & launch background daemon
# ==============================================================================

set -e

# --- Color Formatting ---
GREEN='\\033[0;32m'
BLUE='\\033[0;34m'
YELLOW='\\033[1;33m'
RED='\\033[0;31m'
NC='\\033[0m'

echo -e "${BLUE}▶ Starting {{PROJECT_NAME_TITLE}} LLM Pipeline & MCP Gateway Installation...${NC}\\n"

# Parse optional target version (e.g., curl ... | bash -s -- v1.1.4.7)
FIBER_VERSION=${1:-""}
REPO_URL="git+https://github.com/{{GITHUB_REPO}}.git"

if [ -n "$FIBER_VERSION" ]; then
    INSTALL_TARGET="${REPO_URL}@${FIBER_VERSION}"
    echo -e "${BLUE}▶ Target Version: ${YELLOW}${FIBER_VERSION}${NC}\\n"
else
    INSTALL_TARGET="${REPO_URL}"
    echo -e "${BLUE}▶ Target Version: ${YELLOW}Latest (main branch)${NC}\\n"
fi

# Ensure 'uv' is installed
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}[1/5] 'uv' package manager not found. Installing uv...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
else
    echo -e "${GREEN}[1/5] 'uv' package manager is already installed.${NC}"
fi

# Install Fiber via uv tool
echo -e "${BLUE}[2/5] Installing {{PROJECT_NAME_TITLE}} into isolated sandbox via uv tool...${NC}"
uv tool install --python {{PYTHON_VERSION}} --force "$INSTALL_TARGET"

# Verify binary and resolve isolated Python interpreter path
FIBER_BIN="$HOME/.local/bin/{{PROJECT_NAME_LOWER}}"
if [ ! -f "$FIBER_BIN" ]; then
    echo -e "${RED}Error: {{PROJECT_NAME_TITLE}} binary not found at $FIBER_BIN. Installation failed.${NC}"
    exit 1
fi

UV_TOOL_DIR="$(uv tool dir 2>/dev/null || echo "$HOME/.local/share/uv/tools")"
FIBER_PYTHON="$UV_TOOL_DIR/{{PROJECT_NAME_LOWER}}/bin/python"
if [ ! -x "$FIBER_PYTHON" ]; then
    # Fallback: extract Python interpreter from entrypoint shebang
    FIBER_PYTHON="$(head -n 1 "$FIBER_BIN" | sed 's/^#!//')"
fi

echo -e "${GREEN}[2/5] {{PROJECT_NAME_TITLE}} installed successfully at $FIBER_BIN.${NC}"

# Bootstrap workspace anchor (bind.around) & local node keys (sign init)
echo -e "${BLUE}[3/5] Bootstrapping Anchor & Node Identity...${NC}"
"$FIBER_PYTHON" -m xphi.node.space.bind.around
"$FIBER_BIN" sign init
"$FIBER_BIN" sign status
echo -e "${GREEN}[3/5] Workspace anchor (~/.anchor/bound.json) and node keys (~/.ssh/fiber_ed25519 & local_cipher.key) provisioned.${NC}"

# Configure user-space systemd service
echo -e "${BLUE}[4/5] Configuring User-space Systemd Daemon (No sudo required)...${NC}"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_FILE="$SERVICE_DIR/{{PROJECT_NAME_LOWER}}.service"

mkdir -p "$SERVICE_DIR"

cat <<EOF > "$SERVICE_FILE"
[Unit]
Description={{PROJECT_NAME_TITLE}} LLM Pipeline Daemon
After=network.target

[Service]
ExecStart=$FIBER_BIN daemon -s all
Restart=always
RestartSec=5
Environment="PATH=$HOME/.local/bin:/usr/bin:/bin"

[Install]
WantedBy=default.target
EOF

echo -e "${GREEN}[4/5] Systemd service file created at $SERVICE_FILE.${NC}"

# Reload systemd & start daemon
echo -e "${BLUE}[5/5] Starting {{PROJECT_NAME_TITLE}} Daemon in the background...${NC}"

if command -v systemctl &> /dev/null && systemctl --user show-environment &> /dev/null; then
    systemctl --user daemon-reload
    systemctl --user enable --now {{PROJECT_NAME_LOWER}}.service
    echo -e "${GREEN}[5/5] Daemon successfully started and enabled on boot!${NC}"
else
    echo -e "${YELLOW}[!] Systemd is not running (e.g., inside basic WSL or Docker).${NC}"
    echo -e "You can manually start the daemon by running: ${BLUE}{{PROJECT_NAME_LOWER}} daemon -s all${NC}"
fi

echo -e "\\n============================================================"
echo -e "${GREEN}🎉 {{PROJECT_NAME_TITLE}} Installation & Key Provisioning Complete!${NC}"
echo -e "============================================================"
echo -e "  ${YELLOW}Check Identity      :${NC} {{PROJECT_NAME_LOWER}} sign status"
echo -e "  ${YELLOW}Check Daemon Status :${NC} systemctl --user status {{PROJECT_NAME_LOWER}}"
echo -e "  ${YELLOW}View Live Logs      :${NC} journalctl --user -fu {{PROJECT_NAME_LOWER}}"
echo -e "  ${YELLOW}Test Gateway        :${NC} curl -s http://127.0.0.1:8356/v1/mcp-gateway/tools"
echo -e "============================================================\\n"

if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    echo -e "${RED}[Warning] $HOME/.local/bin is not in your PATH.${NC}"
    echo -e "Please add it to your ~/.bashrc or ~/.zshrc:"
    echo -e "  ${BLUE}export PATH=\\"\\$HOME/.local/bin:\\$PATH\\"${NC}\\n"
fi
"""

def generate_install_script():
    output_path = FIBER_ROOT / "phase" / "install.sh"
    
    rendered_content = (
        INSTALL_TEMPLATE
        .replace("{{PROJECT_NAME_TITLE}}", PROJECT_NAME_TITLE)
        .replace("{{PROJECT_NAME_LOWER}}", PROJECT_NAME_LOWER)
        .replace("{{GITHUB_REPO}}", GITHUB_REPO)
        .replace("{{PYTHON_VERSION}}", PYTHON_VERSION)
    )
        
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(rendered_content)
        
    st = os.stat(output_path)
    os.chmod(output_path, st.st_mode | stat.S_IEXEC)
        
    try:
        rel_path = output_path.relative_to(FIBER_ROOT.parent)
    except ValueError:
        rel_path = output_path
    print(f"[+] Successfully generated Universal Installer at: {rel_path}")

if __name__ == "__main__":
    generate_install_script()