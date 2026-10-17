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

# Install Script Template (Bash)
INSTALL_TEMPLATE = """#!/usr/bin/env bash
# ==============================================================================
# {{PROJECT_NAME_TITLE}} Universal Installer (Linux / systemd)
# 
# 이 스크립트는 다음 작업을 수행:
# 1. 'uv' 패키지 매니저 감지 및 자동 설치
# 2. uv tool을 이용한 격리된 샌드박스(USER 모드) 환경에 {{PROJECT_NAME_TITLE}} 설치
# 3. User-space Systemd 서비스 등록 및 데몬 백그라운드 구동
# ==============================================================================

set -e

# --- Color Formatting ---
GREEN='\\033[0;32m'
BLUE='\\033[0;34m'
YELLOW='\\033[1;33m'
RED='\\033[0;31m'
NC='\\033[0m' # No Color

echo -e "${BLUE}▶ Starting {{PROJECT_NAME_TITLE}} LLM Pipeline & MCP Gateway Installation...${NC}\\n"

# 사용자가 버전을 명시했는지 확인 (예: curl ... | bash -s -- v1.1.4.6)
FIBER_VERSION=${1:-""}
REPO_URL="git+https://github.com/{{GITHUB_REPO}}.git"

if [ -n "$FIBER_VERSION" ]; then
    INSTALL_TARGET="${REPO_URL}@${FIBER_VERSION}"
    echo -e "${BLUE}▶ Target Version: ${YELLOW}${FIBER_VERSION}${NC}\\n"
else
    INSTALL_TARGET="${REPO_URL}"
    echo -e "${BLUE}▶ Target Version: ${YELLOW}Latest (main branch)${NC}\\n"
fi

# uv 설치 확인 및 자동 설치
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}[1/4] 'uv' package manager not found. Installing uv...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
else
    echo -e "${GREEN}[1/4] 'uv' package manager is already installed.${NC}"
fi

# uv tool을 이용해 Fiber 설치
echo -e "${BLUE}[2/4] Installing {{PROJECT_NAME_TITLE}} to isolated sandbox via uv tool...${NC}"
uv tool install --python {{PYTHON_VERSION}} --force "$INSTALL_TARGET"

# 실행 경로 검증
FIBER_BIN="$HOME/.local/bin/{{PROJECT_NAME_LOWER}}"
if [ ! -f "$FIBER_BIN" ]; then
    echo -e "${RED}Error: {{PROJECT_NAME_TITLE}} binary not found at $FIBER_BIN. Installation failed.${NC}"
    exit 1
fi
echo -e "${GREEN}[2/4] {{PROJECT_NAME_TITLE}} installed successfully at $FIBER_BIN.${NC}"

# User-space Systemd 서비스 등록
echo -e "${BLUE}[3/4] Configuring User-space Systemd Daemon (No sudo required)...${NC}"
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

echo -e "${GREEN}[3/4] Systemd service file created at $SERVICE_FILE.${NC}"

# 4. Systemd 데몬 리로드 및 시작
echo -e "${BLUE}[4/4] Starting {{PROJECT_NAME_TITLE}} Daemon in the background...${NC}"

if command -v systemctl &> /dev/null && systemctl --user show-environment &> /dev/null; then
    systemctl --user daemon-reload
    systemctl --user enable --now {{PROJECT_NAME_LOWER}}.service
    echo -e "${GREEN}[4/4] Daemon successfully started and enabled on boot!${NC}"
else
    echo -e "${YELLOW}[!] Systemd is not running (e.g., inside basic WSL or Docker).${NC}"
    echo -e "You can manually start the daemon by running: ${BLUE}{{PROJECT_NAME_LOWER}} daemon -s all${NC}"
fi

echo -e "\\n============================================================"
echo -e "${GREEN}🎉 {{PROJECT_NAME_TITLE}} Installation Complete!${NC}"
echo -e "============================================================"
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
    
    rendered_content = INSTALL_TEMPLATE \
        .replace("{{PROJECT_NAME_TITLE}}", PROJECT_NAME_TITLE) \
        .replace("{{PROJECT_NAME_LOWER}}", PROJECT_NAME_LOWER) \
        .replace("{{GITHUB_REPO}}", GITHUB_REPO) \
        .replace("{{PYTHON_VERSION}}", PYTHON_VERSION)
        
    with open(output_path, "w", encoding="utf-8", newline='\n') as f:
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