# fiber.phase.gen.brew
import sys
import urllib.request
import hashlib
from pathlib import Path
from xphi.node.space.bind.resolver import resolve_path

# =====================================================================
# Configuration Constants
# =====================================================================
GITHUB_REPO = "wittgena/fiber"
PYTHON_VERSION = "3.12"
PROJECT_NAME_TITLE = "Fiber"
PROJECT_NAME_LOWER = "fiber"
PROJECT_DESC = "The Universal Integration Boundary for heterogeneous AI ecosystems"
DEFAULT_VERSION = "v1.1.4.6"

FIBER_ROOT = resolve_path("fiber")

# =====================================================================
# Formula Template
# =====================================================================
FORMULA_TEMPLATE = """class {{PROJECT_NAME_TITLE}} < Formula
  desc "{{PROJECT_DESC}}"
  homepage "https://github.com/{{GITHUB_REPO}}"
  url "{{TARBALL_URL}}"
  sha256 "{{SHA256_HASH}}"
  
  depends_on "python@{{PYTHON_VERSION}}"

  def install
    # libexec에 격리된 venv 생성 및 파이썬 패키지 설치
    system "python{{PYTHON_VERSION}}", "-m", "venv", libexec
    system libexec/"bin/pip", "install", "-U", "pip"
    # 소스 디렉토리에서 패키지 설치
    system libexec/"bin/pip", "install", "."
    
    # pyproject.toml의 [project.scripts]에 의해 생성된 실행 파일을 PATH에 심볼릭 링크
    bin.install_symlink libexec/"bin/{{PROJECT_NAME_LOWER}}"
  end

  service do
    run [opt_bin/"{{PROJECT_NAME_LOWER}}", "daemon", "-s", "all"]
    keep_alive true
    environment_variables PATH: std_env_path
    log_path var/"log/{{PROJECT_NAME_LOWER}}/daemon.log"
    error_log_path var/"log/{{PROJECT_NAME_LOWER}}/daemon.error.log"
  end

  test do
    system "#{bin}/{{PROJECT_NAME_LOWER}}", "--help"
  end
end
"""

def generate_formula(version: str):
    tarball_url = f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{version}.tar.gz"
    output_path = FIBER_ROOT / f"{PROJECT_NAME_LOWER}.rb"
    
    print(f"[*] Target Version: {version}")
    print(f"[*] Downloading {tarball_url}...")
    
    try:
        response = urllib.request.urlopen(tarball_url)
        tar_data = response.read()
        sha256_hash = hashlib.sha256(tar_data).hexdigest()
        print(f"[*] Calculated SHA256: {sha256_hash}")
    except Exception as e:
        print(f"[!] Failed to download or calculate hash: {e}")
        sys.exit(1)
        
    # 템플릿 상수 치환 (Chain of replaces)
    rendered_content = FORMULA_TEMPLATE \
        .replace("{{PROJECT_NAME_TITLE}}", PROJECT_NAME_TITLE) \
        .replace("{{PROJECT_NAME_LOWER}}", PROJECT_NAME_LOWER) \
        .replace("{{PROJECT_DESC}}", PROJECT_DESC) \
        .replace("{{GITHUB_REPO}}", GITHUB_REPO) \
        .replace("{{PYTHON_VERSION}}", PYTHON_VERSION) \
        .replace("{{TARBALL_URL}}", tarball_url) \
        .replace("{{SHA256_HASH}}", sha256_hash)
        
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_content)
        
    # FIBER_ROOT를 기준으로 상대 경로 출력
    try:
        rel_path = output_path.relative_to(FIBER_ROOT.parent)
    except ValueError:
        rel_path = output_path
    print(f"[+] Successfully generated Homebrew formula at: {rel_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m fiber.phase.gen.brew <version>")
        print(f"Example: python -m fiber.phase.gen.brew {DEFAULT_VERSION}")
        sys.exit(1)
        
    target_version = sys.argv[1]
    generate_formula(target_version)