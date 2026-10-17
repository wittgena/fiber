# fiber.phase.gen.brew
import sys
import urllib.request
import hashlib
from pathlib import Path
from xphi.node.space.bind.resolver import resolve_path

# Configuration Constants
GITHUB_REPO = "wittgena/fiber"
PYTHON_VERSION = "3.12"
PROJECT_NAME_TITLE = "Fiber"
PROJECT_NAME_LOWER = "fiber"
PROJECT_DESC = "The Universal Integration Boundary for heterogeneous AI ecosystems"
DEFAULT_VERSION = "v1.1.4.7"

FIBER_ROOT = resolve_path("fiber")

# Formula Template
FORMULA_TEMPLATE = """class {{PROJECT_NAME_TITLE}} < Formula
  desc "{{PROJECT_DESC}}"
  homepage "https://github.com/{{GITHUB_REPO}}"
  url "{{TARBALL_URL}}"
  sha256 "{{SHA256_HASH}}"
  
  depends_on "python@{{PYTHON_VERSION}}"

  def install
    # .git 폴더가 없는 tarball 빌드 시 hatch-vcs(setuptools-scm)에 버전 강제 주입
    ENV["SETUPTOOLS_SCM_PRETEND_VERSION"] = version.to_s

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
    tag_version = version if version.startswith("v") else f"v{version}"
    tarball_url = f"https://github.com/{GITHUB_REPO}/archive/refs/tags/{tag_version}.tar.gz"
    output_path = FIBER_ROOT / f"{PROJECT_NAME_LOWER}.rb"
    
    print(f"[*] Target Version: {tag_version}")
    print(f"[*] Downloading {tarball_url}...")
    
    try:
        req = urllib.request.Request(tarball_url, headers={"User-Agent": "Fiber-Formula-Generator"})
        with urllib.request.urlopen(req, timeout=30) as response:
            tar_data = response.read()
        sha256_hash = hashlib.sha256(tar_data).hexdigest()
        print(f"[*] Calculated SHA256: {sha256_hash}")
    except Exception as e:
        print(f"[!] Failed to download or calculate hash: {e}")
        sys.exit(1)
        
    rendered_content = (
        FORMULA_TEMPLATE
        .replace("{{PROJECT_NAME_TITLE}}", PROJECT_NAME_TITLE)
        .replace("{{PROJECT_NAME_LOWER}}", PROJECT_NAME_LOWER)
        .replace("{{PROJECT_DESC}}", PROJECT_DESC)
        .replace("{{GITHUB_REPO}}", GITHUB_REPO)
        .replace("{{PYTHON_VERSION}}", PYTHON_VERSION)
        .replace("{{TARBALL_URL}}", tarball_url)
        .replace("{{SHA256_HASH}}", sha256_hash)
    )
        
    output_path.write_text(rendered_content, encoding="utf-8")
        
    try:
        rel_path = output_path.relative_to(FIBER_ROOT.parent)
    except ValueError:
        rel_path = output_path
    print(f"[+] Successfully generated Homebrew formula at: {rel_path}")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python -m fiber.phase.gen.brew [version]")
        print(f"Example: python -m fiber.phase.gen.brew {DEFAULT_VERSION}")
        sys.exit(1)
        
    target_version = sys.argv[1] if len(sys.argv) == 2 else DEFAULT_VERSION
    generate_formula(target_version)