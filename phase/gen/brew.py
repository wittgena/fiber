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
BIND_MODULE = "xphi.node.space.bind.around"

FIBER_ROOT = resolve_path("fiber")

# Formula Template
FORMULA_TEMPLATE = """class {{PROJECT_NAME_TITLE}} < Formula
  desc "{{PROJECT_DESC}}"
  homepage "https://github.com/{{GITHUB_REPO}}"
  url "{{TARBALL_URL}}"
  sha256 "{{SHA256_HASH}}"
  
  depends_on "python@{{PYTHON_VERSION}}"

  def install
    # Inject pretend version for hatch-vcs (setuptools-scm) when building from tarball without .git
    ENV["SETUPTOOLS_SCM_PRETEND_VERSION"] = version.to_s
    ENV["LDFLAGS"] = "-Wl,-headerpad_max_install_names"

    # Create isolated virtualenv in libexec and install package
    system "python{{PYTHON_VERSION}}", "-m", "venv", libexec
    system libexec/"bin/pip", "install", "-U", "pip"
    system libexec/"bin/pip", "install", "."
    
    # Symlink CLI entrypoint into Homebrew PATH
    bin.install_symlink libexec/"bin/{{PROJECT_NAME_LOWER}}"
  end

  def post_install
    # Ensure working and log directories exist to prevent launchd exit 78 on service start
    (var/"{{PROJECT_NAME_LOWER}}").mkpath
    (var/"log/{{PROJECT_NAME_LOWER}}").mkpath
  end

  def caveats
    <<~EOS
      To complete local workspace binding (~/.anchor/bound.json) and provision
      your Zero-Env node keys (~/.ssh/fiber_ed25519 & local_cipher.key),
      run once in your terminal:

        #{opt_bin}/{{PROJECT_NAME_LOWER}} init

      To start the background daemon service (use full tap name to avoid core conflict):
        brew services start {{GITHUB_REPO}}/{{PROJECT_NAME_LOWER}}
    EOS
  end

  service do
    run [opt_bin/"{{PROJECT_NAME_LOWER}}", "daemon", "-s", "all"]
    keep_alive true
    working_dir var/"{{PROJECT_NAME_LOWER}}"
    environment_variables PATH: std_service_path_env
    log_path var/"log/{{PROJECT_NAME_LOWER}}/daemon.log"
    error_log_path var/"log/{{PROJECT_NAME_LOWER}}/daemon.error.log"
  end

  test do
    ENV["HOME"] = testpath
    system "#{bin}/{{PROJECT_NAME_LOWER}}", "init"
    assert_predicate testpath/".anchor/bound.json", :exist?
    assert_predicate testpath/".ssh/fiber_ed25519", :exist?
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
        .replace("{{BIND_MODULE}}", BIND_MODULE)
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