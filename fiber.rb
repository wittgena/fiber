class Fiber < Formula
  desc "The Universal Integration Boundary for heterogeneous AI ecosystems"
  homepage "https://github.com/wittgena/fiber"
  url "https://github.com/wittgena/fiber/archive/refs/tags/v1.1.4.7.tar.gz"
  sha256 "0239da342b14075265f32955f3884123a2eb9d4c2a1ec366b7f1acbf44410163"
  
  depends_on "python@3.12"

  def install
    # .git 폴더가 없는 tarball 빌드 시 hatch-vcs(setuptools-scm)에 버전 강제 주입
    ENV["SETUPTOOLS_SCM_PRETEND_VERSION"] = version.to_s

    # libexec에 격리된 venv 생성 및 파이썬 패키지 설치
    system "python3.12", "-m", "venv", libexec
    system libexec/"bin/pip", "install", "-U", "pip"
    # 소스 디렉토리에서 패키지 설치
    system libexec/"bin/pip", "install", "."
    
    # pyproject.toml의 [project.scripts]에 의해 생성된 실행 파일을 PATH에 심볼릭 링크
    bin.install_symlink libexec/"bin/fiber"
  end

  service do
    run [opt_bin/"fiber", "daemon", "-s", "all"]
    keep_alive true
    environment_variables PATH: std_env_path
    log_path var/"log/fiber/daemon.log"
    error_log_path var/"log/fiber/daemon.error.log"
  end

  test do
    system "#{bin}/fiber", "--help"
  end
end
