class Fiber < Formula
  desc "The Universal Integration Boundary for heterogeneous AI ecosystems"
  homepage "https://github.com/wittgena/fiber"
  url "https://github.com/wittgena/fiber/archive/refs/tags/v1.1.4.8.tar.gz"
  sha256 "b9a15631373710a8b04616affd2dcbe318b160e2034c97852c629bbe062c43e3"
  
  depends_on "python@3.12"

  def install
    # Inject pretend version for hatch-vcs (setuptools-scm) when building from tarball without .git
    ENV["SETUPTOOLS_SCM_PRETEND_VERSION"] = version.to_s
    ENV["LDFLAGS"] = "-Wl,-headerpad_max_install_names"

    # Create isolated virtualenv in libexec and install package
    system "python3.12", "-m", "venv", libexec
    system libexec/"bin/pip", "install", "-U", "pip"
    system libexec/"bin/pip", "install", "."
    
    # Symlink CLI entrypoint into Homebrew PATH
    bin.install_symlink libexec/"bin/fiber"
  end

  def post_install
    # Ensure working and log directories exist to prevent launchd exit 78 on service start
    (var/"fiber").mkpath
    (var/"log/fiber").mkpath
  end

  def caveats
    <<~EOS
      To complete local workspace binding (~/.anchor/bound.json) and provision
      your Zero-Env node keys (~/.ssh/fiber_ed25519 & local_cipher.key),
      run once in your terminal:

        #{opt_libexec}/bin/python -m xphi.node.space.bind.around
        #{opt_bin}/fiber sign init
        #{opt_bin}/fiber sign status

      To start the background daemon service (use full tap name to avoid core conflict):
        brew services start wittgena/fiber/fiber
    EOS
  end

  service do
    run [opt_bin/"fiber", "daemon", "-s", "all"]
    keep_alive true
    working_dir var/"fiber"
    environment_variables PATH: std_service_path_env
    log_path var/"log/fiber/daemon.log"
    error_log_path var/"log/fiber/daemon.error.log"
  end

  test do
    ENV["HOME"] = testpath
    system "#{libexec}/bin/python", "-m", "xphi.node.space.bind.around"
    system "#{bin}/fiber", "sign", "init"
    system "#{bin}/fiber", "sign", "status"
    assert_predicate testpath/".anchor/bound.json", :exist?
    assert_predicate testpath/".ssh/fiber_ed25519", :exist?
  end
end
