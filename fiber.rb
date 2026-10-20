class Fiber < Formula
  desc "The Universal Integration Boundary for heterogeneous AI ecosystems"
  homepage "https://github.com/wittgena/fiber"
  url "https://github.com/wittgena/fiber/archive/refs/tags/v1.1.4.7.tar.gz"
  sha256 "0239da342b14075265f32955f3884123a2eb9d4c2a1ec366b7f1acbf44410163"
  
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
    # Ensure log directory exists to prevent launchd exit 78 on service start
    (var/"log/fiber").mkpath
  end

  def caveats
    <<~EOS
      To complete local workspace binding and provision your Zero-Env node keys
      (~/.ssh/fiber_ed25519 & local_cipher.key), run once in your terminal:

        #{opt_libexec}/bin/python -m xphi.node.space.bind.around
        fiber sign init
        fiber sign status
    EOS
  end

  service do
    run [opt_bin/"fiber", "daemon", "-s", "all"]
    keep_alive true
    working_dir var
    log_path var/"log/fiber/daemon.log"
    error_log_path var/"log/fiber/daemon.error.log"
  end

  test do
    ENV["HOME"] = testpath
    system "#{libexec}/bin/python", "-m", "xphi.node.space.bind.around"
    system "#{bin}/fiber", "sign", "init"
    system "#{bin}/fiber", "sign", "status"
    assert_predicate testpath/".ssh/fiber_ed25519", :exist?
  end
end
