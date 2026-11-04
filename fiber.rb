class Fiber < Formula
  desc "Agent Runtime Harness"
  homepage "https://github.com/dphi-network/fiber"
  url "https://github.com/dphi-network/fiber/archive/refs/tags/v1.1.4.10.tar.gz"
  sha256 "57617721e0c443e12b45da09675a41e040116aba6318f1bd2911f38a34395237"

  depends_on "python@3.12"
  depends_on "redis"

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

  def caveats
    <<~EOS
      To bootstrap your local workspace (~/.anchor/bound.json) and provision
      local node keys (~/.ssh/fiber_ed25519 & local_cipher.key), run once:

        fiber init

      To start the Redis tunnel and background daemon service:
        brew services start redis
        brew services start dphi-network/fiber/fiber
    EOS
  end

  service do
    run [opt_bin/"fiber", "daemon"]
    keep_alive true
    working_dir var/"fiber"
    environment_variables PATH: std_service_path_env
    log_path var/"log/fiber/daemon.log"
    error_log_path var/"log/fiber/daemon.error.log"
  end

  test do
    ENV["HOME"] = testpath
    system bin/"fiber", "init"
    assert_path_exists testpath/".anchor/bound.json"
    assert_path_exists testpath/".anchor/origin.json"
    assert_path_exists testpath/".ssh/fiber_ed25519"
    assert_path_exists testpath/".ssh/fiber_ed25519.pub"
  end
end
