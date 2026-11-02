class Fiber < Formula
  desc "AI Runtime Harness"
  homepage "https://github.com/wittgena/fiber"
  url "https://github.com/wittgena/fiber/archive/refs/tags/v1.1.4.8.tar.gz"
  sha256 "b9a15631373710a8b04616affd2dcbe318b160e2034c97852c629bbe062c43e3"

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

  def post_install
    # Ensure working and log directories exist to prevent launchd exit 78 on service start
    (var/"fiber").mkpath
    (var/"log/fiber").mkpath
  end

  def caveats
    <<~EOS
      To bootstrap your local workspace (~/.anchor/bound.json) and provision
      local node keys (~/.ssh/fiber_ed25519 & local_cipher.key), run once:

        #{opt_bin}/fiber init

      To start the Redis tunnel and background daemon service:
        brew services start redis
        brew services start wittgena/fiber/fiber
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
    system "#{bin}/fiber", "init"
    assert_predicate testpath/".anchor/bound.json", :exist?
    assert_predicate testpath/".ssh/fiber_ed25519", :exist?
    assert_predicate testpath/".ssh/fiber_ed25519.pub", :exist?
  end
end
