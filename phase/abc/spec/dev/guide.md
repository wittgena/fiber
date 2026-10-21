# fiber.phase.abc.spec.dev.guide
@desc: fiber install & dev guide

## 1. Directory Structure & Execution Modes

Fiber autonomously bifurcates its runtime topology into **DEV** or **USER** mode by introspecting its physical execution path at boot time (Self-Bifurcation).

### [DEV Mode] — `/usr/local/self`

- **Location:** Core repositories (`fiber`, `xphi`, etc.) coexist as sibling directories under `/usr/local/self/`.
- **Installation:** **Zero package installation (`pip install`) is required.**
  *(Note: If `uv pip install -e .` is executed for IDE indexing convenience, `hatch.py` detects DEV mode and dynamically injects local source bindings as `xphi @ file://...`)*
- **Execution:** Invoked directly as a Python module (`python -m`) from the workspace root (`/usr/local/self`).
- **Internal Mechanics (`around.py` & `resolver.py`):** Detecting that execution originates outside `site-packages`, the bootstrapper executes a three-stage isolation sequence:
  1. **Replicate & Relaunch:** Copies bootstrap modules into the local sandbox and overwrites the current process (`os.execvp`) with `PYTH_REPLICATED=1` to guarantee runtime isolation.
  2. **Topology Binding:** Scans sibling core directories and dynamically injects `xphi.pth` into the active virtual environment's `site-packages`.
  3. **SSOT Bootstrap:** Synthesizes `bound.json` to anchor virtual path aliases and namespace routing rules.
- **Sandbox Topology:** Isolates runtime state and cryptographic keys within `/usr/local/self/anchor/`.

### [USER Mode] — `~/fiber-user` & Homebrew Cellar

- **Location:** Isolated user workspace (`~/fiber-user`) for `uv pip` installations, or macOS Homebrew Cellar (`$(brew --prefix)/Cellar/fiber/.../libexec`) for standalone daemon deployments.
- **Installation:** Statically installed into `site-packages` via `uv pip install` or provisioned as an isolated binary tap via `brew install`.
- **Internal Mechanics (`resolver.py` & `hatch.py`):**
  - **Topology Alignment:** Uses `hatch-vcs` to treat Git history as the Single Source of Truth (SSOT). Official releases lock to synchronized tags, while dirty/development states bind to the active branch topology.
  - **Explicit Override:** Supports forced topology overrides or version mismatch testing via environment variables (`FIBER_BUILD_DIST`, `FIBER_XPHI_REMOTE_REF`, `FIBER_XPHI_LOCAL_REF`).
  - **Site-Packages Detection:** At runtime, `resolver.py` detects execution from within `site-packages` (whether inside a `pyenv` virtualenv or Homebrew's `libexec` runtime), traces installed package roots, and compiles the user-space topology.
- **Sandbox Topology:** Halts upward directory traversal and anchors the runtime sandbox at `~/.anchor/` (`~/.anchor/bound.json`).

---

## 2. Environment Setup & Execution Sequence

### Phase A: Runtime Isolation (`pyenv`)

```bash
# Provision physically separated virtual environments (Python >= 3.12, e.g., 3.13.2)
pyenv virtualenv 3.13.2 self
pyenv virtualenv 3.13.2 fiber-user

```

### Phase B: DEV Mode (Zero-Install, Self-Binding)

```bash
## 1. Enter workspace root
cd /usr/local/self

## 2. Bind DEV virtual environment
pyenv local self

## 3. Register Zsh Alias (Prevents binary shadowing against USER/Homebrew installations)
## Recommended in ~/.zprofile to strictly separate DEV entrypoint from global 'fiber' CLI
alias fiber-dev="python -m fiber.phase.cli.main"

## 4. Trigger self-replication & topology binding (around.py injects xphi.pth & anchor/bound.json)
fiber-dev --help

## 5. Provision local node identity & verify topology
fiber-dev sign init
fiber-dev sign status
## Confirm /usr/local/self/anchor/bound.json is generated

```

### Phase C: USER Mode via `uv pip` (SDK & Local Control Plane Simulation)

Allows deterministic testing of static package distributions across local, remote, or intentionally mismatched Git topologies.

```bash
## 1. Enter dedicated user sandbox & bind virtualenv
mkdir -p ~/fiber-user && cd ~/fiber-user
pyenv local fiber-user

## 2. Topology-Aligned Installation Scenarios (Select one based on verification target)

  ## [Scenario 1: Dirty Local] Bind directly to uncommitted local sibling sources
  ## (Default flow when installing from local path; targets local fiber and xphi directories)
  uv pip install /usr/local/self/fiber

  ## [Scenario 2: Remote Dist Simulation] Local fiber + Remote xphi
  ## (Builds fiber from local source while fetching synchronized tag/main of xphi from GitHub)
  FIBER_BUILD_DIST=1 uv pip install /usr/local/self/fiber

  ## [Scenario 3: Direct Remote Release] Clean installation from GitHub release tag
  ## (Downloads both fiber and xphi from remote repository without local source dependencies)
  uv pip install git+https://github.com/wittgena/fiber.git@v1.1.4.7

  ## [Scenario 4: Mismatch & Ref Locking] Forced cross-version compatibility testing
  ## (Locks xphi to a specific remote tag or local commit against latest local fiber)
  FIBER_XPHI_REMOTE_REF=v1.1.1 FIBER_BUILD_DIST=1 uv pip install /usr/local/self/fiber
  FIBER_XPHI_LOCAL_REF=v1.1.4.8 uv pip install /usr/local/self/fiber

## 3. Bootstrap workspace topology (~/.anchor/bound.json)
python -m xphi.node.space.bind.around

## 4. Provision Zero-Env node identity (~/.ssh/fiber_ed25519 & local_cipher.key)
fiber sign init
fiber sign status

```

### Phase D: USER Mode via macOS Homebrew Tap (Standalone Daemon & MCP Gateway)

Best suited for running `fiber daemon` as a background OS service to govern local AI agents (Cursor, Claude Code, `continue.dev`) without managing Python virtual environments.

```bash
## 1. Install Fiber and its isolated runtime in a single command
brew install wittgena/fiber/fiber

## 2. Initialize Zero-Env node identity & verify cryptographic keychain
fiber sign init
fiber sign status

## 3. Start background daemon service (Exposes :8356/v1 LLM & MCP Gateway)
brew services start wittgena/fiber/fiber

## Note: If Xcode Command Line Tools or macOS build metadata issues occur during tap build,
## fall back to the Phase C (uv pip) installation above.

```

---

## 3. Troubleshooting & Operational Notes

* **Build Error (`Unknown version source: vcs`):**
If `hatchling.plugin.exceptions.UnknownPluginError` occurs during installation, the VCS plugin is missing from the isolated build environment. Verify that `"hatch-vcs"` is explicitly listed under `[build-system] requires` in `pyproject.toml`.
* **Topology Mismatch Halt (`fatal: couldn't find remote ref refs/tags/...`):**
Occurs during `FIBER_BUILD_DIST=1` when the local `fiber` Git tag does not have a matching counterpart on the remote `xphi` repository. This is an intentional fail-fast guard against unaligned deployments. Synchronize repository tags or explicitly supply `FIBER_XPHI_REMOTE_REF`.
* **Shell Alias & PATH Shadowing:**
If `alias fiber="..."` is defined globally in `~/.zshrc` or if Homebrew's `/opt/homebrew/bin/fiber` precedes `pyenv` shims in `$PATH`, invoking `fiber` inside `~/fiber-user` may route to the wrong runtime. Keep DEV mode aliased to `fiber-dev` and verify active binary resolution via `which fiber`.
* **Permission Denied on `xphi.pth` Injection:**
In DEV mode, `around.py` writes `xphi.pth` directly into the active environment's `site-packages`. Ensure a user-writable `pyenv` virtualenv (`self`) is active rather than the system/global Python installation.
* **Debugging Virtual Path Resolution (`bound.json`):**
`bound.json` (`anchor/bound.json` in DEV, `~/.anchor/bound.json` in USER/Homebrew) is the SSOT that translates logical URIs (`:io:`, `:workspace:`, `sign`) into physical filesystem paths. Inspect this file first when diagnosing path resolution or module discovery anomalies.

---

## 4. Architecture & Self-Bifurcation Topology

Rather than relying on static configuration files, Fiber introspects its execution location at boot time and dynamically constructs its sandbox and virtual filesystem bindings:

```mermaid
flowchart TD
    Start([Execution Trigger]) --> CheckLocation{Introspect Execution Path\n(Inside site-packages?)}

    %% DEV Mode Flow
    CheckLocation -->|No: Raw Source Tree| DevMode[DEV Mode Detected\n/usr/local/self]
    
    subgraph DEV [DEV Workspace: /usr/local/self]
        direction TB
        DevMode --> CheckRep{PYTH_REPLICATED=1\n(Already Replicated?)}
        CheckRep -->|No| Copy[Replicate bootstrap into anchor/]
        Copy --> Relaunch(os.execvp: Relaunch process from sandbox copy)
        Relaunch -.-> CheckRep
        
        CheckRep -->|Yes| BindDev[Auto-scan sibling core directories]
        BindDev --> MakePTH(Inject dynamic xphi.pth into virtualenv)
        BindDev --> MakeBoundDev(Generate /usr/local/self/anchor/bound.json)
    end

    %% USER Mode Flow (uv pip & Homebrew)
    CheckLocation -->|Yes: Installed Package| UserMode[USER Mode Detected\n~/fiber-user or Homebrew Cellar]
    
    subgraph USER [USER Runtime: uv pip Virtualenv / Homebrew libexec]
        direction TB
        UserMode --> BindUser[Trace installed core package paths]
        BindUser --> MakeBoundUser(Generate ~/.anchor/bound.json)
    end

    %% Convergence to SSOT
    MakeBoundDev --> SSOT
    MakeBoundUser --> SSOT

    subgraph Runtime [Virtual Topology & Keychain]
        SSOT[(bound.json\nSingle Source of Truth)] --> VirtFS[Resolve Virtual Paths & Namespaces\n:io:, :workspace:, sign, xphi:heartbeat]
        VirtFS --> Keychain[Node Identity & Vault\n~/.ssh/fiber_ed25519 & local_cipher.key]
    end
```