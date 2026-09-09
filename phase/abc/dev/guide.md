# fiber.phase.abc.dev.guide
@desc: fiber install & dev guide

## 1. Directory Structure & Execution Modes

본 프로젝트는 구동 환경(실행 위치 및 방식)에 따라 DEV와 USER 모드로 자가 분기(Self-Bifurcation)함.

### [DEV 모드] - /usr/local/self

* **위치:** `/usr/local/self/` 하위에 `fiber`, `xphi` 등 코어 프로젝트가 Sibling으로 존재.
* **설치:** **원칙적으로 별도의 패키지 설치(`pip install`)가 일절 필요 없음.**
*(단, 개발 편의를 위해 `pip install -e .` 실행 시, `hatch.py`가 이를 DEV 모드로 감지하여 `xphi @ file://...` 형태로 로컬 소스 바인딩을 자동 주입함)*
* **실행 방식:** 워크스페이스 루트(`/usr/local/self`)에서 `python -m`으로 모듈 직접 실행.
* **내부 기전 (`around.py` & `resolver.py`):** 소스 경로에서 직접 실행되므로 `site-packages` 내부 실행이 아님을 인지하고 다음 과정을 수행함.
1. **Replicate & Relaunch:** 구동 초기, 관련 모듈이 자신을 샌드박스 내부로 복사한 뒤 `PYTH_REPLICATED=1` 플래그와 함께 프로세스를 재실행(`os.execvp`)하여 런타임을 격리함.
2. **Topology Binding:** 주변 코어 디렉토리를 자동 스캔하여, 현재 가상환경의 `site-packages`에 `xphi.pth` 파일을 생성(동적 바인딩).
3. **SSOT Bootstrap:** 경로와 네임스페이스 통신 규칙이 담긴 `bound.json`을 자동 생성함.


* **위상(Sandbox):** 워크스페이스 내부에 `anchor/` 디렉토리를 생성하여 런타임 데이터를 격리.

### [USER 모드] - ~/fiber

* **위치:** 일반 유저의 설치 및 구동을 시뮬레이션하기 위한 격리 경로 (`~/fiber`).
* **설치 방식:** `pip install`을 통해 가상환경의 `site-packages`에 정적 설치.
* **내부 기전 (`resolver.py` & `hatch.py`):**
* **위상 정렬(Topology Alignment):** `hatch-vcs`를 통해 Git 형상을 단일 진실 공급원(SSOT)으로 삼음. 설치 시 억지스러운 통제 없이 자연스러운 흐름을 따름(정식 릴리즈는 태그 동기화, 개발 중 상태는 최신(Dirty/main) 위상으로 결합).
* 관찰자의 개입이 필요할 경우, 환경변수(`FIBER_BUILD_DIST`, `FIBER_XPHI_REMOTE_REF`, `FIBER_XPHI_LOCAL_REF`)를 통해 명시적 불일치(Mismatch) 결합이나 특정 브랜치 강제 라우팅을 수행함.
* 실행 시 코드가 `site-packages` 내부에 위치함을 감지하여 USER 모드로 동작하며, 패키지 경로를 추적하여 `bound.json` 토폴로지를 구성함.

* **Sandbox:** 상위 경로 탐색을 멈추고 강제로 홈 디렉토리로 점프하여 `~/.anchor/` 디렉토리를 생성.

---

## 2. Environment Setup & Execution Sequence

### Phase A: 환경 격리 (pyenv)

```bash
# Python 3.13.2 기준, self/user 런타임 물리적 분리
pyenv virtualenv 3.13.2 self
pyenv virtualenv 3.13.2 fiber-user
```

### Phase B: DEV 모드 (No Install, Self-binding)

```bash
## 1. 워크스페이스 루트 진입
cd /usr/local/self

## 2. self(dev) 가상환경 바인딩
pyenv local self

## 3. Zsh Alias 등록 (USER 모드와의 명령어 충돌 방지)
## ~/.zprofile 등에 아래 alias를 등록하여 DEV 모드 전용 명령어로 분리 사용 권장
alias fiber-dev="python -m fiber.phase.cli.main"

## 4. 즉시 실행 (around.py가 런타임에 pth 및 bound.json 자동 주입/재실행)
fiber-dev --help

## 5. 위상 검증
## /usr/local/self/anchor/ 디렉토리 및 내부에 bound.json이 정상 생성되었는지 확인
```

### Phase C: USER 모드 (Static Distribution Simulation)

USER 모드 테스트 시, 로컬 소스의 유무 및 관찰자의 목적에 따라 유연한 설치 정렬이 가능함.

```bash
## 1. 테스트 전용 샌드박스 진입
mkdir -p ~/fiber
cd ~/fiber

## 2. USER 가상환경 바인딩
pyenv local fiber-user
pip install --upgrade pip

## 3. 목적별 위상 정렬 설치 시나리오

  ## [시나리오 1: Dirty Local] 현재 수정 중인 로컬 소스(미커밋 포함) 그대로 연동
  ## (아무 옵션이 없을 때의 기본 흐름. fiber와 xphi 모두 로컬 물리 폴더를 타겟팅함)
  uv pip install /usr/local/self/fiber

  ## [시나리오 2: Remote Dist] 로컬 소스를 기반으로 원격 배포 상태 강제 시뮬레이션
  ## (fiber는 로컬을 쓰되, xphi는 원격(GitHub)의 동기화된 태그나 main 브랜치에서 가져옴)
  FIBER_BUILD_DIST=1 uv pip install /usr/local/self/fiber

  ## [시나리오 3: Direct Remote] 로컬 소스 없이 Github에서 직접 릴리즈 설치
  ## (임시 빌드 환경을 인지하여 fiber, xphi 모두 원격에서 다운로드)
  uv pip install git+https://github.com/wittgena/fiber.git@v1.1.2

  ## [시나리오 4: Mismatch & Locked] 특정 위상(태그/브랜치) 강제 불일치 테스트
  ## (fiber는 로컬 최신, xphi는 원격의 레거시(v1.0.0) 또는 로컬의 특정 커밋으로 강제 고정)
  FIBER_XPHI_REMOTE_REF=v1.0.0 FIBER_BUILD_DIST=1 uv pip install /usr/local/self/fiber
  FIBER_XPHI_LOCAL_REF=v1.1.2 uv pip install /usr/local/self/fiber

## 4. 설치된 CLI 실행
fiber --help

## 5. 위상 검증
## ~/.anchor/ 디렉토리 및 내부에 bound.json이 정상 생성되었는지 확인
```

---

## 3. Troubleshooting & Notes

* **빌드 오류 (Unknown version source: vcs):**
설치 시 `hatchling.plugin.exceptions.UnknownPluginError`가 발생한다면, 빌드 샌드박스에 VCS 플러그인이 누락된 것입니다. `pyproject.toml` 최상단의 `[build-system]` 섹션 `requires` 배열에 반드시 `"hatch-vcs"`가 포함되어 있는지 확인하십시오.
* **위상 불일치 붕괴 (fatal: couldn't find remote ref refs/tags/...):**
원격 배포 시뮬레이션(`FIBER_BUILD_DIST=1`) 중 이 오류가 발생한다면, `fiber`의 Git 태그와 `xphi` 원격 저장소의 Git 태그가 동기화되지 않은 것입니다. 이는 오류가 아니라 비정합적 결합을 차단하는 정상적인 방어 기전입니다. 양쪽 저장소의 태그를 일치시키거나, `FIBER_XPHI_REMOTE_REF`를 통해 위상을 명시적으로 재정렬하십시오.
* **Alias 섀도잉(Shadowing) 충돌 방지:**
전역 쉘 설정에 `alias fiber="..."`가 등록되어 있을 경우, USER 모드(`~/fiber`)에서 정상적으로 패키지를 설치해도 alias가 우선순위를 가져가 `ModuleNotFoundError`를 유발합니다. DEV 모드용 명령어는 `fiber-dev` 등으로 분리하여 운용하십시오.
* **권한 오류 (Permission Denied):**
DEV 모드 실행 시 가상환경의 `site-packages` 디렉토리에 `xphi.pth`를 기록하는 과정에서 권한 오류로 인해 스크립트가 강제 종료될 수 있습니다. 글로벌 파이썬 환경이 아닌, 권한이 온전히 확보된 독립 가상환경(`pyenv` 등)이 활성화되어 있는지 확인하십시오.
* **가상 경로 체계 디버깅:**
구동 후 샌드박스에 생성되는 `bound.json` 파일은 가상 파일 시스템 경로(`:io:`, `:workspace:` 등)를 실제 물리적 경로로 치환하는 단일 진실 공급원(SSOT)입니다. 경로 매핑이나 모듈 인식 문제가 발생할 경우, 최우선적으로 해당 파일의 위상 구조를 검증하십시오.

---

## 4. Architecture & Self-Bifurcation Topology

본 프로젝트는 정적인 설정 파일에 의존하지 않고, 런타임에 스스로의 물리적 위치를 인지하여 환경에 맞게 진화하는 **'자가 인지 및 위상 결합(Self-Bifurcation & Topology Binding)'** 기전을 가집니다. 아래 다이어그램은 실행 명령이 내려진 직후 시스템이 공간을 분기하고 샌드박스를 구축하는 흐름을 보여줍니다.

```mermaid
flowchart TD
    Start([Execution Trigger]) --> CheckLocation{자신의 위치 인지\n(site-packages 내부인가?)}

    %% DEV Mode Flow
    CheckLocation -->|No: 소스 코드 상태| DevMode[DEV 모드 감지\n/usr/local/self]
    
    subgraph DEV [DEV Workspace: /usr/local/self]
        direction TB
        DevMode --> CheckRep{PYTH_REPLICATED=1\n(복제되었는가?)}
        CheckRep -->|No| Copy[자신을 anchor/ 하위로 복제]
        Copy --> Relaunch(os.execvp: 복제본으로 프로세스 덮어쓰기 및 재실행)
        Relaunch -.-> CheckRep
        
        CheckRep -->|Yes| BindDev[주변 Sibling 디렉토리 자동 스캔]
        BindDev --> MakePTH(가상환경에 xphi.pth 동적 주입)
        BindDev --> MakeBoundDev(anchor/bound.json 생성)
    end

    %% USER Mode Flow
    CheckLocation -->|Yes: 설치된 패키지 상태| UserMode[USER 모드 감지\n~/fiber]
    
    subgraph USER [USER Workspace: ~/fiber]
        direction TB
        UserMode --> BindUser[가상환경 내 설치된 코어 패키지 추적]
        BindUser --> MakeBoundUser(~/.anchor/bound.json 생성)
    end

    %% Convergence to SSOT
    MakeBoundDev --> SSOT
    MakeBoundUser --> SSOT

    subgraph Runtime [Virtual Topology]
        SSOT[(bound.json\n단일 진실 공급원)] --> VirtFS[가상 경로 및 네임스페이스 치환\n:io:, :workspace:, xphi:heartbeat 등]
    end
```