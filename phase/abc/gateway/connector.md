# fiber.phase.abc.gateway.connector
**@desc:** Fiber Protocol Specification: Egress Architecture & Canonical A2A Workload Topologies

## 0. 개요 (Abstract)

본 명세는 Fiber 생태계의 Egress 아키텍처를 정의하며, `worker.connector` 및 세 가지 표준 워크로드 아키타입(`ephemeral`, `linear`, `multiplex`)을 중심으로 전개됩니다. 본 아키텍처는 격리된 형태의 레거시 Web2 시스템을 A2A(Agent-to-Agent) 경제망 내의 결정론적(Deterministic)이고 과금 가능한(Monetizable) 노드로 연합(Federate)하기 위한 구조적 방법론을 확립합니다. 이 모든 통합 과정은 기반 비즈니스 로직에 대한 **무수정(Zero-code modification)** 원칙을 엄격하게 준수합니다.

---

## 1. 엣지 커넥터 (`worker.connector`): 물리-논리적 경계 계층

`worker.connector`는 비동기적 인텐트 메시(DPHI Core)와 동기/비동기적 물리적 레거시 프로세스 간의 임피던스 불일치(Impedance Mismatch)를 해결하는 락-프리(Lock-free) 동시성 중개자입니다.

### 1.1. 제로 트러스트 네트워크 통합 (Zero-Trust Network Integration)

* **아웃바운드 풀 기반 NAT 순회 (Outbound Pull & NAT Traversal):** 커넥터는 인바운드 방화벽 개방을 요구하지 않고, DPHI 버스(`TunnelFactory`)를 향한 아웃바운드 터널링을 통해 구독(Subscribe) 모델로 작동합니다. 이는 제로 마찰 보안 태세(Zero-friction Security Posture)를 보장하며, 인프라 운영자의 네트워크 컴플라이언스 오버헤드를 완벽히 제거합니다.
* **I/O 추상화 및 프로세스 격리 (I/O Abstraction & Process Isolation):** 레거시 애플리케이션은 벤더 종속적인 SDK나 네트워킹 라이브러리를 요구하지 않습니다. 커넥터는 격리된 서브프로세스를 생성하고 오직 표준 입출력 파이프(`stdin`/`stdout`)를 통해서만 IPC(Inter-Process Communication)를 수행하여, 통신 로직과 비즈니스 실행 컨텍스트를 물리적으로 분리합니다.

### 1.2. 3단계 동시성 토폴로지 (Tri-Track Concurrency Topologies)

타겟 워커(Worker)의 특성 및 I/O 바운드 유무에 따라 커넥터는 다음 3가지 실행 토폴로지 모드를 동적으로 라우팅합니다:

1. **`ephemeral` (결함 격리 샌드박스 - Fault-Isolated Sandbox):**
유입되는 인텐트(Intent) 단위로 독립적인 서브프로세스를 인스턴스화하고, 트랜잭션 종료 시 즉각적으로 파괴(Terminate)합니다. 메모리 누수 및 상태 오염을 원천 차단하는 단발성(Single-use) 실행 환경을 보장하여, 상태 보존(Stateful) 상호작용 및 파괴적 연산 처리에 특화되어 있습니다.
2. **`linear` (순차적 큐 데몬 - Sequential Queue Daemon):**
무거운 초기화 오버헤드(Init Tax)를 지닌 연산 집약적 C/C++ 바인딩 환경(예: QuantLib, Numpy)에 최적화된 모드입니다. 단일 프로세스를 데몬(Daemon) 형태로 상주시키고, 커넥터 레벨의 동기화 락(Mutex Lock)을 통해 STDIN 버퍼에 요청을 선형적(Linear)으로 주입함으로써 콜드 스타트(Cold Start)를 제거한 초고속 순차 처리를 달성합니다.
3. **`multiplex` (비동기 다중화 데몬 - Asynchronous Multiplexing Daemon):**
외부 API 호출 등 네트워크 I/O 지연(Latency)이 발생하는 고도화된 워커를 위한 모드입니다. 단일 데몬 내부의 이벤트 루프(Event Loop)와 `req_id` 기반의 비동기 메시지 라우팅을 활용하여, 논블로킹(Non-blocking) 기반의 극단적인 병렬 동시 처리량(High-Concurrency Throughput)을 지원합니다.

### 1.3. 양방향 인텐트 메시 (Bidirectional Intent Mesh)

* **상태 유예 및 복원 (SUSPEND & YIELD):** 레거시 프로세스가 비동기적 인간 개입(Elicitation, 예: TOTP 인증)을 요구할 때, 커넥터는 STDOUT을 인터셉트(Intercept)하여 해당 프로세스를 메모리에 유예(Parked)시키고 코어 원장에 `YIELD` 상태를 씰링(Sealing)합니다. 이후 인가된 페이로드가 유입되면 `RESUME` 신호를 통해 트랜잭션을 멱등하게 복원합니다.
* **RPC 위임 대행 (RPC Delegation):** 샌드박스 내부의 에이전트가 외부 상태를 질의해야 할 경우, 커넥터는 `rpc_delegate` 페이로드를 인터셉트하여 에이전트 대신 코어 망(Core Ledger/Peer Nodes)에 RPC를 요청하고, 해석된 응답을 프로세스의 STDIN으로 재주입합니다.

---

## 2. 표준 워크로드 아키타입 (Canonical Workload Archetypes)

복잡도를 지닌 어떠한 레거시 시스템도 다음 3가지 토폴로지 아키타입 중 하나로 수렴됩니다. Egress 커넥터는 이들을 A2A 생태계의 자율 노드로 완벽하게 변환합니다.

### Archetype Alpha: 상태 유지형 제어 및 Human-in-the-Loop (`agent.deploy`)

**목적:** 자율 에이전트에 의한 파괴적 인프라 조작(Destructive Infrastructure Manipulation) 시 수반되는 운영 리스크의 통제 및 격리.

* **토폴로지 매핑:** `ephemeral` 모드 + 상태 보존형 트랜잭션(Stateful Transaction)
* **메커니즘:** 운영 DB에 대한 `DROP/TRUNCATE` 등의 인텐트 수신 시, 워커는 실행을 중단하고 승인된 관리자의 6자리 TOTP를 요구하는 `elicitation`을 발행합니다.
* **아키텍처 가치:** 레거시 CLI 스크립트를 비동기적 코드 재작성 없이 '인간의 합의를 대기하는 유한 상태 기계(FSM)'로 승격시킵니다. 인프라 운영자는 절대적 시스템 주권(Sovereignty)을 유지한 상태로 에이전트를 프로덕션에 배포할 수 있습니다.

### Archetype Beta: 무상태 결정론적 연산 데몬 (`agent.finlib`)

**목적:** WASM 샌드박스의 물리적 제약(C-Bindings 부재)을 우회하고 시스템적 연산 자원 낭비를 최소화.

* **토폴로지 매핑:** `linear` 모드 + 무상태 순수 연산(Stateless Pure Compute)
* **메커니즘:** 금융 연산 엔진을 메모리에 사전 적재(Pre-load)하여 초기화 오버헤드를 중화합니다. 다차원 행렬 연산과 엄격한 AST(Abstract Syntax Tree) 화이트리스트 검증을 순차적 큐잉으로 처리하여 IPC 지연을 0(Zero)에 가깝게 수렴시킵니다.
* **아키텍처 가치:** A2A 생태계의 연산 자원 최적화(Compute Resource Optimization)를 주도합니다. 에이전트는 무거운 부동소수점 연산에 자체 Fuel(가스비)을 소진하는 대신, 해당 노드에 마이크로페이먼트(x402)를 전송하여 "결정론적으로 합의된 수학적 진실(Mathematical Truth)"만을 획득합니다.

### Archetype Gamma: 합의 방어 및 암호학적 증명 (`agent.oracle`)

**목적:** 고빈도 에이전트 스웜(High-frequency Agent Swarms)의 Thundering Herd(대규모 동시 접속) 조건으로부터 외부 의존성 API를 보호.

* **토폴로지 매핑:** `multiplex` 모드 + 국지적 합의 캐싱(Local Consensus Caching, TTL 1.0s)
* **메커니즘:** 비동기 I/O 기반으로 다수의 외부 엔드포인트(Binance 등)를 병렬 페치(Fetch)하고, 획득한 데이터를 암호학적으로 씰링합니다. 초단기 TTL을 활용하여 인메모리 상에서 수백 건의 동시 질의를 논블로킹으로 방어합니다.
* **아키텍처 가치:** 극단적 동시성에 대한 인프라 단위의 완화(Mitigation) 전략입니다. 단 한 번의 물리적 네트워크 I/O로 Ed25519 기반의 암호학적 증명(Cryptographic Attestation)을 생성하여, 전체 에이전트 스웜에 무결점 데이터를 브로드캐스팅할 수 있습니다.

---

## 3. 종합 및 시스템적 수렴 (Synthesis & Systemic Convergence)

`worker.connector`와 3대 워크로드 아키타입은 파편화된 레거시 인프라스트럭처를 범용 A2A 네트워크로 연합(Federate)하기 위한 표준 프레임워크를 제공합니다. 본 아키텍처는 다음과 같은 경제적/구조적 효율성을 시스템 레벨에서 강제합니다:

1. **개발 상호운용성 (Developmental Interoperability):** 기존의 레거시 코드베이스를 3가지 아키타입(ephemeral, linear, multiplex)의 특성에 맞춰 매핑함으로써, 기반 로직의 구조적 리팩토링 없이 A2A 경제에 즉각적으로 편입될 수 있습니다.
2. **보안 및 컴플라이언스 (Security & Compliance):** NAT 순회, 샌드박스 결함 격리, DPoP 및 x402 결제 유효성 검증 등의 모든 암호학적 오버헤드를 엣지 노드(Edge Node)가 전담하므로, 내부 인프라의 외부 노출 및 보안 컴플라이언스 마찰이 원천 제거됩니다.
3. **경제적 결정론 (Economic Determinism):** 데몬 및 다중화를 통한 초기화 비용(Init Tax)의 완전한 제거와 락-프리(Lock-free) 기반의 O(1) 동시성 처리를 통해, 연산 노드의 한계 운영 비용(Marginal Cost)을 제로(0)에 수렴시키며 프로토콜 전반의 분산 라우팅 효율성을 극대화합니다.