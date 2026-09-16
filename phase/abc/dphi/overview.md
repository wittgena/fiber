# fiber.phase.abc.dphi.overview
@desc: DPHI Universal Cryptographic Compute & Metering Gateway Architecture

## 1. 개요 (Abstract)

* DPHI Gateway는 연산 실행(Execution)과 정산(Settlement) 프로세스를 물리적, 논리적으로 분리하는 암호학적 계량 프록시 아키텍처이다.
* 특정 런타임 및 정산 계층에 종속되지 않는 표준 호환 인터페이스(OpenAI API 등)를 제공한다.
* 모든 마이크로 과금 정산은 인메모리(In-memory) 상에서 동적으로 처리되며, 암호학적 영수증(Receipts) 발행을 통해 무결성을 증명하는 투명 프록시(Transparent Proxy)로 동작한다.

## 2. 아키텍처 핵심 원칙 (Core Architectural Principles)

* **BYOC (Execution Agnosticism):** 투명한 리버스 프록시로 동작하며 특정 연산 런타임을 강제하지 않는다. 경계 인가(Perimeter authorization), 고주파 계량(High-frequency metering), 과금 서명 검증을 수행한 후 외부 비즈니스 로직 및 백엔드로 트래픽을 포워딩한다.
* **BYOS (Ledger Agnosticism):** 과금 세션을 캐노니컬 해시(`canonical_hash`, AuditReceipt)로 봉인 후 코어 엔진 내 처리를 종료한다. Egress Adapter 플러그인을 통해 RDBMS, 규제 보관소, 스마트 컨트랙트(EVM) 등의 외부 원장으로 증명 데이터를 비동기 라우팅하여 스토리지 지연을 분리한다.
* **Zero-Gas In-Memory Netting:** 오프체인 PTA(Phase Transition Anchor) 모델을 적용하여 모든 API 마이크로 과금(예: $0.0001 단위)을 인메모리 상에서 처리한다. 이를 통해 DB Row-locking 현상 및 외부 결제 게이트웨이의 가스비(Gas fees)를 배제한다.
* **Asynchronous I/O Decoupling:** 암호학적 처리 파이프라인과 네트워크 I/O를 분리한다. 원장 영속성(Ledger persistence) 처리는 Distributed Pub/Sub 기반의 백그라운드 큐를 통해 비동기 수행된다.

## 3. 핵심 엔진 컴포넌트 (Core Engine Components)

* **`edge.llm` (Zero-Trust LLM Gateway):** 산업 표준 API(OpenAI 규격)를 지원하는 라우터이다. LLM 요청을 WASM 커널의 연산 의도(Compute Intent)와 결속시킨다. 토큰 소모량을 내부 연산 단위(`Fuel`)로 변환하며, 예산 초과 감지 시 커넥션을 파이프라인 레벨에서 차단(Kill-switch)한다.
* **`dvm.wasm` (Deterministic WASM Sandbox):** 외부 런타임 및 컨테이너 의존성이 배제된 네이티브 WASM 격리 환경이다. `max_fuel` 파라미터를 통해 명령어를 계량하며, 메모리 경계 침범이나 무한 루프 시 커널 트랩(Trap) 및 종료를 트리거한다. I/O가 격리되어 상태 발산율(State divergence rate) 0.000%를 보장한다.
* **PTA Billing Adapter:** 중앙화된 락(Lock) 없이 동시성 상태 전환을 관리하고 병렬 API 요청의 미사용 잔고를 추적한다. Ed25519 서명 및 PTA 포인터 검증을 `< 1ms` 내에 수행한다.
* **Dynamic Metering Engine:** 외부 컴퓨팅 백엔드의 응답 메트릭을 인터셉트하여 실시간 PTA 차감을 실행한다. LLM 추론 토큰과 샌드박스 연산 사이클을 단일 `Fuel` 단위로 통합 처리한다.
* **Merkle Rollup Compressor:** 마이크로 과금 해시 데이터를 단일 캐노니컬 루트 해시(`canonical_hash`)로 병합하여 암호학적 증명인 `AuditReceipt`를 발행한다.

## 4. 통합 라이프사이클 및 API 규격 (Unified Lifecycle & API Specification)

* 상호 서명 검증 및 MitM 방어를 위해 모든 엔드포인트는 `VerifiedHttpClient` 기반 통신을 권장한다.
* **Step 1. Ingress & Pre-Auth:** 요청 헤더에 유효한 영수증(`X-X402-Receipt`)이 없을 경우, 403 에러 대신 `HTTP 402 Payment Required` 코드 및 x402 인보이스 챌린지를 반환하여 암호학적 결제를 유도한다.
* **Step 2. Execution Routing:**
* *Compute Mode (`POST /v1/public/agent/execute`):* 페이로드를 `dvm.wasm`으로 라우팅하여 에이전트 코드를 실행한다.
* *Proxy Mode:* 암호학적 봉투(Cryptographic envelope)를 제거하고 원본 요청을 구성된 백엔드로 포워딩한다.
* *LLM Gateway Mode (`POST /v1/chat/completions`):* 프롬프트 요청을 WASM 커널 인가와 결속시켜 LLM 파이프라인으로 전송한다.


* **Step 3. Post-Metering & Dynamic Deduction:** 백엔드 반환 응답에서 스트림 청크 단위로 사용량 메트릭을 파싱하여 실시간 PTA 차감을 실행한다.
* **Step 4. State Collapse & Topological Sealing:** 세션 종료 시 잔고를 병합하여 다중 서명 기반 `AuditReceipt`를 발행한다. LLM 모드의 경우, API 응답 JSON의 `system_fingerprint` 및 `usage` 객체 내에 감사 해시를 주입(Topological Sealing)하여 반환한다.
* **Step 5. Agnostic Egress:** 클라이언트에게 API 응답을 반환하는 동시에 롤업 데이터를 Egress Adapter로 비동기 전송한다.

## 5. 목표 성능 지표 (Target Performance Metrics)

본 지표는 DPHI의 Lock-free 메모리 아키텍처와 WASM AOT(Ahead-of-Time) 컴파일 최적화가 적용된 상태를 기준으로 하며, 컴퓨팅 최적화 노드(Compute-optimized Node) 환경에서의 p99(상위 99%) 타겟을 정의합니다.

* **WASM 인스턴스 콜드 부트 (Instantiation): `< 1ms`**
  * AOT 컴파일을 통해 네이티브 기계어로 사전 번역된 모듈을 메모리에 적재합니다. OS 레벨의 컨테이너(수백 ms)와 비교할 수 없는 마이크로초 단위의 샌드박스 프로비저닝을 보장합니다.
* **인메모리 상계 및 롤업 (Netting & Merkle Emission): `< 1ms`**
  * 중앙화된 DB 락(Lock)을 배제한 PTA 알고리즘을 통해, 고주파 마이크로 과금 차감 및 머클 트리 해시 갱신을 순수 메모리 대역폭 속도로 처리합니다.
* **프록시 체류 시간 (Pass-through Latency): `< 1 ms`**
  * Zero-copy I/O 및 비동기 네트워크 스택(e.g., io_uring/epoll)을 활용하여, HTTP 파싱 및 라우팅 오버헤드를 1ms 이내로 통제합니다.
* **암호학적 연산 오버헤드 (Cryptographic Overhead): `< 1 ms`**
  * SIMD 가속을 활용한 페이로드 정규화/해싱 및 Ed25519 다중 서명 검증을 엣지(Edge)에서 즉시 처리하여 외부 인증 서버 통신 병목을 제거합니다.
* **샌드박스 강제 종료 지연 (Kinetic Trap Latency): `< 1ms`**
  * 연산 예산(Fuel) 고갈 또는 메모리 경계 침범 시 하드웨어 트랩(SIGSEGV)을 활용하여 즉각적으로 WASM 컨텍스트를 파괴하고 자원을 회수합니다.
* **코어 처리량 (Throughput): `30,000+ TPS` (초기 벤치마크 기준)**
  * 원장 영속성(Persistence)과 암호학적 롤업을 Egress 큐로 완전 분리(Decoupling)하여, 단일 컴퓨팅 노드에서도 선형적인 성능 확장을 통해 초당 수만 건의 결제/라우팅 트랜잭션을 처리합니다.

## 6. 청산 엔진 특화 기능 스펙 (Settlement Engine-Specific Features)

* **크레딧 초기화 (PTA Bootstrap):**
* 생명주기 시작 전, 클라이언트의 선결제 자산(법정화폐 또는 암호화폐)을 시스템 내 초기 PTA 상태 포인터로 주입(Inject)하여 고주파 결제 대기 상태를 구성한다.

* **마이너스 PTA 및 지연 결제 (Overdraft & Deferred Billing):**
* 사전에 정의된 기업 화이트리스트 정책에 따라 신용 한도 초과(마이너스 잔액) 상태의 PTA 전환을 허용한다.
* 초과된 부채는 클라이언트의 암호학적 서명과 결속되며, 이후 Egress Adapter를 통해 월별 인보이스 발행 또는 스마트 컨트랙트 기반의 강제 청산(On-Chain Clearing)으로 사후 연계된다.


* **외부 응답 헤더 기반 동적 과금 (Standard Header Parsing):**
* 외부 백엔드 서버가 응답 헤더에 표준 사용량 규격(예: `X-Dphi-Consume-Fuel: 500`)을 명시하여 반환할 경우, 동적 계량 엔진이 이를 파싱하여 인메모리 차감을 수행한다.
* 해당 인터페이스를 통해 단일 차감 기준이 아닌 다차원 가격 정책 알고리즘을 시스템에 동적으로 적용할 수 있다.

* **O(log N) 기반 머클 트리 검증 (Merkle Tree Aggregation):**
* 롤업 압축 과정에서 개별 마이크로 과금 해시를 이진 트리(Binary Tree) 형태로 집계한다.
* 이를 통해 발행된 `AuditReceipt`는 결제 분쟁 및 규제 감사 발생 시 `O(log N)`의 시간 복잡도로 무결성 검증이 가능하도록 보장한다.