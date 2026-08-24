# phase.doc.arch.overview
@lineage: dphi.pdoc.arch.overview
@lineage: abc.dphi.arch.overview
@desc: DPHI Universal Cryptographic Compute & Metering Gateway Architecture

---

## 1. Abstract (개요)

본 문서는 결정론적(Deterministic) 제로 트러스트 연산 게이트웨이이자 초고주파(Ultra-high-frequency) 암호학적 계량 프록시 역할을 수행하는 DPHI Gateway의 아키텍처를 정의한다. 워크로드 실행(Workload execution)을 호스트 OS의 신뢰 가정(Trust assumptions)으로부터 분리하여, 레거시 프록시 아키텍처가 가진 보안 및 과금 구조의 한계를 극복한다.

본 아키텍처의 핵심 철학은 **실행과 정산의 완전한 분리(Absolute Decoupling of Execution and Settlement)**, 그리고 마찰 없는 프로토콜 통합(Frictionless Protocol Integration)이다. DPHI는 클라이언트에게 무엇을 실행할지, 어디서 정산할지 강제하지 않는다. 산업 표준(OpenAI API 등)을 그대로 수용하는 표준 호환 인터페이스(Standard-Compliant Interface)를 통해 불변의 수학적 증명을 강제하며, 인메모리(In-memory) 상에서 마이크로 비용을 동적으로 정산하고 수학적으로 위조 불가능한 영수증(Receipts)을 발행하는 투명한 프록시(Transparent Proxy)로 동작한다.

---

## 2. Core Architectural Principles (핵심 원칙)

DPHI 아키텍처는 다음의 결정론적 원칙을 통해 엄격한 물리적 제한과 암호학적 보장을 제공한다:

* **Bring Your Own Compute (BYOC) - Execution Agnosticism:** DPHI는 투명한 리버스 프록시로 동작하며 특정 런타임을 강제하지 않는다. 기업은 기존의 AI 모델, 로드 밸런서, 백엔드 서버를 그대로 유지할 수 있다. DPHI는 경계 인가(Perimeter authorization), 고주파 계량(High-frequency metering), 암호학적으로 서명된 과금만을 엄격히 처리하며, 검증된 트래픽만 핵심 비즈니스 로직으로 전달한다.
* **Bring Your Own Settlement (BYOS) - Ledger Agnosticism:** 과금 세션이 정산되어 `canonical_hash` (AuditReceipt)로 봉인되면 코어 엔진의 역할은 종료된다. 플러그인 형태의 Egress Adapters를 통해 이 수학적 증명을 내부 RDBMS (PostgreSQL), 규제 보관소(Compliance vaults), Web3 스마트 컨트랙트(EVM) 등 어떠한 타겟으로든 라우팅할 수 있다. 이를 통해 고속 과금 처리와 스토리지 지연(Storage latency)을 완전히 분리한다.
* **Zero-Gas In-Memory Netting:** 모든 마이크로 트랜잭션(예: $0.0001 단위의 토큰별 LLM API 과금)은 오프체인 UTXO (Unspent Transaction Output) 모델을 기반으로 철저히 인메모리에서 연산된다. 실행 상태를 묶고 정리하여 기존 데이터베이스의 Row-locking 병목 현상과 결제 게이트웨이의 최소 수수료 문제를 제거한다. 중간 가스비(Gas fees) 없이 노드당 100,000+ TPS 이상의 코어 청산 처리량을 달성한다.
* **Asynchronous I/O Decoupling:** 스레드 블로킹 없이 높은 처리량을 유지하기 위해, 암호학적 파이프라인과 네트워크 I/O를 엄격히 분리한다. 데이터 롤업 및 원장 영속성(Ledger persistence) 확보는 Distributed Pub/Sub를 통한 백그라운드 큐로 위임된다.

---

## 3. Core Engine Components (핵심 엔진 컴포넌트)

* **Zero-Trust LLM Gateway (`edge.llm`) [NEW]:** 산업 표준 API(OpenAI 규격)를 완벽하게 지원하는 투명한 보안 라우터. 클라이언트의 비결정론적 LLM 요청을 WASM 커널의 결정론적 연산 의도(Compute Intent)와 결속시킨다. 토큰 소모량을 내부 연산 단위인 WASM Fuel과 등가 교환하여 처리하며, 예산 초과(예: 무한 루프, 프롬프트 폭탄) 감지 시 파이프라인 레벨에서 스트림 연결을 즉각적으로 안전하게 차단(Physical Kill-switch)한다.
* **Deterministic WASM Sandbox (`dvm.wasm`):** 모든 연산을 네이티브 WASM 런타임에 격리하여 Python 런타임 및 Docker 컨테이너 의존성을 완전히 배제한다. 정밀한 명령어 계량을 위해 `max_fuel`을 도입했다. 메모리 경계 침범(Boundary breaches) 또는 무한 루프 발생 시 커널 레벨의 트랩(Trap) 및 자동 종료가 트리거된다. I/O가 격리된 메모리 공간에서만 실행되어, 이기종 CPU 아키텍처 간 수백만 사이클 동안 0.000%의 상태 발산율(State divergence rate)을 보장한다.
* **UTXO Billing Adapter:** 중앙화된 락(Lock) 없이 동시성 상태 전환을 관리하며, 수천 개의 병렬 API 요청에 걸쳐 미사용 잔고(Fuel/Credit)를 추적한다. 요청을 수학적으로만 평가하여 Ed25519 서명 및 UTXO 포인터를 `< 150 µs` 내에 검증한다.
* **Dynamic Metering Engine (Unified Economy):** 임의의 컴퓨팅 사용량을 실시간 UTXO 차감으로 변환한다. 외부 컴퓨팅 백엔드의 응답을 인터셉트하여 사용량 메트릭을 추출하며, 이때 LLM 추론 토큰(Cognitive Token)과 샌드박스 연산 사이클(Compute Cycle)을 단일한 `Fuel` 단위로 완벽하게 통합(Unification)하여 다룬다.
* **Merkle Rollup Compressor & Cryptographic Notarization:** 기존 데이터베이스 로깅을 코어 엔진이 생성하는 동기식 암호학적 증명으로 대체한다. 수만 개의 개별 마이크로 과금 해시를 단일 캐노니컬 루트 해시(`canonical_hash`)로 병합하여, 수학적으로 위조 불가능한 `AuditReceipt`를 발행한다.

---

## 4. The Unified Lifecycle & Public API Specification (통합 라이프사이클)

모든 엔드포인트는 `VerifiedHttpClient` 사용을 권장하여 서명을 상호 검증하고 중간자(MitM) 공격을 방어한다.

1. **Ingress & Pre-Auth (HTTP 402 Negotiation):** 클라이언트가 유효한 인증 키나 영수증(`X-X402-Receipt`) 없이 시스템에 접근할 경우, 403(Forbidden) 에러로 단순 거부하는 대신 웹 표준인 **`HTTP 402 Payment Required`** 상태 코드와 함께 L402 인보이스 챌린지를 반환한다. 이를 통해 클라이언트 SDK는 기존 코드를 수정할 필요 없이, 백그라운드에서 자연스럽게 암호학적 결제 핸드셰이크를 수행할 수 있다.
2. **Execution Routing (Sandbox vs. Proxy vs. LLM):**
* *Compute Mode:* `POST /v1/public/agent/execute`를 통해 페이로드를 `dvm.wasm`으로 라우팅하여 서드파티 에이전트 코드를 안전하게 실행한다.
* *Proxy Mode:* DPHI가 암호학적 봉투(Cryptographic envelope)를 제거하고 원본 요청을 기업의 기존 백엔드로 포워딩한다.
* *LLM Gateway Mode:* `POST /v1/chat/completions`를 통해 들어온 표준 프롬프트 요청을 WASM 커널 인가와 결속시켜 LLM 파이프라인으로 전송한다.


3. **Post-Metering & Dynamic Deduction:** 엔터프라이즈 백엔드 또는 LLM 공급자가 응답과 사용량 메트릭을 DPHI 파이프라인으로 반환한다. Dynamic Metering Engine은 스트림 청크 단위로 사용량을 파싱하고 인메모리 상에서 실시간 UTXO 차감(Fuel Deduction)을 실행한다.
4. **State Collapse & Topological Sealing:** 세션 종료 시 UTXO Adapter는 남은 잔고를 병합하고 다중 서명으로 봉인된 `AuditReceipt`를 발행한다. LLM Gateway Mode의 경우, 커널이 발급한 감사 해시(Audit Hash)와 과금 내역을 표준 응답 JSON의 `system_fingerprint` 및 `usage` 객체 내부에 투명하게 포함(Topological Sealing)하여 에이전트에게 반환함으로써 기존 규격을 해치지 않고 응답의 무결성을 증명한다.
5. **Agnostic Egress:** 실제 API 응답을 클라이언트에게 반환함과 동시에, 롤업 데이터를 구성된 Egress Adapter로 넘겨 백엔드 스토리지에 비동기 저장한다.

---

## 5. Agnostic Egress & Adapter Matrix (어댑터 매트릭스)

* **`adapter.egress.rdbms` (PostgreSQL, Oracle):** Web2 SaaS 과금을 위한 기존 월별 인보이싱 처리를 목적으로 롤업 해시를 관계형 DB에 저장한다.
* **`adapter.egress.vault` (Enterprise Private Vaults):** 증명을 비공개로 유지하며, EU AI Act 준수를 위한 법적 감사(Audit) 시에만 접근을 허용한다.
* **`adapter.egress.da` (Celestia, EigenDA):** 합의 오버헤드(Consensus overhead) 없이 분산 네트워크를 위한 퍼블릭 데이터 가용성(DA)을 제공하며, Web3 및 DePIN 유스케이스에 적합하다.
* **`adapter.egress.evm` (Ethereum, Base, Arbitrum):** 무신뢰 스마트 컨트랙트 정산을 위해 순 부채 영수증(Net debt receipt)을 표준 EVM `calldata` 형식으로 포맷팅한다. L1/L2 스마트 컨트랙트를 통해 확정된 부채를 비동기적으로 정산(Deferred pull settlement)한다.

---

## 6. Target Performance Metrics (목표 성능 지표)

* **Microsecond Instantiation:** 최적화된 마이크로 VM의 일반적인 ~50ms 초기화 오버헤드를 우회하여 `< 500 µs`의 콜드 부트 지연 시간(Cold boot latency) 달성.
* **Proxy & LLM Pass-through Latency:** 표준 API 및 LLM 라우팅 과정에서 `< 500 µs`의 추가 오버헤드 발생.
* **Kinetic Trap Latency (Execution Termination):** 메모리 경계 침범, 무한 루프, 또는 LLM 스트림 예산(Fuel) 초과 감지 시 `< 100 µs` 내에 스트림 파괴 및 커널 레벨 종료 트리거.
* **Cryptographic Overhead:** 100KB 페이로드 당 정규화(Canonicalization) 및 해싱 연산에 `< 200 µs` 소요. 다중 서명 합의 검증에 `< 150 µs` 소요.
* **Merkle Emission:** 머클 경로 방출(Merkle path emission)은 `< 500 µs` 내 완료.
* **Cryptographic Netting Latency:** 상태 얽힘(State entanglement) 및 UTXO 업데이트 당 `< 1 ms` 소요.
* **Core Throughput:** 단일 노드 당 초당 `100,000+` 상태 전환(TPS) 초과. 기존 데이터베이스의 Row-locking 지연에 영향을 받지 않음.