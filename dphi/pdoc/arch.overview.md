# dphi.pdoc.arch.overview
@lineage: abc.dphi.arch.overview
@desc: DPHI Universal Cryptographic Compute & Metering Gateway Architecture

---

## 1. Abstract

본 문서는 deterministic Zero-Trust compute gateway이자 초고주파(ultra-high-frequency) cryptographic metering proxy 역할을 수행하는 DPHI Gateway의 아키텍처를 정의한다. Workload execution을 host OS의 trust assumptions으로부터 분리하여 레거시 proxy 아키텍처의 구조적 한계를 극복한다.

본 아키텍처의 핵심 철학은 **Execution과 Settlement의 완전한 분리(Absolute Decoupling)**, 그리고 마찰 없는 프로토콜 탈취(Frictionless Protocol Hijacking)이다. DPHI는 무엇을 실행할지, 어디서 정산할지 강제하지 않는다. 가장 널리 쓰이는 산업 표준(OpenAI API 등)을 그대로 수용하는 트로이 목마(Trojan Horse) 인터페이스를 통해 Immutable mathematical proofs를 강제하며, in-memory에서 micro-costs를 동적으로 정산하고 수학적으로 위조 불가능한 receipts를 발행하는 transparent proxy로 동작한다.

---

## 2. Core Architectural Principles (핵심 원칙)

DPHI 아키텍처는 다음의 deterministic 원칙을 통해 엄격한 물리적 제한과 암호학적 보장을 강제한다:

* **Bring Your Own Compute (BYOC) - Execution Agnosticism:** DPHI는 transparent reverse proxy로 동작하며 특정 runtime을 강제하지 않는다. 기업은 기존 AI models, load balancers, backend servers를 그대로 유지할 수 있다. DPHI는 perimeter authorization, high-frequency metering, cryptographically signed billing만을 엄격히 처리하며, 검증된 트래픽만 핵심 비즈니스 로직으로 전달한다.
* **Bring Your Own Settlement (BYOS) - Ledger Agnosticism:** Billing session이 정산되어 `canonical_hash` (AuditReceipt)로 봉인되면 Engine의 역할은 종료된다. Pluggable Egress Adapters를 통해 이 mathematical proof를 내부 RDBMS (PostgreSQL), compliance vaults, Web3 Smart Contracts (EVM) 등 어떤 타겟으로든 라우팅한다. 이를 통해 high-frequency billing과 storage latency를 완전히 분리한다.
* **Zero-Gas In-Memory Netting:** 모든 micro-transactions(예: $0.0001 단위의 token-by-token LLM API billing)는 off-chain UTXO (Unspent Transaction Output) 모델을 기반으로 strictly in-memory에서 연산된다. Execution states를 묶고 정리하여 기존 database row-locking 병목현상과 payment gateway의 최소 수수료 문제를 제거한다. Intermediate gas fees 없이 노드당 100,000+ TPS 이상의 core clearing throughput을 달성한다.
* **Asynchronous I/O Decoupling:** Thread blocking 없이 high-throughput capacity를 유지하기 위해 cryptographic pipeline과 network I/O를 엄격히 분리한다. Data rollup 및 ledger persistence는 Distributed Pub/Sub를 통한 background queues로 위임된다.

---

## 3. Core Engine Components (핵심 엔진 컴포넌트)

* **Zero-Trust LLM Gateway (`edge.llm`) [NEW]:** 세상에서 가장 널리 쓰이는 표준 API(OpenAI 규격)의 형태를 띠는 트로이 목마 라우터. 클라이언트의 비결정론적 LLM 요청을 WASM 커널의 결정론적 연산 의도(Compute Intent)로 종속시킨다. 토큰 소모량을 WASM Fuel과 등가 교환하여 처리하며, 예산 초과(OOM 프롬프트 폭탄 등) 감지 시 파이프라인 레벨에서 스트림 커넥션을 즉각 물리적으로 강제 절단(Physical Kill-switch)한다.
* **Deterministic WASM Sandbox (`dvm.wasm`):** 모든 연산을 native WASM runtime에 격리하여 Python runtime 및 Docker container 의존성을 완전히 배제한다. 정밀한 instruction metering을 위해 `max_fuel`을 도입했다. Memory boundary breaches 또는 infinite loops 발생 시 자동 kernel-level trap 및 termination이 트리거된다. I/O-segregated memory space에서만 실행되어, 이기종 CPU architectures 간 수백만 사이클 동안 0.000%의 state divergence rate를 보장한다.
* **UTXO Billing Adapter:** Centralized locks 없이 concurrent state transitions를 관리하며 수천 개의 병렬 API consumers에 걸쳐 unspent balances (Fuel/Credit)를 추적한다. Requests를 수학적으로만 평가하여 Ed25519 signatures 및 UTXO pointers를 `< 150 µs` 내에 검증한다.
* **Dynamic Metering Engine (Unified Economy):** 임의의 compute usage를 실시간으로 UTXO cost deductions로 변환한다. External compute backend의 responses를 인터셉트하여 usage metrics를 추출하며, 이때 LLM 추론 토큰(Cognitive Token)과 샌드박스 연산 사이클(Compute Cycle)을 단일한 `Fuel` 단위로 완벽하게 통합(Unification)하여 다룬다.
* **Merkle Rollup Compressor & Cryptographic Notarization:** 기존 database logging을 core `dphi.wasm` engine이 생성하는 synchronous cryptographic proofs로 대체한다. 수만 개의 개별 micro-billing hashes를 단일 Canonical Root Hash (`canonical_hash`)로 병합하여, 수학적으로 위조 불가능한 `AuditReceipt`를 발행한다.

---

## 4. The Unified Lifecycle & Public API Specification (통합 라이프사이클)

모든 endpoints는 `VerifiedHttpClient` 사용을 권장하여 signatures를 상호 검증하고 MitM 공격을 방어한다.

1. **Ingress & Pre-Auth (HTTP 402 Hijacking):** 클라이언트가 API Key나 유효한 영수증(`X-X402-Receipt`) 없이 시스템에 접근할 경우, 403 에러로 매몰차게 차단하는 대신 표준 **`HTTP 402 Payment Required`** 상태 코드와 함께 L402 인보이스 챌린지를 반환한다. 이는 외부 에이전트들이 시스템을 일반적인 엔드포인트로 착각하면서도 자연스럽게 암호학적 결제 핸드셰이크를 수행하도록 유도한다.
2. **Execution Routing (Sandbox vs. Proxy vs. LLM):**
* *Compute Mode:* `POST /v1/public/agent/execute`를 통해 payload를 `dvm.wasm`으로 라우팅하여 third-party agent code를 안전하게 실행한다.
* *Proxy Mode:* DPHI가 cryptographic envelope를 제거하고 raw request를 기업의 기존 backend로 포워딩한다.
* *LLM Gateway Mode:* `POST /v1/chat/completions`를 통해 들어온 표준 프롬프트 요청을 WASM 커널 인가와 결속시켜 LLM 파이프라인으로 전송한다.


3. **Post-Metering & Dynamic Deduction:** Enterprise backend 또는 LLM Provider(OpenAI, Anthropic 등)가 응답과 usage metadata를 DPHI 파이프라인으로 반환한다. Dynamic Metering Engine은 스트림 청크 단위로 usage를 파싱하고 in-memory 상에서 UTXO 실시간 차감(Fuel Deduction)을 실행한다.
4. **State Collapse & Topological Sealing:** Session 종료 시 UTXO Adapter는 남은 잔고를 병합하고, Multi-sig sealed `AuditReceipt`를 발행한다. LLM Gateway Mode의 경우, 커널이 발급한 감사 해시(Audit Hash)와 과금 내역을 표준 응답 JSON의 `system_fingerprint` 및 `usage` 객체 내부에 투명하게 은닉(Topological Sealing)하여 에이전트에게 반환함으로써 응답의 무결성을 증명한다.
5. **Agnostic Egress:** 실제 API response를 클라이언트에게 반환함과 동시에, Rollup Data를 구성된 Egress Adapter로 넘겨 backend storage에 비동기 저장한다.

---

## 5. Agnostic Egress & Adapter Matrix (어댑터 매트릭스)

* **`adapter.egress.rdbms` (PostgreSQL, Oracle):** Web2 SaaS Billing을 위한 기존 월별 인보이싱 처리를 목적으로 Rollup Hash를 relational row에 저장한다.
* **`adapter.egress.vault` (Enterprise Private Vaults):** Proofs를 비공개로 유지하며, EU AI Act Compliance를 위한 법적 audit 시에만 접근을 허용한다.
* **`adapter.egress.da` (Celestia, EigenDA):** Consensus overhead 없이 decentralized networks를 위한 public Data Availability (DA)를 제공하며, Web3 / DePIN 유스케이스에 적합하다.
* **`adapter.egress.evm` (Ethereum, Base, Arbitrum):** Trustless smart contract settlement를 위해 net debt receipt를 표준 EVM `calldata` 형식으로 포맷팅한다. L1/L2 smart contracts를 통해 확정된 debt를 비동기적으로 정산(deferred pull settlement)한다.

---

## 6. Target Performance Metrics (목표 성능 지표)

* **Microsecond Instantiation:** 최적화된 micro-VMs의 일반적인 ~50ms 초기화 오버헤드를 우회하여 `< 500 µs`의 cold boot latency 달성.
* **Proxy & LLM Pass-through Latency:** 표준 API 및 LLM 라우팅 과정에서 `< 500 µs`의 추가 overhead 발생.
* **Kinetic Trap Latency (Kill-switch):** Memory boundary breaches, 무한 루프, 또는 LLM 스트림 예산(Fuel) 초과 감지 시 `< 100 µs` 내에 스트림 파괴 및 kernel-level termination 트리거.
* **Cryptographic Overhead:** 100KB payload 당 Canonicalization 및 hashing compute에 `< 200 µs` 소요. Multi-signature consensus validation에 `< 150 µs` 소요.
* **Merkle Emission:** Merkle path emission은 `< 500 µs` 내 완료.
* **Cryptographic Netting Latency:** State entanglement 및 UTXO update 당 `< 1 ms` 소요.
* **Core Throughput:** 단일 노드 당 초당 `100,000+` state transitions (TPS) 초과. 기존 database row-locking latency의 영향을 전혀 받지 않음.