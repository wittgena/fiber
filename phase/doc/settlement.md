# phase.doc.settlement
@lineage: dphi.pdoc.settlement
> **@desc:** DPHI 범용 암호학적 계량 및 청산 아키텍처 (Universal Cryptographic Metering & Settlement Architecture)

---

## 1. 개요 (Abstract)

본 문서는 사용량 기반 AI 경제와 엔터프라이즈 마이크로 빌링(Micro-billing)을 위해 설계된 결정론적 초고빈도 암호학적 계량 및 청산 게이트웨이인 **DPHI Settlement Engine**의 아키텍처를 정의합니다.

이 아키텍처의 핵심 철학은 **'실행(Execution)'과 '청산(Settlement)'의 절대적 분리(Absolute Decoupling)**, 그리고 마찰 없는 프로토콜 통합(Frictionless Protocol Integration)입니다. DPHI는 "범용 암호학적 계량 프록시(Universal Cryptographic Metering Proxy)"로 작동합니다. 어떠한 컴퓨팅 환경이든 그 앞에 투명하게 배치되어, API 트래픽을 가로채고 인메모리 상에서 미세 비용을 동적으로 상계(Netting)한 뒤, 수학적으로 위조가 불가능한 영수증을 발행합니다.

**DPHI는 클라이언트에게 무엇을 실행할지, 어디에 청산할지 강제하지 않습니다.** 엔터프라이즈 기업들은 산업 표준(OpenAI 규격 등)을 통해 트래픽을 기존 자체 LLM이나 SaaS 백엔드로 라우팅(BYOC)할 수 있으며, 연산 결과로 발행된 암호학적 과금 증명을 기존 관계형 데이터베이스(RDBMS)부터 퍼블릭 Web3 네트워크까지 원하는 저장 매체나 원장(BYOS)에 아키텍처적 마찰 없이 자유롭게 저장할 수 있습니다.

---

## 2. 핵심 아키텍처 원칙 (Core Architectural Principles)

### 2.1. Bring Your Own Compute (BYOC) - 실행 환경의 불가지성

DPHI는 투명한 리버스 프록시(Transparent Reverse Proxy) 역할을 수행하며, 워크로드를 특정 런타임에 강제하지 않습니다. 기업은 기존의 AI 모델, 로드 밸런서, 백엔드 서버를 그대로 유지할 수 있습니다. DPHI는 오직 경계(Perimeter)에서의 인가(Authorization), 초고빈도 계량, 그리고 암호학적으로 서명된 빌링 처리만을 엄격하게 수행한 뒤, 검증된 트래픽만을 핵심 비즈니스 로직으로 전달합니다.

### 2.2. Bring Your Own Settlement (BYOS) - 원장의 불가지성

빌링 세션이 상계(Netted)되어 캐노니컬 해시(`canonical_hash`, AuditReceipt)로 봉인되면, 엔진의 책임은 종료됩니다. 플러그인 형태의 이그레스 어댑터(Egress Adapters)는 이 수학적 증명을 사내 RDBMS(PostgreSQL), 컴플라이언스 볼트, 또는 Web3 스마트 컨트랙트(EVM) 등 어떤 대상(Target)으로든 라우팅할 수 있으며, 이를 통해 초고빈도 빌링 연산과 스토리지 지연 시간(Storage Latency)을 완벽히 분리합니다.

### 2.3. 가스비 없는 인메모리 상계 (Zero-Gas In-Memory Netting)

토큰당 과금되는 $0.0001 수준의 LLM API 빌링을 포함한 모든 마이크로 트랜잭션은 오프체인 UTXO(Unspent Transaction Output) 모델을 사용하여 **오직 인메모리(In-memory) 상에서만 연산**됩니다. 이를 통해 기존 데이터베이스의 Row-locking 병목 현상과 결제 대행사의 최소 수수료 문제를 근본적으로 제거하며, 중간 수수료(Gas fees) 없이 노드당 100,000+ TPS의 코어 청산 처리량을 달성합니다.

---

## 3. 핵심 엔진 컴포넌트 (Core Engine Components)

### 3.1. 표준 호환 LLM 게이트웨이 (`edge.llm`)

세상에서 가장 널리 쓰이는 산업 표준 API(OpenAI 규격 등)를 완벽히 지원하는 보안 라우터입니다. 클라이언트의 비결정론적 LLM 요청을 WASM 커널의 결정론적 연산 의도(Compute Intent)와 결속(Binding)시킵니다. 인메모리 상계 엔진과 직접 연동되어 예산 초과 감지 시 스트림 커넥션을 즉각적으로 안전하게 차단(Physical Kill-switch)합니다.

### 3.2. UTXO 빌링 어댑터 (`kernel.dphi.adapter.utxo`)

중앙 집중식 락(Lock) 없이 동시성 상태 전환을 관리합니다.

* **상태 포인터 (State Pointers):** 수천 개의 병렬 API 소비자에 걸쳐 미사용 잔액(연료/크레딧)을 추적합니다.
* **마이크로 빌링 검증:** 요청을 수학적으로만 평가하여 Ed25519 서명 및 UTXO 포인터를 `< 150 µs` 내에 검증합니다.
* **신용 한도 초과 및 엔터프라이즈 크레딧 (Overdraft):** 기업 화이트리스트 정책에 따라 마이너스(초과) UTXO를 허용하는 지연 결제(Deferred billing)를 지원합니다. 초과분은 클라이언트의 암호학적 서명으로 결속되며, 추후 Egress 어댑터를 통해 월별 청구서나 스마트 컨트랙트 청산으로 연계됩니다.

### 3.3. 동적 계량 엔진 (Unified Economy Engine)

임의의 컴퓨팅 사용량을 실시간으로 UTXO 비용 차감으로 변환합니다.

* **단일 연료 경제 (Energy-Intelligence Unification):** LLM 추론 토큰(Cognitive Token)과 샌드박스 연산 사이클(Compute Cycle)을 단일한 `Fuel(연료)` 단위로 통합하여 다룹니다.
* **표준 헤더 파싱:** 외부 백엔드가 응답 헤더에 표준 규격(예: `X-Dphi-Consume-Fuel: 500`)을 담아 반환하면 이를 즉각 파싱하여 과금하며, 다차원 가격 정책 알고리즘을 동적으로 적용할 수 있습니다.

### 3.4. 머클 롤업 압축기 (Merkle Rollup Compressor)

수만 개의 개별 마이크로 빌링 해시를 단일 이진 트리(Binary Tree)로 집계하여 유일한 표준 루트 해시(Canonical Root Hash)를 계산합니다. 이는 결제 분쟁 해결 및 규제 감사 요구를 충족하기 위해, 불변성을 보장하며 `O(log N)`으로 검증 가능한 사용 증명서(`AuditReceipt`)를 생성합니다.

---

## 4. 통합 청산 생명주기 (The Unified Settlement Lifecycle)

기존 엔터프라이즈 인프라 전면에 독립형 계량 게이트웨이로 배포될 경우, 생명주기는 다음과 같이 작동합니다.

0. **크레딧 초기화 (UTXO Bootstrap):**
생명주기 시작 전, 클라이언트가 법정화폐나 크립토를 통해 선결제하면, DPHI는 해당 크레딧을 인메모리 상의 초기 UTXO 상태 포인터로 주입하여 고빈도 결제 준비를 마칩니다.
1. **인그레스 및 프로토콜 협상 (HTTP 402 Negotiation):**
클라이언트가 유효한 영수증(`X-X402-Receipt`) 없이 API를 요청할 경우, 단순한 권한 거부(403) 대신 웹 표준인 `HTTP 402 Payment Required`를 반환하여 SDK가 백그라운드에서 자연스럽게 암호학적 결제 핸드셰이크를 수행하도록 유도합니다.
2. **실행 라우팅 (Execution Routing):**
* *LLM Gateway Mode:* 표준 프롬프트 요청을 커널의 인가와 결속시켜 LLM 파이프라인으로 전송합니다.
* *Proxy Mode:* 암호학적 봉투(Envelope)를 벗겨내고 원본 요청을 기업의 기존 백엔드(예: 자체 RAG 서버)로 투명하게 전달합니다.


3. **사후 계량 및 동적 차감 (Post-Metering & Dynamic Deduction):**
백엔드 또는 LLM 파이프라인이 사용량 데이터를 반환하면, 동적 계량 엔진이 이를 파싱하여 정확한 마이크로 비용을 계산하고 인메모리 상에서 실시간 UTXO 차감을 실행합니다.
4. **상태 붕괴 및 위상학적 씰링 (State Collapse & Topological Sealing):**
세션 종료 시 UTXO 어댑터는 클라이언트의 남은 잔액과 부채를 병합하고, 다중 서명으로 봉인된 감사 영수증(`AuditReceipt`)을 발행합니다. LLM 요청의 경우, 이 감사 해시를 표준 응답 JSON의 `system_fingerprint` 등에 투명하게 은닉(Topological Sealing)하여 응답의 무결성을 증명합니다.
5. **불가지론적 이그레스 (Agnostic Egress):**
실제 API 응답을 클라이언트에게 반환함과 동시에, 구성된 Egress Adapter(RDBMS, Web3 등)로 롤업 데이터를 비동기 전달하여 백엔드 저장을 완료합니다.

---

## 5. 불가지론적 이그레스 및 어댑터 매트릭스 (Agnostic Egress & Adapter Matrix)

| 어댑터 타입 | 타겟 인프라 | 핵심 유즈케이스 (Primary Use Case) |
| --- | --- | --- |
| **`adapter.egress.rdbms`** | PostgreSQL, Oracle | **Web2 SaaS 빌링:** 롤업 해시를 기존 관계형 DB에 기록하여, 전통적인 월별 청구서 발송 및 신용 초과분(Overdraft) 사후 정산에 활용. |
| **`adapter.egress.vault`** | Enterprise Private Vaults | **규제 컴플라이언스:** 증명 데이터를 프라이빗 볼트에 보관하며, 법적 감사(Audit) 시 AI 시스템의 추적성과 부인 방지를 증명. |
| **`adapter.egress.da`** | Celestia, EigenDA | **Web3 / DePIN:** 합의 오버헤드 없이 탈중앙화 네트워크를 위한 퍼블릭 데이터 가용성(DA) 확보. |
| **`adapter.egress.evm`** | Ethereum, Base, Arbitrum | **온체인 청산 (On-Chain Clearing):** 상계된 순 부채 영수증을 표준 EVM `calldata` 포맷으로 변환하여 무신뢰 기반의 스마트 컨트랙트 강제 청산 실행. |

---

## 6. 목표 성능 지표 (Target Performance Metrics)

* **프록시 및 LLM 패스스루 지연 (Pass-through Latency):** 표준 API 라우팅 과정에서 추가 오버헤드 `< 500 µs`.
* **물리적 차단 지연 (Kinetic Trap Latency):** 예산(Fuel) 초과 또는 이상 감지 시 스트림 파괴 및 커널 종료 `< 100 µs`.
* **암호학적 상계 지연 (Netting Latency):** 상태 얽힘(Entanglement) 및 UTXO 업데이트당 `< 1 ms`.
* **코어 처리량 (Throughput):** 단일 노드 기준 초당 `100,000+` 상태 전환 (TPS). 전통적인 데이터베이스 Row-locking 지연으로부터 완전히 면역.