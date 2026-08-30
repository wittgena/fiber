# fiber.phase.doc.milestone.sandbox.align
@desc: DPHI Sandbox Architecture & Multi-Tier Execution

## 1. System Architecture Principles

본 시스템은 기계 경제(A2A) 환경에 최적화된 결정론적 상태 전이(Deterministic State Transition)와 **비신뢰(Zero-Trust) 연산**을 강제하기 위한 프랙탈(Fractal) 샌드박스 아키텍처를 정의합니다.

* **Edge Translation (경계 캡슐화):** 외부 네트워크(EVM, CEX) 연동은 시스템 외곽(Tier 1)으로 격리됩니다. 외부 증명 데이터(TxHash 등)는 검증 즉시 파기(Drop)되며, 코어 런타임에는 오직 정규화된 내부 규격(Fuel, Unlocked Macaroon)만 주입됩니다.
* **Ephemeral Runtime (상태 비영구성):** 영구적으로 실행되는 데몬이나 컨테이너(Docker)는 배제됩니다. 모든 시스템 모듈과 연산은 요청 단위로 인스턴스화되며, 연산 완료 및 상태 루트(State Root) 산출 즉시 런타임은 메모리에서 소멸(Destroy)됩니다.
* **Lock-Free UTXO (동시성 보장):** 데이터베이스 락(Lock) 기반의 Account 모델을 배제하고 독립적 상태인 UTXO 트리를 채택하여, 트래픽 병목 없이 CPU 코어 수에 비례하는 선형적 확장을 보장합니다.

---

## 2. Sandbox Execution Tiers (샌드박스 실행 규격)

연산의 결정론(Determinism)과 I/O 권한을 기준으로 런타임을 3계층으로 분리 통제합니다.

### Tier 1: General I/O Isolate

외부 네트워크와의 비결정론적 통신 및 프로토콜 변환(Adapter)을 담당하는 게이트웨이 계층입니다.

* **Runtime:** V8 Isolate (Deno / Cloudflare Workers)
* **Determinism:** None (비결정론적)
* **Network I/O:** Allow (외부 API, RPC 호출 허용)
* **State Mutation:** Deny (시스템 상태/UTXO 직접 변경 불가)
* **Use Case:** 결제망 402 인터셉트, 외부 증명(Proof) 오라클 검증, 페이로드 전처리.

### Tier 2: Constrained Pyodide

개발자의 범용 비즈니스 로직(Python)을 수용하되, 환경을 통제하여 결정론적 결과를 강제하는 계층입니다.

* **Runtime:** Pyodide (WASM 기반 제약형 Python 인터프리터)
* **Determinism:** Environment-level (환경 통제 기반 결정론)
* **Network I/O:** Deny (모든 소켓 및 외부 네트워크 차단)
* **State Mutation:** Allow (연산 결과에 따른 Fuel 차감 승인)
* **Use Case:** AI 에이전트 인퍼런스, 데이터 변환, 내부 상태 기반 오케스트레이션.
* **Constraints:** `time`, `random` 등의 비결정적 시스템 호출은 시스템이 주입한 정적(Mock) 값으로 강제 치환됩니다.

### Tier 3: Pure Direct WASM

시스템 코어 모듈 및 A2A 네이티브 트랜잭션을 위한 최고 보안/최고 속도의 완전 결정론적 계층입니다.

* **Runtime:** Direct Native WASM
* **Determinism:** Instruction-level (명령어 수준의 절대적 결정론)
* **Network I/O:** Deny (원천 차단)
* **State Mutation:** Allow (최종 상태 씰링 및 영수증 발행)
* **Use Case:** UTXO 상태 트리 갱신, `AuditReceipt` 암호학적 서명, 코어 원장(Ledger) 모듈 실행.
* **Constraints:** 인스트럭션(CPU Instruction) 단위의 정밀한 Fuel 계량(Metering)이 적용됩니다.

---

## 3. Transaction & State Pipeline (트랜잭션 파이프라인)

외부 요청이 내부 상태로 치환되어 연산 및 해체되는 표준 흐름입니다.

1. **Ingress & Validation (Tier 1)**
* **Input:** `X-X402-Receipt: macaroon="<LOCKED>", proof="<EXT_HASH>"`
* **Process:** Tier 1 어댑터가 외부 RPC를 호출하여 `proof` 유효성을 검증합니다.

2. **Translation & Minting (Cross-Tier)**
* **Process:** 검증 완료 즉시 `proof` 파라미터는 메모리에서 파기됩니다.
* **Output:** 유입된 가치에 비례하는 내부 `Fuel` 맵핑 및 `Unlocked Macaroon` 생성.

3. **Execution (Tier 2 / Tier 3)**
* **Input:** `Unlocked Macaroon`, `Target Code (Python or WASM)`
* **Process:** 할당된 런타임이 외부 I/O가 차단된 상태에서 코드를 실행하며 실시간으로 `Fuel`을 차감(Metering)합니다.

4. **Commit & Destruction (Tier 3)**
* **Process:** 연산 종료 시 UTXO Delta를 계산하고 상태 루트(State Root)를 도출합니다. `AuditReceipt`를 발급한 뒤 샌드박스 인스턴스는 즉각 해체(Destroy)됩니다.

---

## 4. System Scalability & Operations (시스템 확장성 규격)

* **Zero-Idle Overhead:** 데몬 프로세스 배제 및 찰나의 샌드박스 생명주기(ms 단위)를 통해 유휴 자원(Idle Resource) 점유 및 가비지 컬렉션(GC) 병목을 원천 제거합니다.
* **Adapter Pluggability:** 결제망 연동 로직은 Tier 1에 격리되므로, 코어(Tier 3) 로직의 수정 및 배포 중단 없이 새로운 외부 네트워크(EVM, CEX) 어댑터를 실시간으로 추가/제거할 수 있습니다.
* **Ceiling-less Throughput:** UTXO 구조를 통한 상태의 독립성 보장으로 DB 트랜잭션 Lock을 제거, 트래픽 유입량에 비례하는 무제한 동시성 처리(Concurrency)를 지원합니다.