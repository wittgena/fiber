# fiber.phase.abc.dphi.sandbox.milestone
@desc: DPHI Sandbox Architecture & Multi-Tier Execution

## 1. System Architecture Principles

본 문서는 결정론적 상태 전이(Deterministic State Transition)와 제로 트러스트(Zero-Trust) 연산을 수행하는 샌드박스 아키텍처 원칙을 정의한다.

* **Edge Translation:** 외부 네트워크(EVM, CEX) 통신은 Tier 1으로 격리된다. 외부 증명 데이터(TxHash 등)는 검증 완료 직후 메모리에서 파기(Drop)되며, 코어 런타임에는 정규화된 내부 규격(Fuel, Unlocked Macaroon)만 주입된다.
* **Ephemeral Runtime:** 상시 대기하는 데몬이나 컨테이너(Docker) 방식을 사용하지 않는다. 모든 샌드박스 인스턴스는 요청 단위로 생성되며, 연산 완료 및 상태 루트(State Root) 산출 직후 메모리에서 해제(Destroy)된다.
* **Lock-Free UTXO:** 데이터베이스 락(Lock) 기반의 Account 모델 대신 개별 상태가 독립된 UTXO 트리를 채택하여, 동시성 제어 병목을 제거하고 CPU 코어 수에 비례하는 선형적 확장을 지원한다.

---

## 2. Sandbox Execution Tiers (샌드박스 실행 규격)

연산의 결정론(Determinism) 통제 수준과 I/O 권한을 기준으로 런타임을 3계층으로 분리하여 설계한다.

### Tier 1: General I/O Isolate

외부 네트워크와의 프로토콜 변환(Adapter) 및 비결정론적 통신을 담당하는 게이트웨이 계층.

* **Runtime:** V8 Isolate (Deno / Cloudflare Workers)
* **Determinism:** None (비결정론적)
* **Network I/O:** Allow (외부 API, RPC 호출 허용)
* **State Mutation:** Deny (시스템 상태 및 UTXO 직접 변경 불가)
* **Use Case:** 결제망 402 인터셉트, 외부 증명(Proof) 오라클 검증, 페이로드 전처리.

### Tier 2: Constrained Pyodide

비즈니스 로직(Python)을 수용하되, 통제된 런타임 환경을 통해 결정론적 연산 결과를 강제하는 계층.

* **Runtime:** Pyodide (WASM 기반 제약형 Python 인터프리터)
* **Determinism:** Environment-level (환경 통제 기반 결정론)
* **Network I/O:** Deny (모든 소켓 및 외부 네트워크 통신 차단)
* **State Mutation:** Allow (연산 결과에 따른 Fuel 차감 승인)
* **Use Case:** AI 에이전트 인퍼런스, 데이터 변환, 내부 상태 기반 오케스트레이션.
* **Constraints:** `time`, `random` 등의 비결정적 시스템 호출은 커널이 주입한 정적(Mock) 값으로 강제 치환된다.

### Tier 3: Pure Direct WASM

시스템 코어 모듈 및 네이티브 트랜잭션 처리를 위한 완전 결정론적(Fully Deterministic) 실행 계층.

* **Runtime:** Direct Native WASM
* **Determinism:** Instruction-level (명령어 수준의 완전한 결정론)
* **Network I/O:** Deny (원천 차단)
* **State Mutation:** Allow (최종 상태 씰링 및 영수증 발행 권한 부여)
* **Use Case:** UTXO 상태 트리 갱신, `AuditReceipt` 암호학적 서명, 코어 원장(Ledger) 모듈 실행.
* **Constraints:** 인스트럭션(CPU Instruction) 단위의 정밀한 Fuel 계량(Metering)이 적용된다.

---

## 3. Transaction & State Pipeline (트랜잭션 파이프라인)

외부 요청이 인입되어 내부 상태로 치환, 연산, 해체되는 표준 흐름은 다음과 같다.

1. **Ingress & Validation (Tier 1)**

* **Input:** `X-X402-Receipt: macaroon="<LOCKED>", proof="<EXT_HASH>"`
* **Process:** Tier 1 어댑터가 외부 RPC를 호출하여 `proof` 데이터의 유효성을 검증한다.

2. **Translation & Minting (Cross-Tier)**

* **Process:** 검증 완료 즉시 `proof` 파라미터는 메모리에서 파기된다.
* **Output:** 유입된 가치에 비례하는 내부 `Fuel`을 맵핑하고 `Unlocked Macaroon`을 생성하여 코어로 전달한다.

3. **Execution (Tier 2 / Tier 3)**

* **Input:** `Unlocked Macaroon`, `Target Code (Python or WASM)`
* **Process:** 할당된 런타임이 외부 I/O가 차단된 격리 환경에서 코드를 실행하며, 사용량에 따라 실시간으로 `Fuel`을 차감(Metering)한다.

4. **Commit & Destruction (Tier 3)**

* **Process:** 연산 종료 시 UTXO Delta를 계산하여 상태 루트(State Root)를 도출한다. 최종 `AuditReceipt`를 발급한 뒤 샌드박스 인스턴스는 즉각 메모리에서 해체(Destroy)된다.

---

## 4. System Scalability & Operations (시스템 확장성 규격)

* **Zero-Idle Overhead:** 데몬 프로세스를 배제하고 요청 단위의 짧은 샌드박스 생명주기(ms 단위)를 적용하여, 유휴 자원(Idle Resource) 점유 및 가비지 컬렉션(GC)으로 인한 병목을 방지한다.
* **Adapter Pluggability:** 결제망 연동 로직을 Tier 1에 격리함으로써, 코어(Tier 3) 로직의 수정이나 배포 중단 없이 새로운 외부 네트워크(EVM, CEX) 어댑터를 동적으로 추가 및 제거할 수 있다.
* **Lock-Free Throughput:** UTXO 구조를 통한 상태의 독립성을 확보하여 데이터베이스 수준의 트랜잭션 Lock을 제거하고, 트래픽 유입량에 비례하는 동시성(Concurrency) 확장을 지원한다.