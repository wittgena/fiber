# fiber.phase.abc.llm.problem.definition
@desc: Fiber Agent Infrastructure - Core Problem Definition & Alignment Strategy
**문서 목적:** 자율형 AI 에이전트 연동 환경에서 발생하는 구조적 취약점의 본질 규명 및 해결을 위한 아키텍처 정렬 방향 정의

---

## 1. Origin

현재의 연동 환경 문제는 두 가지 기술적 변화가 맞물리면서 발생한다.

1. **에이전트 자율성의 확장:** 애플리케이션 계층의 소프트웨어 가드레일만으로는 AI 모델의 무한 루프나 프롬프트 인젝션으로 인한 통제 불능 상태를 제어할 수 없다.
2. **연동 프로토콜의 무상태(Stateless) 전환:** 확장성을 위해 MCP 2.0(`2026-07-28` 스펙) 등 주요 프로토콜이 세션을 폐기하고 무상태 아키텍처로 전환되었다. 이로 인해 상태 유지와 보안 검증의 책임이 개별 클라이언트에게 전가되었다.

---

## 2. Core Problems

위의 기원으로 인해 프로덕션 환경에서 다음 세 가지 구조적 취약점이 발생한다.

### 2.1. 비용 통제 불능 (Unpredictable Billing Runaways)

* **현상:** 에이전트의 예기치 않은 동작(루프, 악의적 인젝션)이 API 호출 비용 및 컴퓨팅 자원의 무제한 소모로 이어진다.
* **원인:** 기존 소프트웨어 수준의 예산 통제(Guardrails)는 런타임 단계에서 쉽게 우회되거나 무력화된다.

### 2.2. 애플리케이션 계층의 보안 취약성 (Application-Layer Permissiveness)

* **현상:** 복잡한 도구(Tool) 호출을 표준 애플리케이션 래퍼(Wrapper)에 위임함으로써, 호스트 시스템이 커맨드 인젝션 및 공급망 공격에 노출된다.
* **원인:** 실행 환경(Execution)과 호출 환경(Client) 간의 물리적/논리적 격리(Isolation)가 부재하다.

### 2.3. 무상태 전환에 따른 책임 전가 (The Burden of Statelessness)

* **상태 증발:** 세션 파기 기조로 인해 다단계 도구 호출(상태 전이)의 맥락 유지가 불가능해졌다.
* **보안 오버헤드:** 매 요청마다 DPoP, SPIFFE, L402와 같은 무거운 암호학적 인증 및 과금 증명을 반복해야 한다.
* **멱등성 및 동시성 문제:** 네트워크 단절 시 클라이언트의 재시도로 인한 중복 실행, 다중 에이전트 접근 시의 레이스 컨디션 방어 책임을 클라이언트(엔터프라이즈)가 직접 구현해야 한다.

---

## 3. 정렬 및 해결 방향 (Alignment & Resolution Strategy)

Fiber 인프라는 위 문제들을 해결하기 위해 시스템의 복잡성을 중앙에서 흡수하는 '충돌 방지 계층(Anti-Corruption Layer)'으로 Gateway를 배치하여 다음 세 가지 차원으로 아키텍처를 정렬한다.

### 3.1. 무상태 복잡성의 흡수 (Complexity Sink)

* **상태 기계(FSM) 도입:** 무상태 프로토콜 위에 `INITIALIZE - MUTATE - COMMIT - QUERY`로 이어지는 트랜잭션 앵커(Transition Bridge)를 구축한다.
* **인증 및 멱등성 중앙화:** 엣지(Edge) 단계에서 DPoP/SPIFFE 신원 증명을 처리하고, `x_idempotency_key`와 캐시(Redis) 기반의 리플레이 방어(Nonce Lock)를 통해 동시성 충돌과 중복 실행을 차단한다.

### 3.2. 실행 격리와 예산의 물리적 통제 (Deterministic Isolation)

* **WASM 멤풀 큐잉:** 도구 호출을 호스트 OS에서 직접 실행하지 않고, 결정론적 이벤트(`LogicStream`)로 변환하여 격리된 WASM 커널 멤풀에 대기시킨다.
* **물리적 차단 (Kinetic Trap):** 토큰 및 자원 사용량을 하드웨어 수준(Fuel)으로 계측하며, 예산 초과 시 런타임 연결을 물리적으로 종료하여 비용 누수를 원천 차단한다.

### 3.3. 주체 간의 무마찰 디커플링 (Zero-Friction Decoupling)

* **Client (Agent) 측면:** 서버의 상태 유지 여부나 내부 원장 처리 구조를 알 필요 없이, Gateway가 제공하는 단일화된 비동기 인터페이스(HTTP 202 Polling)와 멱등성 키만으로 안전하게 도구를 호출한다.
* **Provider (Server) 측면:** 기존의 세션 기반 레거시 서버 로직을 수정할 필요 없이(Legacy Flush), 복잡한 보안 및 상태 제어를 Gateway에 위임하고 순수 비즈니스 로직(MCP JSON-RPC) 처리에만 집중한다.