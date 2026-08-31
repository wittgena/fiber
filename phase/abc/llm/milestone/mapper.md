# fiber.phase.abc.llm.mapper.milestone
다음은 불필요한 수식어와 과장된 서사를 배제하고, 원문의 모든 기술적 내용을 누락 없이 아키텍처 스펙(Technical Specification) 관점에서 객관적이고 엄밀하게 재작성한 문서입니다.

# Specification: fiber.phase.abc.llm.mapper.milestone

## 1. 개요 (Overview)

본 문서는 다수의 LLM 프로바이더 및 엔터프라이즈 레거시 시스템 간의 데이터 규격 파편화(Data Fragmentation) 문제를 해결하기 위한 `fiber.dphi.model.mapper` 모듈의 아키텍처와 향후 마일스톤을 정의한다.

기존 프록시 시스템이 의존하던 하드코딩 방식의 파서(Parser) 및 조건문(`if-else`) 기반 데이터 처리는 런타임 에러와 전역 상태 오염을 유발하는 구조적 한계를 지닌다. 이를 해결하기 위해 DPHI 매퍼 모듈은 **경로 기반 선언적 탐색(Declarative Path Resolution)** 및 **이중 방어 복원(Dual-Guard Recovery)** 아키텍처를 채택하였다. 이를 통해 비표준 응답 데이터를 내부 표준 규격인 OpenAI `ModelResponse` 형식으로 정규화(Normalization)한다.

또한, 본 선언적 아키텍처를 기반으로 시스템이 스스로 레거시 규격을 학습하고 연동 룰을 생성하는 '자율 통합 루프(Autonomous Integration Loop)'로의 전환 마일스톤을 정의한다.

---

## 2. 상태 변환 엔진 아키텍처 (State Translation Engine Architecture)

현재의 상태 변환 엔진은 스키마 변경 및 비표준 API 응답에 대응하여 파이프라인의 중단을 방지하도록 설계되었다.

### 2.1. `StateTraverser` (선언적 횡단기)

이기종 시스템의 응답 데이터는 딕셔너리(`dict`), 리스트(`list`), 파이썬 객체(`object`)가 혼합된 하이브리드 토폴로지를 갖는다. `StateTraverser`는 프로그래밍 제어문 대신 단일 문자열 경로(Path)를 통해 데이터를 탐색 및 추출한다.
탐색 과정에서 존재하지 않는 인덱스나 속성에 접근할 경우, `IndexError`나 `AttributeError` 예외를 발생시키는 대신 사전에 정의된 기본값(Default)을 반환하여 파이프라인의 연속성을 보장한다.

```python
# [구현 예시] 비표준 Tool Call 응답 구조 탐색
from fiber.dphi.model.mapper.traverser import StateTraverser

# 딕셔너리, 리스트, 객체가 혼합된 구조에서 경로(Path) 문자열을 통한 안전한 데이터 추출
f_name = StateTraverser.resolve(raw_resp, "content.parts.0.function_call.name", default=None)

```

### 2.2. `STATE_EXTRACTION_RULES` (정적 룰셋 레지스트리)

데이터 매핑 및 추출 로직을 코드 레벨에서 분리하여 선언적 JSON/Dict 명세서 형태로 추상화한 레지스트리이다.
새로운 API 규격을 연동할 때 애플리케이션 코드를 수정할 필요 없이, 해당 레지스트리에 새로운 경로 규칙(Rule)을 추가하는 것만으로 파이프라인 호환성을 확보할 수 있다.

### 2.3. `StateMapper` & `ImperativeFallbackRule` (이중 방어 파이프라인)

API 스키마 변경 등으로 인해 선언적 룰 기반 데이터 추출이 실패할 경우를 대비하여, 다음과 같은 3단계 복구 파이프라인을 운영한다.

1. **Phase 1 (Declarative):** `STATE_EXTRACTION_RULES`와 `StateTraverser`를 사용하여 데이터를 추출한다.
2. **Phase 2 (Imperative):** Phase 1 실패 시, `ImperativeFallbackRule`을 통해 런타임 타입 검사(Heuristic Analysis)를 수행하여 누락된 툴 호출(Tool Call)이나 텍스트 청크 데이터를 복원한다.
3. **Phase 3 (Assembly):** 추출 및 복원된 데이터를 취합하여 내부 파이프라인 미들웨어가 처리할 수 있는 표준 스펙(`ModelResponse` Choice 객체)으로 조립(Assembly)한다.

---

## 3. 보안 및 안정성 명세 (Security & Stability Specification)

본 아키텍처는 엔터프라이즈 환경에서 요구하는 시스템 안정성 및 보안 기준을 다음의 메커니즘을 통해 충족한다.

* **Crash-Free Guarantee (크래시 방지):** 데이터 탐색 시 `get` 및 `getattr`의 방어적 체이닝을 적용한다. 타겟 API가 비정상적인 형태의 JSON을 반환하더라도 파이프라인 프로세스가 500 Internal Server Error 등으로 중단되지 않도록 보장한다.
* **Zero Injection Attack Surface (인젝션 공격 표면 제거):** 데이터 파싱 과정에서 `eval()`, `exec()` 함수나 불안전한 객체 역직렬화를 일절 사용하지 않는다. 순수 문자열 기반의 경로 탐색만을 수행하여 외부 페이로드 파싱을 통한 원격 코드 실행(RCE) 등의 인젝션 공격 취약점을 원천 차단한다.

---

## 4. 전략적 마일스톤: 자율 통합 루프 (Autonomous Integration Loop)

현재의 선언적 룰(JSON/Path) 기반 추출 구조는 궁극적으로 AI 에이전트가 외부 API의 연동 작업을 자체적으로 수행하는 '자율 통합(Autonomous Integration)' 아키텍처를 구현하기 위한 기반 스펙이다.
복잡한 파이썬 파싱 코드를 직접 생성하는 대신, API 응답 데이터를 분석하여 JSON 데이터 경로(Path String)를 도출하는 방식을 통해 에이전트의 코드 작성 오류를 방지한다.

### 마일스톤 워크플로우 (Milestone Workflow)

새로운 사내망 DB나 비표준 외부 API 연동 시, 시스템 내에서 다음과 같은 자율 연동 루프가 실행된다.

1. **Discovery & Inference (탐색 및 추론):** 시스템 내 LLM 에이전트가 연동 대상 API의 응답 페이로드(Payload) 샘플 및 API 문서를 분석한다.
2. **Rule Synthesis (선언적 룰 생성):** 파이썬 파싱 코드를 직접 작성하지 않고, `STATE_EXTRACTION_RULES`에 주입할 선언적 경로 문자열(예: `"payload.data.items.2.func"`)만을 도출 및 생성한다.
3. **E2E Pipeline Simulation (시뮬레이션):** 생성된 룰을 `StateMapper`의 메모리에 주입한 뒤, 격리된 워크플로우 환경(`workflow.llm.compat`)에서 실제 End-to-End 파이프라인 테스트를 가동한다.
4. **Log Analysis & Self-Healing (로그 분석 및 자율 복구):** 파싱 실패(필드 누락 등) 관련 로그가 발생할 경우, 에이전트가 해당 로그를 피드백으로 수신하여 데이터 추출 경로(Path)를 자체 수정한 후 시뮬레이션 루프를 재시도한다.
5. **Commit & Hot-Reload (운영 배포):** 테스트 케이스를 100% 통과한 룰은 시스템 레지스트리에 영구 등록(Commit)되며, 서버 재기동 과정 없이 런타임 트래픽 라우팅에 즉시 반영된다.