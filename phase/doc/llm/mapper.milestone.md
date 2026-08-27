# phase.doc.llm.mapper.milestone

## 1. Executive Summary

2026년, AI 생태계는 수많은 LLM Provider와 엔터프라이즈 레거시 시스템들이 쏟아내는 '비표준 데이터 규격(Data Fragmentation)'으로 인해 심각한 통합 위기를 맞았습니다. 1세대 프록시 시스템(LiteLLM 등)은 이를 해결하기 위해 수만 줄의 `if-else` 블록과 하드코딩된 파이썬 파서(Parser)를 작성했으나, 이는 끊임없는 런타임 에러(Crash)와 전역 상태 오염(State Contamination)을 유발하는 기술적 부채의 핵심 원인이 되었습니다.

`fiber.dphi.model.mapper` 모듈은 파이썬 코드로 데이터를 파싱하는 구시대적 패러다임을 폐기합니다. 현재 구축된 시스템은 경로 기반의 선언적 탐색(Declarative Path Resolution)과 **이중 방어 복원(Dual-Guard Recovery)** 아키텍처를 통해, 어떠한 기형적인 응답 데이터가 들어와도 완벽한 OpenAI 표준 규격(`ModelResponse`)으로 정규화(Normalization)합니다.

나아가, 본 문서는 이 선언적 아키텍처를 기반으로 인간의 개입 없이 AI 에이전트가 레거시 시스템의 규격을 스스로 학습하고 연동하는 '자율 통합 루프(Autonomous Integration Loop)'로 진화하기 위한 전략적 마일스톤(Milestone)을 제시합니다.

---

## 2. Current State: Declarative State Translation Engine

현재 DPHI의 상태 변환 엔진은 외부의 혼돈(비표준 API, 변경된 스키마 등)에 의해 파이프라인이 파괴되지 않고 유연하게 흡수하는 **안티프래질(Anti-fragile)** 철학에 기반하여 구현되어 있습니다.

### 2.1. `StateTraverser` (선언적 횡단기)

이기종 시스템이 반환하는 데이터는 딕셔너리(`dict`), 리스트(`list`), 파이썬 객체(`object`)가 복잡하게 얽힌 하이브리드 토폴로지(Mixed Topology)를 가집니다. `StateTraverser`는 파이썬의 제어문(`if-else`) 대신 단일 문자열 경로를 통해 안전하게 횡단합니다.
탐색 중 존재하지 않는 인덱스나 속성을 만나도 `IndexError`나 `AttributeError`를 발생시키지 않고 조용히 기본값(Default)을 반환하여 시스템의 생존을 보장합니다.

```python
# [구현 예시] Gemini가 반환하는 비표준 Tool Call 응답 구조 탐색
from fiber.dphi.model.mapper.traverser import StateTraverser

# 딕셔너리와 리스트, 객체가 섞여 있어도 경로(Path) 문자열만으로 안전하게 추출
f_name = StateTraverser.resolve(raw_resp, "content.parts.0.function_call.name", default=None)

```

### 2.2. `STATE_EXTRACTION_RULES` (정적 룰셋 레지스트리)

데이터를 추출하는 로직을 코드에서 완전히 분리하여, 선언적 JSON/Dict 명세서로 추상화했습니다.
현재는 엔지니어에 의해 정의되며, 새로운 API 규격이 추가될 때 파이프라인 코드를 수정할 필요 없이 이 딕셔너리에 규칙만 추가하면 즉시 호환됩니다.

### 2.3. `StateMapper` & `ImperativeFallbackRule` (이중 방어 파이프라인)

선언적 룰이 API의 갑작스러운 스키마 변경으로 인해 실패할 경우를 대비하여 3단계 복원(Recovery) 파이프라인을 가동합니다.

1. **Phase 1 (Declarative):** `STATE_EXTRACTION_RULES`와 `Traverser`를 이용해 $O(1)$ 속도로 데이터 추출.
2. **Phase 2 (Imperative):** 실패 시 `ImperativeFallbackRule`을 통해 런타임 타입 검사(Heuristic Analysis)로 숨겨진 툴 호출이나 텍스트 청크를 강제 복원.
3. **Phase 3 (Assembly):** 추출된 데이터를 조립하여 파이프라인 미들웨어가 읽을 수 있는 표준 스펙(`ModelResponse` Choice 객체)으로 포장.

---

## 3. Security & Stability Value (현재 아키텍처의 가치)

현재 구현된 이 구조만으로도 2026년 엔터프라이즈 환경이 요구하는 극한의 보안 및 안정성 기준을 충족합니다.

* **Crash-Free Guarantee:** `get`과 `getattr`의 방어적 체이닝을 통해, 타겟 API가 망가진 JSON을 내려보내도 DPHI 파이프라인은 절대 500 에러로 중단되지 않습니다.
* **Zero Injection Attack Surface:** 외부 데이터 파싱을 위해 `eval()`, `exec()` 또는 복잡한 객체 역직렬화를 사용하지 않고 순수 문자열 경로 탐색만 수행하므로 파서를 노린 보안 취약점(RCE 등)이 원천 차단됩니다.

---

## 4. Strategic Milestone: The "Autonomous Integration Loop"

현재의 '선언적 룰(JSON/Path) 기반 추출 아키텍처'는 단순히 코드를 깔끔하게 만들기 위한 것이 아닙니다. 이는 인간 엔지니어를 시스템 연동 루프에서 완전히 배제하고, AI가 직접 레거시 API를 연동하는 **자율 통합(Autonomous Integration)** 시대로 넘어가기 위한 핵심 초석(Enabler)입니다.

LLM은 복잡하고 예외가 많은 '파이썬 파싱 코드'를 무결점(Bug-free)으로 작성하는 것에는 취약하지만, API 문서를 읽고 '데이터가 있는 JSON 경로(Path String)를 유추하는 작업'에는 압도적인 성능을 보입니다.

향후 DPHI는 이 아키텍처를 기반으로 다음의 자율 룰셋 합성 워크플로우(Self-Synthesizing Workflow)를 달성하는 것을 주요 마일스톤으로 삼습니다.

### 🚀 Milestone Workflow (예정된 E2E 자율 루프)

새로운 사내망 DB나 비표준 외부 API를 DPHI 프록시와 연동해야 할 때, 다음과 같은 자율 루프가 가동됩니다.

1. **Discovery & Inference (탐색 및 추론):** LLM 에이전트가 타겟 API의 응답 샘플(Payload)과 문서를 분석합니다.
2. **Rule Synthesis (선언적 룰 생성):** 에이전트는 파이썬 코드를 건드리지 않고, 오직 `STATE_EXTRACTION_RULES`에 주입할 `path` 문자열 룰(예: `"payload.data.items.2.func"`)만을 생성합니다.
3. **E2E Pipeline Simulation (시뮬레이션):** DPHI 프레임워크는 생성된 룰을 `StateMapper` 메모리에 주입하고, 격리된 워크플로우 내에서 실제 E2E 파이프라인 테스트(`workflow.llm.compat` 방식)를 가동합니다.
4. **Log Analysis & Self-Healing (로그 피드백 및 자율 복구):** 파싱 에러(예: 런타임 크래시가 아닌, 필드 누락 로그)가 발생하면, 에이전트가 해당 로그를 피드백으로 받아 추출 경로(Path)를 수정하여 루프를 재시도합니다.
5. **Commit & Hot-Reload (운영 배포):** 테스트를 100% 통과하면 해당 룰이 시스템에 영구 등록(Commit)되며, 서버 재기동 없이 즉시 트래픽 라우팅에 적용됩니다.