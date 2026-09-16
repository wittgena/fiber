# fiber.phase.abc.milestone.llm.record

## 1. 개요 (Overview)
본 마일스톤은 `fiber.llm.pipeline`의 슬롯(Slot) 기반 아키텍처를 활용하여, LLM 파이프라인의 요청 및 응답 상태를 기록(Record)하고 이를 기반으로 물리적 네트워크 통신 없이 상태를 재현(Replay)하는 시스템 구축을 목표로 합니다.

## 2. 현재 상태 (Current Status)
현재 `fiber.dev.e2e.llm.trace` 테스트 스위트를 통해 다음의 기반 구조가 검증 완료되었습니다.
* **Facade Entrypoint:** LiteLLM/OpenAI 표준 인터페이스를 유지하면서 내부적으로 Netty 스타일의 채널 파이프라인(`DuplexChannel`) 실행.
* **Pipeline Slots:** `PRE_TRANSLATE`, `POST_TRANSLATE`, `PRE_OBSERVER` 등 런타임 훅(Hook)의 결정론적 순서 제어 확보.
* **Short-circuit 및 비동기 격리:** 캐시/가드레일에 의한 I/O 차단 및 트레이서의 비동기 실행(Fire-and-forget) 검증 완료.

## 3. 해결 과제 (Problem Statement)
현재 LLM 애플리케이션 테스트 및 디버깅은 다음의 한계를 가집니다.
* **비결정론적 응답 (Non-deterministic):** 매번 달라지는 LLM 응답으로 인해 CI/CD 환경에서의 신뢰성 있는 테스트 자동화가 불가.
* **비용 및 지연시간:** 테스트 시마다 물리적 API 호출이 발생하여 토큰 비용과 대기 시간 소요.
* **벤더 종속성 디버깅:** 공급자(Gemini, OpenAI 등)별 페이로드 변환 과정에서 발생하는 문제를 로컬 환경에서 독립적으로 재현하기 어려움.

## 4. 핵심 목표 아키텍처: 3-Point Snapshot
파이프라인의 특정 슬롯에서 발생하는 상태 변이를 3개의 지점으로 나누어 스냅샷으로 캡처합니다.

1. **Entry Snapshot (요청 진입점):**
   * 위치: 파이프라인 최상단 (사용자 입력)
   * 내용: 표준화된 OpenAI 규격의 원본 요청(Messages, Config 등).
2. **Translator Snapshot (변환기):**
   * 위치: `POST_TRANSLATE` 슬롯 통과 직후
   * 내용: 시스템이 특정 벤더(Provider)의 API 규격으로 직렬화/변환한 실제 페이로드.
3. **Exit Snapshot (최종 응답):**
   * 위치: 전송 계층(Transport) 응답 직후
   * 내용: 벤더로부터 수신한 원시 응답(Raw Response) 및 정규화된 내부 객체(`ModelResponse`).

## 5. 기대 효과 (Milestone Deliverables)
* **비용 제로의 결정론적 CI/CD:** 기록된 스냅샷(Replay)을 통해 API 통신 없이 즉각적이고 동일한 응답을 반환하여 완벽한 자동화 테스트 환경 제공.
* **로컬 장애 재현 (Time-Travel Debugging):** 프로덕션에서 발생한 특정 벤더의 타임아웃(503), 포맷 오류 등을 스냅샷 파일(.jsonl)로 추출하여 로컬에서 100% 동일하게 재현.
* **번역 계층(Translator) 독립 검증:** 비즈니스 로직(Entry)과 통신 벤더(Translator) 간의 데이터 변환 무결성을 API 호출 없이 구조적으로 검증.

## 6. 실행 단계 (Action Items)

### Phase 1: Snapshot Schema Design
* 3-Point 데이터를 직렬화하기 위한 표준 스냅샷 데이터 스키마 정의 (JSON/JSONL 포맷).
* 스트리밍(Chunk) 응답과 단건(Batch) 응답을 모두 포괄할 수 있는 구조 설계.

### Phase 2: Recorder Interceptor 구현
* 파이프라인의 `PRE_TRANSLATE` 및 `PRE_OBSERVER` 슬롯에 장착될 상태 기록용 채널(`SnapshotRecorderChannel`) 개발.
* 컨텍스트 메타데이터(`ExecutionMetadata`, `Trace ID`) 기반 파일 I/O 로직 작성.

### Phase 3: Replay Transport 구현
* 실제 물리 네트워크(e.g., `CompletionTransport`)를 대체하여 스냅샷 데이터를 Inbound로 역전파(`fire_channel_read`)하는 모의 전송 계층(`ReplayTransport`) 개발.

### Phase 4: E2E Verification
* `fiber.dev.e2e.llm.trace` 스위트에 `Phase 7: Record & Replay` 시나리오 추가.
* 기록 -> 물리망 차단 -> 재현의 사이클이 기존 파이프라인 훅(Tracer, Guardrail 등)과 완벽히 호환됨을 검증.