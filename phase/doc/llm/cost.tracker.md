# phase.doc.llm.cost.tracker

## 1. Overview (개요)

`fiber.llm.model.usage` 및 `fiber.llm.model.cost.unit` 모듈은 LLM 호출 시 발생하는 토큰 사용량(Usage)을 추적하고 비용(Cost)을 정산하는 시스템입니다.

**⚠️ 아키텍처 주의 사항 (Architectural Caveat)**
이 모듈은 LiteLLM의 표준 `completion_cost()` 구조와 완벽히 호환되지 않습니다.

1. **Usage 추적 방식의 차이:** LiteLLM은 데코레이터나 함수 리턴 객체에서 토큰을 추출하지만, DPHI 시스템은 `get_emitter` 이벤트 로깅 시스템에 **Interceptor**를 부착하여 비동기적으로 토큰을 수집합니다.
2. **Cost 계산의 불완전성:** 현재의 `UnitCostCalculator` 로직은 복잡한 프롬프트 캐싱(Prompt Caching), 멀티모달(오디오/이미지), 추론(Reasoning) 토큰 비용에 대한 동적 키(Thresholds) 매핑을 시도하고 있으나, **모든 프로바이더에 대해 완벽히 일관되게 동작하지 않습니다.**

---

## 2. Usage Tracking System (사용량 추적 체계)

토큰 사용량을 수집하기 위해서는 반환된 `ModelResponse` 객체를 뜯어보는 대신, **컨텍스트 매니저(`track_usage`)와 이벤트 인터셉터**를 활용해야 합니다.

### 🔌 2.1. 컨텍스트를 통한 투명한(Transparent) 수집

`track_usage()`를 사용하면 해당 블록 안에서 발생하는 모든 LLM 호출의 토큰 사용량이 인터셉터를 통해 백그라운드에서 자동 누적됩니다.

```python
from fiber.llm import completion
from fiber.llm.model.usage import track_usage

# 1. track_usage 블록 열기
with track_usage() as tracker:
    # 2. 내부에서 여러 번의 LLM 파이프라인 호출
    res1 = completion(model="gpt-4o", messages=[{"role": "user", "content": "Task 1"}])
    res2 = completion(model="gpt-4o-mini", messages=[{"role": "user", "content": "Task 2"}])
    
    # 3. 누적된 총 사용량 확인 (모델별로 그룹핑됨)
    total_usage = tracker.get_total_tokens()
    print(total_usage)
    # Output 예시: 
    # {
    #   "gpt-4o": {"prompt_tokens": 15, "completion_tokens": 20, "total_tokens": 35},
    #   "gpt-4o-mini": {"prompt_tokens": 10, "completion_tokens": 15, "total_tokens": 25}
    # }

```

### ⚙️ 2.2. Interceptor의 동작 원리

이 시스템은 DPHI의 핵심 관측 모듈(`xphi.watcher.plane.emitter`)에 강하게 결합되어 있습니다.

* `llm.entry` 파이프라인에서 `ChannelObserver`가 로깅을 발사할 때, `usage_metrics`와 `model_name` 데이터를 이벤트 객체(LogEvent)에 담습니다.
* `_usage_tracking_interceptor`가 이 이벤트를 낚아채서 `flow_scope` 내부에 존재하는 `UsageTracker` 객체에 데이터를 밀어 넣습니다.
* **제약사항:** `flow_scope` 외곽에서 실행되거나 인터셉터 등록이 누락되면 사용량 추적이 조용히 실패(Silently fail)합니다.

---

## 3. Cost Calculation & Billing (비용 정산 체계)

과금을 담당하는 `TenantEco` 클래스와 `UnitCostCalculator`는 `model_cost_registry.json`의 단가표를 기준으로 토큰당 비용을 산출합니다.

### 🧾 3.1. Tenant Billing (테넌트 과금 산출)

에이전트 시스템에서 특정 유저(Tenant)에게 요금을 부과할 때 사용합니다.

```python
from fiber.llm.model.usage import get_tenant_eco

eco_service = await get_tenant_eco()

# LLM 반환 객체의 usage를 그대로 전달
billing_result = await eco_service.calculate_tenant_billing(
    tenant_id="user_abc_123",
    usage=res.usage,
    model_name="gpt-4o",
    provider="openai"
)

if billing_result["status"] == "success":
    print(f"Final Billed Cost: ${billing_result['billing_intent']['financials']['final_cost']}")

```

### ⚠️ 3.2. Cost Calculator 제약 및 불완전성 안내

현재 `UnitCostCalculator.generic_cost_per_token` 메서드는 다음과 같은 한계를 가지고 있습니다.

1. **Prompt Caching 비용 누락 위험:**
* Anthropic `claude-3-5` 계열이나 OpenAI의 캐시된 토큰(`cache_read_input_token_cost`)을 산출하려 시도하지만, 프로바이더마다 Usage 반환 포맷(`prompt_tokens_details`)이 달라 정확한 캐시 할인율이 적용되지 않을 수 있습니다.


2. **티어(Service Tier)별 임계값 계산 오류:**
* Vertex AI 계열의 단계별 과금(`input_cost_per_token_above_128k_tokens`) 로직이 내장되어 있으나 문자열 파싱(`_get_applicable_threshold_string`)에 의존하여 새로운 단가표 스키마 추가 시 깨질 위험이 존재합니다.


3. **Reasoning / Audio / Image 토큰:**
* `o1`이나 `deepseek-r1`의 추론 토큰, 멀티모달 토큰은 `completion_tokens_details` 파싱을 시도하나, 지원되지 않는 프로바이더의 경우 0달러로 누락 처리되거나 기본 텍스트 토큰 요금으로 잘못 합산될 수 있습니다.

**🛠️ 해결 / 회피 방안 (Workaround)**
비즈니스 로직(실제 과금 결제 등)에서 이 Cost 모듈을 사용할 때는 **보수적인 접근**이 필요합니다.

* 정확한 빌링이 필요한 경우, `total_usage` 값을 직접 취합한 뒤 시스템 외부의 신뢰할 수 있는 단독 빌링 서버에서 정산하는 것을 권장합니다.
* 내부 과금용으로는 참고용 메트릭(Observability Metric)이나 DPHI Kernel(Fuel) 제어 용도로만 활용하십시오.