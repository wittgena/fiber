# fiber.phase.abc.llm.compat.token
@lineage: fiber.phase.doc.llm.compat.token
@lineage: phase.doc.llm.compat.token

## 1. Overview

`fiber.llm.model` 패키지는 LLM 에이전트의 컨텍스트 윈도우 관리에 필수적인 **모델 스펙 조회, 토큰 계산(Token Counting), 텍스트 슬라이싱(Splitting) 및 메시지 트림(Message Trimming)** 기능을 제공합니다.

이 모듈들은 LiteLLM의 핵심 유틸리티(`get_supported_openai_params`, `token_counter`, `trim_messages`, `get_modified_max_tokens`)와 **함수 시그니처 및 동작 방식이 완벽하게 동일**하게 설계되어, 기존 코드를 수정하지 않고 그대로 대체(Drop-in Replacement)할 수 있습니다.

동시에 DPHI 아키텍처에 맞게 Pydantic 무결성 방어와 안전한 토큰 디코딩 로직이 내장되어 있습니다.

---

## 2. Model Info & Capabilities (`llm.model.info`)

LLM 모델별 지원 기능(Vision, Function Calling, Prompt Caching 등)과 지원되는 OpenAI 파라미터 스펙을 조회하는 헬퍼 함수들을 제공합니다.

### 호환되는 주요 함수 (LiteLLM Compatible)

* `get_supported_openai_params(model, custom_llm_provider=None)`
* `get_supported_regions(model)`: 모델의 특정 리전(Region) 지원 여부 조회.
* `supports_function_calling(model)` / `supports_parallel_function_calling(model)`
* `supports_vision(model)` / `supports_audio_input(model)`
* `supports_system_messages(model)` / `supports_prompt_caching(model)`
* `supports_reasoning(model)`: o1, o3, deepseek-r1 등 추론(Reasoning) 모델 여부.
* `supports_native_structured_output(model)`: Provider가 네이티브 JSON 스키마 제약을 지원하는지 여부.

### DPHI 확장 스펙 (`ModelPromptSpec` & `ModelFeatures`)

모델의 가계도(Family)나 추론 능력(Reasoning)을 파악하기 위한 확장 기능을 제공합니다.

* **`get_model_prompt_spec(model_name)`**: 모델 이름에서 계열(`openai_gpt`, `anthropic_claude`, `meta_llama`)을 정규화하여 추출합니다. 프롬프트 포맷 템플릿을 분기할 때 유용합니다.
* **`get_features(model)`**: 해당 모델이 추론 모델(No stop words)인지, `claude-3-5` 같은 프롬프트 캐싱을 지원하는지 등 구체적인 플래그 객체를 반환합니다.

---

## 3. Token Counter (`llm.model.token.counter`)

텍스트, 메시지, 함수 스키마, 심지어 이미지 메타데이터까지 포함하여 정확한 토큰 소모량을 산출합니다.

### 호환되는 주요 함수

```python
from fiber.llm.model.token.counter import token_counter, get_modified_max_tokens

# 1. 일반 대화 메시지 토큰 계산 (함수 정의 포함)
count = token_counter(
    model="gpt-4o", 
    messages=[{"role": "user", "content": "How are you?"}],
    tools=[{"type": "function", "function": {"name": "get_weather", "description": "..."}}]
)

# 2. 동적 Max Tokens 조절 (OOM 방어)
# 입력 토큰량과 버퍼(10%)를 계산하여, 모델 한계를 넘지 않는 안전한 max_tokens를 반환합니다.
safe_max_tokens = get_modified_max_tokens(
    model="gpt-4o",
    base_model="gpt-4o",
    messages=my_messages,
    user_max_tokens=4000,
    buffer_perc=0.1,  # 생략 가능 (기본값: 0.1)
    buffer_num=10.0   # 생략 가능 (기본값: 10.0)
)

```

> **Note:** `buffer_perc`와 `buffer_num` 파라미터는 선택 사항(Optional)이며, 명시적으로 값을 넘기지 않고 생략할 경우 프레임워크가 스마트 기본값(10% 버퍼)을 자동으로 안전하게 적용합니다.

### 내부 개선점

* **Universal Encoder 통합 [NEW]**: 모델 이름(e.g., `llama-3`)에 따라 `tiktoken`과 `HuggingFace Tokenizer`를 자동으로 스위칭하는 유니버설 인코딩 아키텍처가 적용되어 있어, 오픈소스 모델의 토큰 산출 시에도 100%의 정확도를 보장합니다.
* **`TokenEvaluator` 분리**: 글로벌 상태 의존성 없이 인코더 주입(Dependency Injection) 방식으로 토큰을 평가합니다.
* **비용 없는 Vision 연산**: `type: image_url` 객체 발견 시, 이미지를 네트워크에서 다운로드하지 않고 메타데이터(가로/세로 픽셀)만으로 OpenAI의 Vision 타일 계산 공식(`calculate_tiles_needed`)을 적용하여 O(1) 속도로 토큰을 산출합니다.
* **이기종 Provider 포맷 대응 [NEW]**: Anthropic 특화 스키마(`tool_use`, `tool_result`)나 RAG 검색 결과(`search_results`) 같은 비표준 JSON 포맷도 내부적으로 안전하게 파싱하여 토큰을 산출합니다.

---

## 4. Token Window & Trimming (`llm.model.token.window`)

주어진 토큰 한계(Max Limit)에 맞게 대화 내역(`messages`)을 안전하게 잘라냅니다(Trim). LiteLLM의 `trim_messages`와 완벽히 호환됩니다.

### 호환되는 주요 함수

```python
from fiber.llm.model.token.window import trim_messages

trimmed_msgs = trim_messages(
    messages=long_history_messages,
    model="gpt-4o",
    trim_ratio=0.8, # 모델 최대 컨텍스트의 80%를 한계로 지정
)

```

### 안전 트림 규칙 (Safe Eviction Policy)

1. **System Prompt 보존**: 시스템 지시어는 가장 마지막에 잘려나가며, 한계를 넘을 경우 문자열을 토큰 단위로 안전하게 슬라이싱(`ContextWindow.truncate_to_limit`)합니다.
2. **Tool Message 보존**: 마지막 `role: tool` 반환 내역은 LLM의 문맥 유지를 위해 최우선으로 보존됩니다.
3. **Rolling Eviction**: `user`와 `assistant`의 과거 대화는 가장 오래된 것부터 순차적으로 탈락합니다.

---

## 5. Token Splitter (`llm.model.token.splitter`)

LlamaIndex나 LangChain의 `SentenceSplitter`를 대체하기 위한 순수 **Token-Safe Text Splitter**입니다.

### 주요 기능

* **Token ID 기반 슬라이싱**: 문자를 단순히 길이로 자르지 않고, 해당 모델의 Tokenizer(HuggingFace 또는 tiktoken)를 통해 ID 배열로 바꾼 뒤 분할합니다. 이를 통해 한국어나 일본어 같은 **멀티바이트 문자가 청크 경계에서 깨지는(Corruption) 현상을 완벽하게 방지**합니다.
* **HuggingFace 특수 토큰 안전 제거**: 오픈소스 모델의 경우 토크나이저 파싱 시 방해되는 제어 토큰(Special Tokens)을 안전하게 분리(`_strip_huggingface_special_token_ids`)하여 순수 텍스트 토큰만 추출합니다.
* **Overlapping Chunks**: 지정된 `chunk_overlap`만큼 겹치게 분할하여 RAG(검색 증강 생성) 시스템 구축 시 문맥 유실을 방지합니다.

```python
from fiber.llm.model.token.splitter import TokenSplitter

splitter = TokenSplitter(
    chunk_size=500,
    chunk_overlap=50,
    model="gpt-3.5-turbo"
)
chunks = splitter.split_text("Very long document text...")
```