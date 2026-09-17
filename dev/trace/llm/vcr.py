# fiber.dev.trace.llm.vcr
import os
import json
import asyncio
import uuid
from typing import Optional, Any

from fiber.llm.param import ModelResponse
from fiber.llm.router.registry.adapter import AdapterRegistry
from fiber.llm.router.mapper.traverser import StateTraverser

class VCRManager:
    def __init__(self, mode: str, fixture_path: str):
        self.mode = mode.lower()  # 'live', 'record', 'replay'
        self.fixture_path = fixture_path
        self.fixtures = {}
        
        if self.mode == "replay":
            if os.path.exists(self.fixture_path):
                with open(self.fixture_path, "r", encoding="utf-8") as f:
                    self.fixtures = json.load(f)
            else:
                raise FileNotFoundError(
                    f"VCR Fixture not found at '{self.fixture_path}'.\n"
                    f"Please run your test suite with '--vcr record' option first."
                )

    def get_fixture(self, phase_id: str) -> Optional[dict]:
        return self.fixtures.get(phase_id)

    def save_fixture(self, phase_id: str, content: str, is_error: bool = False, error_msg: str = ""):
        if self.mode == "record":
            self.fixtures[phase_id] = {
                "content": content,
                "is_error": is_error,
                "error_msg": error_msg
            }
            # 디렉터리가 존재하지 않으면 안전하게 자동 생성
            os.makedirs(os.path.dirname(self.fixture_path), exist_ok=True)
            with open(self.fixture_path, "w", encoding="utf-8") as f:
                json.dump(self.fixtures, f, indent=2, ensure_ascii=False)


def apply_vcr_patch(vcr: VCRManager):
    if vcr.mode == "live":
        return

    original_get_adapter = AdapterRegistry.get_adapter

    def patched_get_adapter(task_type, provider_name):
        adapter = original_get_adapter(task_type, provider_name)
        if getattr(adapter, "_is_vcr_patched", False):
            return adapter
            
        original_execute = adapter.execute

        async def vcr_execute(msg: Any):
            original_kwargs = getattr(msg, "original_kwargs", {}) if not isinstance(msg, dict) else msg
            phase_id = original_kwargs.get("metadata", {}).get("vcr_phase")
            model_name = getattr(msg, "model", "vcr-mock-model") if not isinstance(msg, dict) else msg.get("model", "vcr-mock-model")
            
            # [REPLAY MODE] 인터넷 통신 없이 저장된 픽스처(Fixture) 반환
            if vcr.mode == "replay" and phase_id:
                fixture = vcr.get_fixture(phase_id)
                if not fixture:
                    raise ValueError(f"No VCR fixture mapped for phase_id: '{phase_id}'")
                
                # 에러 저장본이면 동일하게 예외 발생
                if fixture["is_error"]:
                    raise Exception(fixture["error_msg"])
                
                # 스트리밍 응답 흉내내기 (AsyncGenerator 반환)
                if original_kwargs.get("stream"):
                    async def dummy_stream():
                        yield ModelResponse(
                            id=f"vcr-{uuid.uuid4().hex[:8]}",
                            model=model_name,
                            choices=[{"index": 0, "delta": {"content": fixture["content"]}}]
                        )
                    return dummy_stream()
                
                # 일반 응답 흉내내기 (ModelResponse 객체 반환)
                else:
                    return ModelResponse(
                        id=f"vcr-{uuid.uuid4().hex[:8]}",
                        model=model_name,
                        choices=[{
                            "index": 0, 
                            "message": {"role": "assistant", "content": fixture["content"]}, 
                            "finish_reason": "stop"
                        }],
                        usage={"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
                    )

            ## [RECORD / LIVE MODE] 실제 통신 수행 및 (Record 시) 결과 저장
            try:
                # 동기/비동기 호환 실행 보장
                if asyncio.iscoroutinefunction(original_execute):
                    res = await original_execute(msg)
                else:
                    res = original_execute(msg)
                
                if vcr.mode == "record" and phase_id:
                    if original_kwargs.get("stream"):
                        vcr.save_fixture(phase_id, "[Streaming Content Recorded]")
                    else:
                        content = StateTraverser.resolve(res, "choices.0.message.content", "")
                        vcr.save_fixture(phase_id, content)
                        
                return res
                
            except Exception as e:
                # 네트워크 장애나 통신 에러 자체를 파일에 기록 (Fallback 테스트용)
                if vcr.mode == "record" and phase_id:
                    vcr.save_fixture(phase_id, "", is_error=True, error_msg=str(e))
                raise

        # 어댑터 실행부 치환 및 패치 완료 마커 삽입
        adapter.execute = vcr_execute
        adapter._is_vcr_patched = True
        
        return adapter

    # 어댑터 레지스트리의 싱글톤 반환 로직 자체를 몽키 패치
    AdapterRegistry.get_adapter = patched_get_adapter