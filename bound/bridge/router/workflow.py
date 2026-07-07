# bound.bridge.router.workflow
## @lineage: bound.transport.router.workflow
## @lineage: bound.router.adapter.workflow
## @lineage: bound.bridge.adapter.workflow
## @lineage: xphi.trans.workflow.route
## @lineage: xphi.flow.workflow.route
## @lineage: xphi.flow.route.workflow
## @lineage: anchor.model.router.workflow
import asyncio
import inspect
import re
from functools import wraps
from watcher.plane.emitter import get_emitter

log = get_emitter("router.workflow")

class Event:
    """순수 Python 이벤트 버스"""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class StartEvent(Event): pass

class StopEvent(Event):
    def __init__(self, result=None, **kwargs):
        super().__init__(**kwargs)
        self.result = result

def step(func):
    """메서드를 워크플로우 스텝으로 마킹하는 데코레이터"""
    func.__step_config = True
    return func

class Workflow:
    """
    [Native Micro-Engine]
    LlamaIndex의 이벤트 루프를 순수 파이썬으로 완벽히 복제한 경량 엔진.
    타입 힌트(Annotation)를 읽어 이벤트를 다음 @step으로 전달합니다.
    """
    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    async def run(self, **kwargs):
        current_event = StartEvent(**kwargs)
        
        # 이벤트가 StopEvent가 될 때까지 타입 매칭 루프 구동
        while not isinstance(current_event, StopEvent):
            next_step = self._find_step_for_event(current_event)
            if not next_step:
                log.error(f"[Workflow] Unhandled Event: {type(current_event).__name__}. Halting.")
                break
            
            current_event = await asyncio.wait_for(next_step(current_event), timeout=self.timeout)
            
        if isinstance(current_event, StopEvent):
            return current_event.result

    def _find_step_for_event(self, event: Event):
        """이벤트 타입을 기반으로 실행할 스텝(@step)을 탐색 (Union Type 완벽 지원)"""
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, "__step_config"):
                sig = inspect.signature(func)
                params = list(sig.parameters.values())
                if len(params) >= 2:
                    annotation = params[1].annotation
                    
                    # 1. 파이썬 3.10+ 의 'A | B' (types.UnionType) 또는 'Union[A, B]' 해체
                    origin = typing.get_origin(annotation)
                    if origin is typing.Union or origin is types.UnionType:
                        expected_types = typing.get_args(annotation)
                    else:
                        expected_types = (annotation,)
                    
                    # 2. 다중 타입 중 하나라도 일치하면 해당 스텝 반환
                    if isinstance(event, expected_types):
                        return getattr(self, name)
        return None

class ProcessEvent(Event):
    status: str = "success"

class FinalizeEvent(Event):
    status: str = "success"

class ErrorEvent(Event):
    msg: str = ""

## @router.decorator
def router_rules(cls):
    meta = getattr(cls, "Meta", None)
    meta_trans_rules = getattr(meta, "trans_rules", {}) if meta else {}
    meta_flow = getattr(meta, "flow", []) if meta else []

    docstring = cls.__doc__ or ""
    trans_rule_match = re.search(r'@trans\.rule:\s*(.+)', docstring)
    flow_match = re.search(r'@flow:\s*(.+)', docstring)
    
    doc_trans_rule = trans_rule_match.group(1).strip() if trans_rule_match else None
    doc_flow_rule = flow_match.group(1).strip() if flow_match else None

    step_methods = [
        name for name, func in inspect.getmembers(cls, predicate=inspect.isfunction)
        if hasattr(func, "__step_config")
    ]

    for method_name in step_methods:
        original_method = getattr(cls, method_name)
        
        @wraps(original_method)
        async def wrapper(self, ev, *args, **kwargs):
            result_event = await original_method(self, ev, *args, **kwargs)
            log.info(f"[Router] Step '{method_name}' 완료. 다음 경로 탐색 중...")

            if not isinstance(result_event, Event):
                return result_event

            status = getattr(result_event, "status", None)

            # [A] 전이(Transition) 룰 평가
            if status and status in meta_trans_rules:
                target_event_cls = meta_trans_rules[status]
                log.warning(f"[Router] Meta.trans_rules 발동: '{status}' 감지, '{target_event_cls.__name__}'로 전이")
                msg = getattr(result_event, "msg", f"Transduction rule activated by status: {status}")
                return target_event_cls(msg=msg)
                    
            elif doc_trans_rule and status == "error" and "error" in doc_trans_rule:
                log.warning("[Router] Docstring @trans.rule 발동 (Fallback): 조건 충족, 강제 전이 발생")
                return ErrorEvent(msg=getattr(result_event, "msg", "Transduction Rule Activated via Docstring"))

            # [B] 흐름(Flow) 룰 평가
            if meta_flow and method_name in meta_flow:
                log.info(f"[Router] Meta.flow 규칙에 따라 진행")
                return result_event
            elif doc_flow_rule:
                log.info(f"[Router] Docstring @flow 규칙에 따라 진행")
                return result_event
            
            # [C] 기본 순차 실행
            log.info("[Router] 명시적 룰 없음. 물리적 코드 순서에 따라 실행")
            return result_event
            
        setattr(cls, method_name, wrapper)
    return cls

## @test
@router_rules
class LegacyWorkflow(Workflow):
    """
    @trans.rule: if event.status == 'error' -> trigger_recovery
    @flow: analyze -> process -> finalize
    """
    @step
    async def analyze(self, ev: StartEvent) -> ProcessEvent | ErrorEvent:
        ## 이 상태가 반환되면 Docstring Fallback에 의해 ErrorEvent로 래핑되어 전이됨
        return ProcessEvent(status="error", msg="레거시 룰셋 테스트 중 에러 발생")
        
    @step
    async def handle_error(self, ev: ErrorEvent) -> StopEvent:
        log.error(f">>> [LegacyWorkflow] 에러 처리: {ev.msg}")
        return StopEvent(result="Legacy Error Handled")

async def main():
    log.info("\n## Legacy Workflow 시작 (LlamaIndex 독립 테스트)")
    workflow = LegacyWorkflow(timeout=10.0)
    result = await workflow.run()
    log.info(f"최종 결과: {result}")

if __name__ == "__main__":
    asyncio.run(main())