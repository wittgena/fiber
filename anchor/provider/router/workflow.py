# anchor.provider.router.workflow
"""@desc: A native, lightweight, metaclass-driven event workflow engine"""
import asyncio
import inspect
import typing
import types
from functools import wraps
from watcher.plane.emitter import get_emitter

log = get_emitter("router.workflow")

class Event:
    """@desc: Pure Python event bus base class"""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class StartEvent(Event): 
    """@desc: Triggers the initialization of the workflow"""
    pass

class StopEvent(Event):
    """@desc: Triggers the termination of the workflow and carries the final result"""
    def __init__(self, result=None, **kwargs):
        super().__init__(**kwargs)
        self.result = result

class ProcessEvent(Event):
    """@desc: Standard event for intermediate processing stages"""
    status: str = "success"

class FinalizeEvent(Event):
    """@desc: Standard event for finalizing processing stages"""
    status: str = "success"

class ErrorEvent(Event):
    """@desc: Carries error metadata for fallback routing"""
    msg: str = ""

def step(func):
    """@desc: Decorator marking a method as an executable workflow step"""
    func.__step_config = True
    return func

class WorkflowMeta(type):
    """@desc: Metaclass injecting routing rules at class creation time by parsing the nested Meta class"""
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        if name == "Workflow":
            return cls

        meta = getattr(cls, "Meta", None)
        trans_rules = getattr(meta, "trans_rules", {}) if meta else {}
        meta_flow = getattr(meta, "flow", []) if meta else []
        step_methods = {
            func_name: func for func_name, func in namespace.items()
            if callable(func) and getattr(func, "__step_config", False)
        }

        for method_name, original_method in step_methods.items():
            @wraps(original_method)
            async def wrapper(self, ev, *args, _method_name=method_name, _orig=original_method, **kwargs):
                result_event = await _orig(self, ev, *args, **kwargs)
                log.info(f"[Router] Step '{_method_name}' completed. Searching for the next route...")

                if not isinstance(result_event, Event):
                    return result_event

                status = getattr(result_event, "status", None)

                ## @step.1: Evaluate Transition Rules
                if status and status in trans_rules:
                    target_event_cls = trans_rules[status]
                    log.warning(f"[Router] Meta.trans_rules triggered: Status '{status}' detected, transitioning to '{target_event_cls.__name__}'.")
                    msg = getattr(result_event, "msg", f"Transduction rule activated by status: {status}")
                    return target_event_cls(msg=msg)

                ## @step.2: Evaluate Flow Rules
                if meta_flow and _method_name in meta_flow:
                    log.info("[Router] Proceeding based on Meta.flow rule.")
                    return result_event
                
                ## @step.3: Default Sequential Execution
                log.info("[Router] No explicit rule detected. Proceeding based on physical code sequence and type matching.")
                return result_event
            setattr(cls, method_name, wrapper)
        return cls

class Workflow(metaclass=WorkflowMeta):
    """@desc: A lightweight native micro-engine replicating LlamaIndex's event loop in pure Python"""
    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout

    async def run(self, **kwargs):
        current_event = StartEvent(**kwargs)
        while not isinstance(current_event, StopEvent):
            next_step = self._find_step_for_event(current_event)
            if not next_step:
                log.error(f"[Workflow] Unhandled Event: {type(current_event).__name__}. Halting execution.")
                break
            
            current_event = await asyncio.wait_for(next_step(current_event), timeout=self.timeout)
            
        if isinstance(current_event, StopEvent):
            return current_event.result

    def _find_step_for_event(self, event: Event):
        """@desc: Discovers the next @step to execute based on event types"""
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.isfunction):
            if hasattr(func, "__step_config"):
                sig = inspect.signature(func)
                params = list(sig.parameters.values())
                if len(params) >= 2:
                    annotation = params[1].annotation
                    origin = typing.get_origin(annotation)
                    if origin is typing.Union or origin is types.UnionType:
                        expected_types = typing.get_args(annotation)
                    else:
                        expected_types = (annotation,)
                    
                    if isinstance(event, expected_types):
                        return getattr(self, name)
        return None

class CleanWorkflow(Workflow):
    """@desc: An example workflow utilizing the metaclass architecture"""
    class Meta:
        trans_rules = {"error": ErrorEvent}
        flow = ["analyze", "process", "finalize"]

    @step
    async def analyze(self, ev: StartEvent) -> ProcessEvent | ErrorEvent:
        return ProcessEvent(status="error", msg="Error occurred during clean ruleset testing.")
        
    @step
    async def handle_error(self, ev: ErrorEvent) -> StopEvent:
        """@desc: Fallback route handling errors securely"""
        log.error(f">>> [CleanWorkflow] Error Handled: {ev.msg}")
        return StopEvent(result="Clean Error Handled Successfully")

async def main():
    log.info("\n## Starting Clean Workflow (Metaclass-driven)")
    workflow = CleanWorkflow(timeout=10.0)
    result = await workflow.run()
    log.info(f"Final Execution Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())