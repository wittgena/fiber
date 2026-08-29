# fiber.llm.router.ext.callback.dispatcher
## @lineage: fiber.dphi.model.ext.callback.dispatcher
## @lineage: dphi.model.ext.callback.dispatcher
## @lineage: dphi.model.callback.dispatcher
## @lineage: agent.llm.router.callback.dispatcher
import asyncio
import inspect
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Callable, Generator, List, Protocol, TypeVar
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar

from pydantic import BaseModel, ConfigDict, Field
from deprecated import deprecated
from xphi.arch.event.next import uuid4

DISPATCHER_SPAN_DECORATED_ATTR = "__dispatcher_span_decorated__"

_logger = logging.getLogger(__name__)
_INSTRUMENT_TAGS_KEY = "instrument_tags"

active_instrument_tags: ContextVar[Dict[str, Any]] = ContextVar(
    "instrument_tags", default={}
)
_R = TypeVar("_R")

class BaseEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    timestamp: datetime = Field(default_factory=datetime.now)
    id_: str = Field(default_factory=lambda: str(uuid4()))
    span_id: Optional[str] = Field(default=None)
    tags: Dict[str, Any] = Field(default={})

    @classmethod
    def class_name(cls) -> str:
        return "BaseEvent"

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        return self.model_dump(**kwargs)

    def model_dump(self, **kwargs: Any) -> Dict[str, Any]:
        data = super().model_dump(**kwargs)
        data["class_name"] = self.class_name()
        return data

class BaseInstrumentationHandler(ABC):
    @classmethod
    @abstractmethod
    def init(cls) -> None:
        pass

class BaseEventHandler(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def class_name(cls) -> str:
        return "BaseEventHandler"

    @abstractmethod
    def handle(self, event: BaseEvent, **kwargs: Any) -> Any:
        pass

    async def ahandle(self, event: BaseEvent, **kwargs: Any) -> Any:
        return self.handle(event, **kwargs)

@contextmanager
def instrument_tags(new_tags: Dict[str, Any]) -> Generator[None, None, None]:
    token = active_instrument_tags.set(new_tags)
    try:
        yield
    finally:
        active_instrument_tags.reset(token)

class EventDispatcher(Protocol):
    def __call__(self, event: BaseEvent, **kwargs: Any) -> None: ...

class Dispatcher(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    name: str = Field(default_factory=str)
    event_handlers: List[BaseEventHandler] = Field(default=[])
    span_handlers: List[Any] = Field(default=[])
    parent_name: str = Field(default_factory=str)
    manager: Optional["Manager"] = Field(default=None)
    root_name: str = Field(default="root")
    propagate: bool = Field(default=True)
    current_span_ids: Optional[Dict[Any, str]] = Field(default_factory=dict) # type: ignore

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    @property
    def parent(self) -> Optional["Dispatcher"]:
        if not self.manager or not self.parent_name:
            return None
        return self.manager.dispatchers.get(self.parent_name)

    @property
    def root(self) -> Optional["Dispatcher"]:
        if not self.manager:
            return None
        return self.manager.dispatchers.get(self.root_name)

    def _walk_span_handlers(self) -> Generator[Any, None, None]:
        yield from []

    def add_event_handler(self, handler: BaseEventHandler) -> None:
        self.event_handlers.append(handler)

    def add_span_handler(self, handler: Any) -> None:
        pass

    def event(self, event: BaseEvent, **kwargs: Any) -> None:
        # 튜닝 포인트: 핸들러가 없고 상위 전파도 안 할 거면 즉시 리턴 (루프/태그 갱신 비용 0)
        if not self.event_handlers and not self.propagate:
            return

        c: Optional[Dispatcher] = self
        event.tags.update(active_instrument_tags.get())
        while c:
            for h in c.event_handlers:
                try:
                    h.handle(event, **kwargs)
                except BaseException:
                    pass
            if not c.propagate:
                c = None
            else:
                c = c.parent

    async def aevent(self, event: BaseEvent, **kwargs: Any) -> None:
        # 튜닝 포인트: 핸들러가 없고 상위 전파도 안 할 거면 즉시 리턴
        if not self.event_handlers and not self.propagate:
            return

        c: Optional[Dispatcher] = self
        event.tags.update(active_instrument_tags.get())
        tasks: List[asyncio.Task] = []
        while c:
            for h in c.event_handlers:
                try:
                    tasks.append(asyncio.create_task(h.ahandle(event, **kwargs)))
                except BaseException:
                    pass
            if not c.propagate:
                c = None
            else:
                c = c.parent
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    @deprecated(version="0.10.41", reason="Use `event()` directly.")
    def get_dispatch_event(self) -> EventDispatcher:
        return self.event

    def span_enter(self, *args: Any, **kwargs: Any) -> None: pass
    def span_drop(self, *args: Any, **kwargs: Any) -> None: pass
    def span_exit(self, *args: Any, **kwargs: Any) -> None: pass
    
    def capture_propagation_context(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        tags = active_instrument_tags.get()
        if tags:
            result[_INSTRUMENT_TAGS_KEY] = dict(tags)
        return result

    def restore_propagation_context(self, context: Dict[str, Any]) -> None:
        tags = context.get(_INSTRUMENT_TAGS_KEY)
        if tags:
            active_instrument_tags.set(dict(tags))

    def shutdown(self) -> None: pass
    
    # 튜닝 포인트: span 트레이싱 래핑 완전히 무력화
    def span(self, func: Callable[..., _R]) -> Callable[..., _R]: return func

    @property
    def log_name(self) -> str:
        if self.parent:
            return f"{self.parent.name}.{self.name}"
        return self.name

class Manager:
    def __init__(self, root: Dispatcher) -> None:
        self.dispatchers: Dict[str, Dispatcher] = {root.name: root}

    def add_dispatcher(self, d: Dispatcher) -> None:
        if d.name not in self.dispatchers:
            self.dispatchers[d.name] = d

Dispatcher.model_rebuild()

_root_dispatcher = Dispatcher(name="root", root_name="root", propagate=False)
_global_manager = Manager(root=_root_dispatcher)
_root_dispatcher.manager = _global_manager

def get_dispatcher(name: Optional[str] = None) -> Dispatcher:
    if not name or name == "root":
        return _root_dispatcher
    
    if name in _global_manager.dispatchers:
        return _global_manager.dispatchers[name]
        
    parent_name = ".".join(name.split(".")[:-1])
    if not parent_name:
        parent_name = "root"
        
    if parent_name not in _global_manager.dispatchers:
        get_dispatcher(parent_name)
        
    new_dispatcher = Dispatcher(
        name=name,
        parent_name=parent_name,
        manager=_global_manager,
        root_name="root",
        propagate=False
    )
    _global_manager.add_dispatcher(new_dispatcher)
    return new_dispatcher

dispatcher = _root_dispatcher