# bound.adapter.llama.settings
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from anchor.provider.model.token.splitter import TokenSplitter
from anchor.provider.model.token.window import ContextWindow
from anchor.provider.model.token.counter import token_counter
from anchor.surface.registry.provider import model_cost

from bound.adapter.llama.prompts.utils import is_chat_model
from bound.adapter.llama.base.embeddings.base import BaseEmbedding
from bound.adapter.llama.schema import TransformComponent
from bound.adapter.llama.types import PydanticProgramMode

from xphi.loop.callback.base import BaseCallbackHandler, CallbackManager
from xphi.loop.flow.embedding.utils import EmbedType, resolve_embed_model
from xphi.loop.flow.llm.llm import LLM
from xphi.loop.flow.llm.utils import LLMType, resolve_llm

@dataclass
class _Settings:
    """
    Settings for the system, lazily initialized.
    @manifold: LlamaIndex 의존성을 걷어내고 내부 토폴로지로 대체된 순수 설정 객체
    """
    _llm: Optional[LLM] = None
    _embed_model: Optional[BaseEmbedding] = None
    _callback_manager: Optional[CallbackManager] = None
    
    # 내부 엔진으로 교체된 타입 선언
    _node_parser: Optional[TokenSplitter] = None
    _context_window: Optional[ContextWindow] = None
    _transformations: Optional[List[TransformComponent]] = None

    # ---- LLM & Embedding (유지) ----
    
    @property
    def llm(self) -> LLM:
        if self._llm is None:
            self._llm = resolve_llm("default")
        if self._callback_manager is not None:
            self._llm.callback_manager = self._callback_manager
        return self._llm

    @llm.setter
    def llm(self, llm: LLMType) -> None:
        self._llm = resolve_llm(llm)

    @property
    def pydantic_program_mode(self) -> PydanticProgramMode:
        return self.llm.pydantic_program_mode

    @pydantic_program_mode.setter
    def pydantic_program_mode(self, pydantic_program_mode: PydanticProgramMode) -> None:
        self.llm.pydantic_program_mode = pydantic_program_mode

    @property
    def embed_model(self) -> BaseEmbedding:
        if self._embed_model is None:
            self._embed_model = resolve_embed_model("default")
        if self._callback_manager is not None:
            self._embed_model.callback_manager = self._callback_manager
        return self._embed_model

    @embed_model.setter
    def embed_model(self, embed_model: EmbedType) -> None:
        self._embed_model = resolve_embed_model(embed_model)

    # ---- Callbacks (유지 및 정리) ----

    @property
    def callback_manager(self) -> CallbackManager:
        if self._callback_manager is None:
            self._callback_manager = CallbackManager()
        return self._callback_manager

    @callback_manager.setter
    def callback_manager(self, callback_manager: CallbackManager) -> None:
        self._callback_manager = callback_manager

    # ---- Internal Node Parser (TokenSplitter) ----

    @property
    def node_parser(self) -> TokenSplitter:
        """
        LlamaIndex의 SentenceSplitter를 대체하는 내부 TokenSplitter 반환.
        지연 초기화(Lazy Initialization) 수행.
        """
        if self._node_parser is None:
            # 기본 모델(gpt-3.5-turbo 등)의 메타데이터를 사용하여 초기화
            safe_model = getattr(self.llm, "model", "gpt-3.5-turbo") if self._llm else "gpt-3.5-turbo"
            self._node_parser = TokenSplitter(
                chunk_size=1024,  # 기본값
                chunk_overlap=20, # 기본값
                model=safe_model
            )
        return self._node_parser

    @node_parser.setter
    def node_parser(self, node_parser: TokenSplitter) -> None:
        self._node_parser = node_parser

    # 기존 LlamaIndex 인터페이스와의 호환성을 위한 Property 유지
    @property
    def chunk_size(self) -> int:
        return self.node_parser.chunk_size

    @chunk_size.setter
    def chunk_size(self, chunk_size: int) -> None:
        self.node_parser.chunk_size = chunk_size

    @property
    def chunk_overlap(self) -> int:
        return self.node_parser.chunk_overlap

    @chunk_overlap.setter
    def chunk_overlap(self, chunk_overlap: int) -> None:
        self.node_parser.chunk_overlap = chunk_overlap

    @property
    def text_splitter(self) -> TokenSplitter:
        """Alias for node_parser"""
        return self.node_parser

    @text_splitter.setter
    def text_splitter(self, text_splitter: TokenSplitter) -> None:
        self.node_parser = text_splitter

    # ---- Context Window (PromptHelper 대체) ----

    @property
    def context_window_engine(self) -> ContextWindow:
        """
        LlamaIndex의 PromptHelper를 대체하는 안전한 ContextWindow 인스턴스를 반환합니다.
        """
        if self._context_window is None:
            from anchor.provider.model.token.encoder import encode, decode
            safe_model = getattr(self.llm, "model", "gpt-3.5-turbo") if self._llm else "gpt-3.5-turbo"
            
            self._context_window = ContextWindow(
                encode_fn=lambda text: encode(model=safe_model, text=text),
                decode_fn=lambda tokens: decode(model=safe_model, tokens=tokens)
            )
        return self._context_window

    @property
    def context_window(self) -> int:
        """
        현재 설정된 LLM의 최대 컨텍스트 윈도우 크기를 반환합니다.
        """
        if self._llm and hasattr(self._llm, "metadata"):
            return self._llm.metadata.context_window
            
        safe_model = getattr(self.llm, "model", "gpt-3.5-turbo") if self._llm else "gpt-3.5-turbo"
        # model_cost 레지스트리에서 값을 찾아 반환, 없으면 기본값 4096
        if safe_model in model_cost:
            return model_cost[safe_model].get("max_input_tokens", model_cost[safe_model].get("max_tokens", 4096))
        return 4096

    @property
    def num_output(self) -> int:
        """현재 설정된 LLM의 최대 출력 토큰 크기를 반환합니다."""
        if self._llm and hasattr(self._llm, "metadata"):
            return self._llm.metadata.num_output
        return 256 # 기본 안전 값

    # ---- Transformations ----

    @property
    def transformations(self) -> List[TransformComponent]:
        if self._transformations is None:
            # TokenSplitter 인스턴스 자체가 TransformComponent 인터페이스를 
            # 구현하도록 래핑(Wrapping)하거나 캐스팅할 수 있습니다.
            self._transformations = [self.node_parser]
        return self._transformations

    @transformations.setter
    def transformations(self, transformations: List[TransformComponent]) -> None:
        self._transformations = transformations


# Singleton
Settings = _Settings()