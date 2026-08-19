# eco.bound.agent.adapter.types.schema
## @lineage: bound.eco.agent.adapter.types.schema
## @lineage: bound.agent.adapter.types.schema
## @lineage: ext.router.adapter.types.schema
## @lineage: router.adapter.types.schema
## @lineage: engine.adapter.types.schema
## @lineage: bound.adapter.types.schema
## @lineage: eco.adapter.types.schema
from __future__ import annotations
import base64
import json
import logging
import pickle
import textwrap
import uuid
from abc import ABC, abstractmethod
from binascii import Error as BinasciiError
from dataclasses import dataclass
from enum import Enum, auto
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Union,
)

import filetype
import requests
from dataclasses_json import DataClassJsonMixin
from deprecated import deprecated
from typing_extensions import Self
from PIL import Image

from eco.bound.agent.adapter.pydantic import (
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    GetJsonSchemaHandler,
    JsonSchemaValue,
    PlainSerializer,
    SerializationInfo,
    SerializeAsAny,
    SerializerFunctionWrapHandler,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_serializer,
)
from eco.bound.agent.adapter.pydantic import CoreSchema
from eco.bound.agent.adapter.parser import SAMPLE_TEXT, truncate_text

if TYPE_CHECKING:
    from haystack.schema import Document as HaystackDocument
    from semantic_kernel.memory.memory_record import MemoryRecord
    from eco.bound.agent.llm.model.types.block import BaseContentBlock

DEFAULT_TEXT_NODE_TMPL = "{metadata_str}\n\n{content}"
DEFAULT_METADATA_TMPL = "{key}: {value}"
TRUNCATE_LENGTH = 350
WRAP_WIDTH = 70

ImageType = Union[str, BytesIO]
logger = logging.getLogger(__name__)

EnumNameSerializer = PlainSerializer(
    lambda e: e.value, return_type="str", when_used="always"
)

class BaseComponent(BaseModel):
    """Base component object to capture class names."""

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)

        if "properties" in json_schema:
            json_schema["properties"]["class_name"] = {
                "title": "Class Name",
                "type": "string",
                "default": cls.class_name(),
            }
        return json_schema

    @classmethod
    def class_name(cls) -> str:
        return "base_component"

    def json(self, **kwargs: Any) -> str:
        return self.to_json(**kwargs)

    @model_serializer(mode="wrap")
    def custom_model_dump(
        self, handler: SerializerFunctionWrapHandler, info: SerializationInfo
    ) -> Dict[str, Any]:
        data = handler(self)
        data["class_name"] = self.class_name()
        return data

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        return self.model_dump(**kwargs)

    def __getstate__(self) -> Dict[str, Any]:
        state = super().__getstate__()
        keys_to_remove = []
        for key, val in state["__dict__"].items():
            try:
                pickle.dumps(val)
            except Exception:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            logging.warning(f"Removing unpickleable attribute {key}")
            del state["__dict__"][key]

        keys_to_remove = []
        private_attrs = state.get("__pydantic_private__", None)
        if private_attrs:
            for key, val in state["__pydantic_private__"].items():
                try:
                    pickle.dumps(val)
                except Exception:
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                logging.warning(f"Removing unpickleable private attribute {key}")
                del state["__pydantic_private__"][key]

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        try:
            self.__init__(**state["__dict__"])  # type: ignore
        except Exception:
            super().__setstate__(state)

    def to_dict(self, **kwargs: Any) -> Dict[str, Any]:
        data = self.dict(**kwargs)
        data["class_name"] = self.class_name()
        return data

    def to_json(self, **kwargs: Any) -> str:
        data = self.to_dict(**kwargs)
        return json.dumps(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs: Any) -> Self:  # type: ignore
        data = dict(data)
        if isinstance(kwargs, dict):
            data.update(kwargs)
        data.pop("class_name", None)
        return cls(**data)

    @classmethod
    def from_json(cls, data_str: str, **kwargs: Any) -> Self:  # type: ignore
        data = json.loads(data_str)
        return cls.from_dict(data, **kwargs)

class TransformComponent(BaseComponent, ABC):
    """Base class for transform components in standalone systems."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @abstractmethod
    def __call__(self, nodes: Sequence[BaseNode], **kwargs: Any) -> Sequence[BaseNode]:
        """Transform nodes."""

    async def acall(
        self, nodes: Sequence[BaseNode], **kwargs: Any
    ) -> Sequence[BaseNode]:
        """Async transform nodes."""
        return self.__call__(nodes, **kwargs)


class NodeRelationship(str, Enum):
    SOURCE = auto()
    PREVIOUS = auto()
    NEXT = auto()
    PARENT = auto()
    CHILD = auto()


class ObjectType(str, Enum):
    TEXT = auto()
    IMAGE = auto()
    INDEX = auto()
    DOCUMENT = auto()
    MULTIMODAL = auto()


class Modality(str, Enum):
    TEXT = auto()
    IMAGE = auto()
    AUDIO = auto()
    VIDEO = auto()


class MetadataMode(str, Enum):
    ALL = "all"
    EMBED = "embed"
    LLM = "llm"
    NONE = "none"


class RelatedNodeInfo(BaseComponent):
    node_id: str
    node_type: Annotated[ObjectType, EnumNameSerializer] | str | None = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    hash: Optional[str] = None

    @classmethod
    def class_name(cls) -> str:
        return "RelatedNodeInfo"


RelatedNodeType = Union[RelatedNodeInfo, List[RelatedNodeInfo]]


class BaseNode(BaseComponent):
    """Base node Object interface."""

    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)

    id_: str = Field(
        default_factory=lambda: str(uuid.uuid4()), description="Unique ID of the node."
    )
    embedding: Optional[List[float]] = Field(
        default=None, description="Embedding of the node."
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="A flat dictionary of metadata fields",
        alias="extra_info",
    )
    excluded_embed_metadata_keys: List[str] = Field(
        default_factory=list,
        description="Metadata keys that are excluded from text for the embed model.",
    )
    excluded_llm_metadata_keys: List[str] = Field(
        default_factory=list,
        description="Metadata keys that are excluded from text for the LLM.",
    )
    relationships: Dict[
        Annotated[NodeRelationship, EnumNameSerializer],
        RelatedNodeType,
    ] = Field(
        default_factory=dict,
        description="A mapping of relationships to other node information.",
    )
    metadata_template: str = Field(
        default=DEFAULT_METADATA_TMPL,
        description="Template for how metadata is formatted.",
    )
    metadata_separator: str = Field(
        default="\n",
        description="Separator between metadata fields.",
        alias="metadata_seperator",
    )

    @classmethod
    def get_type(cls) -> str: ...

    @abstractmethod
    def get_content(self, metadata_mode: MetadataMode = MetadataMode.ALL) -> str: ...

    @abstractmethod
    def get_content_blocks(
        self, metadata_mode: MetadataMode = MetadataMode.ALL
    ) -> list[BaseContentBlock]: ...

    def get_metadata_str(self, mode: MetadataMode = MetadataMode.ALL) -> str:
        if mode == MetadataMode.NONE:
            return ""

        usable_metadata_keys = set(self.metadata.keys())
        if mode == MetadataMode.LLM:
            for key in self.excluded_llm_metadata_keys:
                if key in usable_metadata_keys:
                    usable_metadata_keys.remove(key)
        elif mode == MetadataMode.EMBED:
            for key in self.excluded_embed_metadata_keys:
                if key in usable_metadata_keys:
                    usable_metadata_keys.remove(key)

        return self.metadata_separator.join(
            [
                self.metadata_template.format(key=key, value=str(value))
                for key, value in self.metadata.items()
                if key in usable_metadata_keys
            ]
        )

    def get_metadata_content_blocks(
        self, metadata_mode: MetadataMode
    ) -> list[BaseContentBlock]:
        from eco.bound.agent.llm.model.types.block import TextBlock

        if metadata_mode == MetadataMode.NONE:
            return []
        metadata_str = self.get_metadata_str(mode=metadata_mode).strip()
        if not metadata_str:
            return []
        return [TextBlock(text=metadata_str)]

    @abstractmethod
    def set_content(self, value: Any) -> None: ...

    @property
    def hash(self) -> str: ...

    @property
    def node_id(self) -> str:
        return self.id_

    @node_id.setter
    def node_id(self, value: str) -> None:
        self.id_ = value

    @property
    def source_node(self) -> Optional[RelatedNodeInfo]:
        if NodeRelationship.SOURCE not in self.relationships:
            return None
        relation = self.relationships[NodeRelationship.SOURCE]
        if isinstance(relation, list):
            raise ValueError("Source object must be a single RelatedNodeInfo object")
        return relation

    @property
    def prev_node(self) -> Optional[RelatedNodeInfo]:
        if NodeRelationship.PREVIOUS not in self.relationships:
            return None
        relation = self.relationships[NodeRelationship.PREVIOUS]
        if not isinstance(relation, RelatedNodeInfo):
            raise ValueError("Previous object must be a single RelatedNodeInfo object")
        return relation

    @property
    def next_node(self) -> Optional[RelatedNodeInfo]:
        if NodeRelationship.NEXT not in self.relationships:
            return None
        relation = self.relationships[NodeRelationship.NEXT]
        if not isinstance(relation, RelatedNodeInfo):
            raise ValueError("Next object must be a single RelatedNodeInfo object")
        return relation

    @property
    def parent_node(self) -> Optional[RelatedNodeInfo]:
        if NodeRelationship.PARENT not in self.relationships:
            return None
        relation = self.relationships[NodeRelationship.PARENT]
        if not isinstance(relation, RelatedNodeInfo):
            raise ValueError("Parent object must be a single RelatedNodeInfo object")
        return relation

    @property
    def child_nodes(self) -> Optional[List[RelatedNodeInfo]]:
        if NodeRelationship.CHILD not in self.relationships:
            return None
        relation = self.relationships[NodeRelationship.CHILD]
        if not isinstance(relation, list):
            raise ValueError("Child objects must be a list of RelatedNodeInfo objects.")
        return relation

    @property
    def ref_doc_id(self) -> Optional[str]:
        source_node = self.source_node
        if source_node is None:
            return None
        return source_node.node_id

    @property
    @deprecated(version="0.12.2", reason="'extra_info' is deprecated, use 'metadata' instead.")
    def extra_info(self) -> dict[str, Any]:
        return self.metadata

    @extra_info.setter
    @deprecated(version="0.12.2", reason="'extra_info' is deprecated, use 'metadata' instead.")
    def extra_info(self, extra_info: dict[str, Any]) -> None:
        self.metadata = extra_info

    def __str__(self) -> str:
        source_text_truncated = truncate_text(self.get_content().strip(), TRUNCATE_LENGTH)
        source_text_wrapped = textwrap.fill(f"Text: {source_text_truncated}\n", width=WRAP_WIDTH)
        return f"Node ID: {self.node_id}\n{source_text_wrapped}"

    def get_embedding(self) -> List[float]:
        if self.embedding is None:
            raise ValueError("embedding not set.")
        return self.embedding

    def as_related_node_info(self) -> RelatedNodeInfo:
        return RelatedNodeInfo(
            node_id=self.node_id,
            node_type=self.get_type(),
            metadata=self.metadata,
            hash=self.hash,
        )


EmbeddingKind = Literal["sparse", "dense"]


class MediaResource(BaseModel):
    embeddings: dict[EmbeddingKind, list[float]] | None = Field(default=None)
    data: bytes | None = Field(default=None, exclude=True)
    text: str | None = Field(default=None)
    path: Path | None = Field(default=None)
    url: AnyUrl | None = Field(default=None)
    mimetype: str | None = Field(default=None)

    model_config = {"validate_default": True}

    @field_validator("data", mode="after")
    @classmethod
    def validate_data(cls, v: bytes | None, info: ValidationInfo) -> bytes | None:
        if v is None:
            return v
        try:
            base64.b64decode(v, validate=True)
        except BinasciiError:
            return base64.b64encode(v)
        return v

    @field_validator("mimetype", mode="after")
    @classmethod
    def validate_mimetype(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is not None:
            return v
        b64_data = info.data.get("data")
        if b64_data:
            decoded_data = base64.b64decode(b64_data)
            if guess := filetype.guess(decoded_data):
                return guess.mime
        rpath: str | None = info.data.get("path")
        if rpath:
            extension = Path(rpath).suffix.replace(".", "")
            if ftype := filetype.get_type(ext=extension):
                return ftype.mime
        return v

    @field_serializer("path")
    def serialize_path(self, path: Optional[Path], _info: ValidationInfo) -> Optional[str]:
        if path is None:
            return path
        return str(path)

    @property
    def hash(self) -> str:
        bits: list[str] = []
        if self.text is not None:
            bits.append("<empty_string>" if self.text == "" else self.text)
        if self.data is not None:
            bits.append(str(sha256(self.data).hexdigest()))
        if self.path is not None:
            bits.append(str(sha256(str(self.path).encode("utf-8")).hexdigest()))
        if self.url is not None:
            bits.append(str(sha256(str(self.url).encode("utf-8")).hexdigest()))

        if not bits:
            return ""
        return str(sha256("".join(bits).encode("utf-8")).hexdigest())


class Node(BaseNode):
    text_resource: MediaResource | None = Field(default=None)
    image_resource: MediaResource | None = Field(default=None)
    audio_resource: MediaResource | None = Field(default=None)
    video_resource: MediaResource | None = Field(default=None)
    text_template: str = Field(default=DEFAULT_TEXT_NODE_TMPL)

    @classmethod
    def class_name(cls) -> str:
        return "Node"

    @classmethod
    def get_type(cls) -> str:
        return ObjectType.MULTIMODAL

    def get_content(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> str:
        if self.text_resource:
            metadata_str = self.get_metadata_str(metadata_mode)
            if metadata_mode == MetadataMode.NONE or not metadata_str:
                return self.text_resource.text or ""
            return self.text_template.format(
                content=self.text_resource.text or "",
                metadata_str=metadata_str,
            ).strip()
        return ""

    def get_content_blocks(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> list[BaseContentBlock]:
        from eco.bound.agent.llm.model.types.block import (
            TextBlock, ImageBlock, AudioBlock, VideoBlock,
        )
        blocks: list[BaseContentBlock] = []
        blocks.extend(self.get_metadata_content_blocks(metadata_mode))
        if self.text_resource:
            blocks.append(TextBlock(text=self.text_resource.text or ""))
        if self.image_resource:
            blocks.append(ImageBlock(
                image=self.image_resource.data, url=self.image_resource.url,
                path=self.image_resource.path, image_mimetype=self.image_resource.mimetype,
            ))
        if self.audio_resource:
            guess = filetype.get_type(mime=self.audio_resource.mimetype)
            blocks.append(AudioBlock(
                audio=self.audio_resource.data, url=self.audio_resource.url,
                path=self.audio_resource.path, format=guess.extension if guess else None,
            ))
        if self.video_resource:
            blocks.append(VideoBlock(
                video=self.video_resource.data, url=self.video_resource.url,
                path=self.video_resource.path, video_mimetype=self.video_resource.mimetype,
            ))
        return blocks

    def set_content(self, value: str) -> None:
        self.text_resource = MediaResource(text=value)

    @property
    def hash(self) -> str:
        doc_identities = []
        metadata_str = self.get_metadata_str(mode=MetadataMode.ALL)
        if metadata_str:
            doc_identities.append(metadata_str)
        if self.audio_resource is not None:
            doc_identities.append(self.audio_resource.hash)
        if self.image_resource is not None:
            doc_identities.append(self.image_resource.hash)
        if self.text_resource is not None:
            doc_identities.append(self.text_resource.hash)
        if self.video_resource is not None:
            doc_identities.append(self.video_resource.hash)
        return str(sha256("-".join(doc_identities).encode("utf-8", "surrogatepass")).hexdigest())


class TextNode(BaseNode):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "text_resource" in kwargs:
            tr = kwargs.pop("text_resource")
            kwargs["text"] = tr.text if isinstance(tr, MediaResource) else tr["text"]
        super().__init__(*args, **kwargs)

    text: str = Field(default="")
    mimetype: str = Field(default="text/plain")
    start_char_idx: Optional[int] = Field(default=None)
    end_char_idx: Optional[int] = Field(default=None)
    text_template: str = Field(default=DEFAULT_TEXT_NODE_TMPL)

    @classmethod
    def class_name(cls) -> str:
        return "TextNode"

    @property
    def hash(self) -> str:
        doc_identity = str(self.text) + str(self.metadata)
        return str(sha256(doc_identity.encode("utf-8", "surrogatepass")).hexdigest())

    @classmethod
    def get_type(cls) -> str:
        return ObjectType.TEXT

    def get_content(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> str:
        metadata_str = self.get_metadata_str(mode=metadata_mode).strip()
        if metadata_mode == MetadataMode.NONE or not metadata_str:
            return self.text
        return self.text_template.format(content=self.text, metadata_str=metadata_str).strip()

    def get_content_blocks(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> list[BaseContentBlock]:
        from eco.bound.agent.llm.model.types.block import TextBlock
        blocks: list[BaseContentBlock] = []
        blocks.extend(self.get_metadata_content_blocks(metadata_mode))
        blocks.append(TextBlock(text=self.text))
        return blocks

    def set_content(self, value: str) -> None:
        self.text = value

    def get_node_info(self) -> Dict[str, Any]:
        return {"start": self.start_char_idx, "end": self.end_char_idx}

    def get_text(self) -> str:
        return self.get_content(metadata_mode=MetadataMode.NONE)

    @property
    @deprecated(version="0.12.2", reason="'node_info' is deprecated, use 'get_node_info' instead.")
    def node_info(self) -> Dict[str, Any]:
        return self.get_node_info()


class ImageNode(TextNode):
    image: Optional[str] = None
    image_path: Optional[str] = None
    image_url: Optional[str] = None
    image_mimetype: Optional[str] = None
    text_embedding: Optional[List[float]] = Field(default=None)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "image_resource" in kwargs:
            ir = kwargs.pop("image_resource")
            if isinstance(ir, MediaResource):
                kwargs["image_path"] = ir.path.as_posix() if ir.path else None
                kwargs["image_url"] = ir.url
                kwargs["image_mimetype"] = ir.mimetype
            else:
                kwargs["image_path"] = ir.get("path", None)
                kwargs["image_url"] = ir.get("url", None)
                kwargs["image_mimetype"] = ir.get("mimetype", None)

        mimetype = kwargs.get("image_mimetype")
        if not mimetype and kwargs.get("image_path") is not None:
            extension = Path(kwargs["image_path"]).suffix.replace(".", "")
            if ftype := filetype.get_type(ext=extension):
                kwargs["image_mimetype"] = ftype.mime
        super().__init__(*args, **kwargs)

    @classmethod
    def get_type(cls) -> str:
        return ObjectType.IMAGE

    @classmethod
    def class_name(cls) -> str:
        return "ImageNode"

    def resolve_image(self) -> ImageType:
        if self.image is not None:
            import base64
            return BytesIO(base64.b64decode(self.image))
        elif self.image_path is not None:
            return self.image_path
        elif self.image_url is not None:
            response = requests.get(self.image_url, timeout=(60, 60))
            return BytesIO(response.content)
        else:
            raise ValueError("No image found in node.")

    @property
    def hash(self) -> str:
        image_str = self.image or "None"
        image_path_str = self.image_path or "None"
        image_url_str = self.image_url or "None"
        image_text = self.text or "None"
        doc_identity = f"{image_str}-{image_path_str}-{image_url_str}-{image_text}"
        return str(sha256(doc_identity.encode("utf-8", "surrogatepass")).hexdigest())

    def get_content_blocks(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> list[BaseContentBlock]:
        from eco.bound.agent.llm.model.types.block import ImageBlock
        blocks: list[BaseContentBlock] = []
        blocks.extend(self.get_metadata_content_blocks(metadata_mode))
        resolved = self.resolve_image()
        image_data = resolved.read() if isinstance(resolved, BytesIO) else None
        blocks.append(ImageBlock(
            image=image_data, url=self.image_url, path=self.image_path, image_mimetype=self.image_mimetype,
        ))
        return blocks


class IndexNode(TextNode):
    index_id: str
    obj: Any = None

    def _serialize_obj(self) -> Any:
        from eco.bound.agent.adapter.storage.docstore.utils import doc_to_json
        try:
            if self.obj is None:
                return None
            elif isinstance(self.obj, BaseNode):
                return doc_to_json(self.obj)
            elif isinstance(self.obj, BaseModel):
                return self.obj.model_dump()
            else:
                return json.dumps(self.obj)
        except Exception:
            raise ValueError("IndexNode obj is not serializable: " + str(self.obj))

    @model_serializer(mode="wrap")
    def custom_model_dump(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> Dict[str, Any]:
        data = super().custom_model_dump(handler, info)
        data["obj"] = self._serialize_obj()
        return data

    def dict(self, **kwargs: Any) -> Dict[str, Any]:
        data = super().dict(**kwargs)
        data["obj"] = self._serialize_obj()
        return data

    @classmethod
    def from_text_node(cls, node: TextNode, index_id: str) -> IndexNode:
        return cls(**node.dict(), index_id=index_id)

    @classmethod
    def from_dict(cls, data: Dict[str, Any], **kwargs: Any) -> Self:  # type: ignore
        output = super().from_dict(data, **kwargs)
        obj = data.get("obj")
        parsed_obj = None

        if isinstance(obj, str):
            parsed_obj = TextNode(text=obj)
        elif isinstance(obj, dict):
            from eco.bound.agent.adapter.storage.docstore.utils import json_to_doc
            try:
                parsed_obj = json_to_doc(obj)
            except Exception:
                parsed_obj = TextNode(text=str(obj))
        output.obj = parsed_obj
        return output

    @classmethod
    def get_type(cls) -> str:
        return ObjectType.INDEX

    @classmethod
    def class_name(cls) -> str:
        return "IndexNode"


class NodeWithScore(BaseComponent):
    node: SerializeAsAny[BaseNode]
    score: Optional[float] = None

    def __str__(self) -> str:
        score_str = "None" if self.score is None else f"{self.score: 0.3f}"
        return f"{self.node}\nScore: {score_str}\n"

    def get_score(self, raise_error: bool = False) -> float:
        if self.score is None:
            if raise_error:
                raise ValueError("Score not set.")
            return 0.0
        return self.score

    @classmethod
    def class_name(cls) -> str:
        return "NodeWithScore"

    @property
    def node_id(self) -> str: return self.node.node_id

    @property
    def id_(self) -> str: return self.node.id_

    @property
    def text(self) -> str:
        if isinstance(self.node, TextNode):
            return self.node.text
        raise ValueError("Node must be a TextNode to get text.")

    @property
    def metadata(self) -> Dict[str, Any]: return self.node.metadata

    @property
    def embedding(self) -> Optional[List[float]]: return self.node.embedding

    def get_text(self) -> str:
        if isinstance(self.node, TextNode):
            return self.node.get_text()
        raise ValueError("Node must be a TextNode to get text.")

    def get_content(self, metadata_mode: MetadataMode = MetadataMode.NONE) -> str:
        return self.node.get_content(metadata_mode=metadata_mode)

    def get_embedding(self) -> List[float]:
        return self.node.get_embedding()


# Document Classes for Readers
class Document(Node):
    """Generic interface for a data document mapped to standalone core repositories."""

    def __init__(self, **data: Any) -> None:
        if "doc_id" in data:
            value = data.pop("doc_id")
            if "id_" in data:
                logging.warning("'doc_id' is deprecated and 'id_' will be used instead")
            else:
                data["id_"] = value

        if "extra_info" in data:
            value = data.pop("extra_info")
            if "metadata" in data:
                logging.warning("'extra_info' is deprecated and 'metadata' will be used instead")
            else:
                data["metadata"] = value

        if data.get("text"):
            text = data.pop("text")
            if "text_resource" in data:
                text_resource = (
                    data["text_resource"]
                    if isinstance(data["text_resource"], MediaResource)
                    else MediaResource.model_validate(data["text_resource"])
                )
                if (text_resource.text or "").strip() != text.strip():
                    logging.warning("'text' is deprecated and 'text_resource' will be used instead")
            else:
                data["text_resource"] = MediaResource(text=text)

        super().__init__(**data)

    @model_serializer(mode="wrap")
    def custom_model_dump(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> Dict[str, Any]:
        data = super().custom_model_dump(handler, info)
        exclude_set = set(info.exclude or [])
        if "text" not in exclude_set:
            data["text"] = self.text
        return data

    @property
    def text(self) -> str:
        return self.get_content()

    @classmethod
    def get_type(cls) -> str:
        return ObjectType.DOCUMENT

    @property
    def doc_id(self) -> str: return self.id_

    @doc_id.setter
    def doc_id(self, id_: str) -> None: self.id_ = id_

    def __str__(self) -> str:
        source_text_truncated = truncate_text(self.get_content().strip(), TRUNCATE_LENGTH)
        source_text_wrapped = textwrap.fill(f"Text: {source_text_truncated}\n", width=WRAP_WIDTH)
        return f"Doc ID: {self.doc_id}\n{source_text_wrapped}"

    @deprecated(version="0.12.2", reason="'get_doc_id' is deprecated, access the 'id_' property instead.")
    def get_doc_id(self) -> str:
        return self.id_

    def to_haystack_format(self) -> HaystackDocument:
        from haystack import Document as HaystackDocument
        return HaystackDocument(content=self.text, meta=self.metadata, embedding=self.embedding, id=self.id_)

    @classmethod
    def from_haystack_format(cls, doc: HaystackDocument) -> Document:
        return cls(text=doc.content, metadata=doc.meta, embedding=doc.embedding, id_=doc.id)

    def to_embedchain_format(self) -> Dict[str, Any]:
        return {"doc_id": self.id_, "data": {"content": self.text, "meta_data": self.metadata}}

    @classmethod
    def from_embedchain_format(cls, doc: Dict[str, Any]) -> Document:
        return cls(text=doc["data"]["content"], metadata=doc["data"]["meta_data"], id_=doc["doc_id"])

    def to_semantic_kernel_format(self) -> MemoryRecord:
        import numpy as np
        from semantic_kernel.memory.memory_record import MemoryRecord
        return MemoryRecord(
            id=self.id_, text=self.text, additional_metadata=self.get_metadata_str(),
            embedding=np.array(self.embedding) if self.embedding else None,
        )

    @classmethod
    def from_semantic_kernel_format(cls, doc: MemoryRecord) -> Document:
        return cls(
            text=doc._text, metadata={"additional_metadata": doc._additional_metadata},
            embedding=doc._embedding.tolist() if doc._embedding is not None else None, id_=doc._id,
        )

    def to_vectorflow(self, client: Any) -> None:
        import tempfile
        with tempfile.NamedTemporaryFile() as f:
            f.write(self.text.encode("utf-8"))
            f.flush()
            client.embed(f.name)

    @classmethod
    def example(cls) -> Document:
        return Document(text=SAMPLE_TEXT, metadata={"filename": "README.md", "category": "codebase"})

    @classmethod
    def class_name(cls) -> str:
        return "Document"

    # [정렬 1] 의존성이 완벽하게 오염되었던 LlamaCloud 관련 폐쇄형(SaaS) Mapper API 완전 삭제 완료


def is_image_pil(file_path: str) -> bool:
    try:
        with Image.open(file_path) as img:
            img.verify()
        return True
    except (IOError, SyntaxError):
        return False


def is_image_url_pil(url: str) -> bool:
    try:
        response = requests.get(url, stream=True, timeout=(60, 60))
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))
        img.verify()
        return True
    except (requests.RequestException, IOError, SyntaxError):
        return False


class ImageDocument(Document):
    def __init__(self, **kwargs: Any) -> None:
        image = kwargs.pop("image", None)
        image_path = kwargs.pop("image_path", None)
        image_url = kwargs.pop("image_url", None)
        image_mimetype = kwargs.pop("image_mimetype", None)
        _ = kwargs.pop("text_embedding", None)

        if image:
            kwargs["image_resource"] = MediaResource(data=image, mimetype=image_mimetype)
        elif image_path:
            if not is_image_pil(image_path):
                raise ValueError("The specified file path is not an accessible image")
            kwargs["image_resource"] = MediaResource(path=image_path, mimetype=image_mimetype)
        elif image_url:
            if not is_image_url_pil(image_url):
                raise ValueError("The specified URL is not an accessible image")
            kwargs["image_resource"] = MediaResource(url=image_url, mimetype=image_mimetype)
        super().__init__(**kwargs)

    @property
    def image(self) -> str | None:
        if self.image_resource and self.image_resource.data:
            return self.image_resource.data.decode("utf-8")
        return None

    @image.setter
    def image(self, image: str) -> None:
        self.image_resource = MediaResource(data=image.encode("utf-8"))

    @property
    def image_path(self) -> str | None:
        return str(self.image_resource.path) if self.image_resource and self.image_resource.path else None

    @image_path.setter
    def image_path(self, image_path: str) -> None:
        self.image_resource = MediaResource(path=Path(image_path))

    @property
    def image_url(self) -> str | None:
        return str(self.image_resource.url) if self.image_resource and self.image_resource.url else None

    @image_url.setter
    def image_url(self, image_url: str) -> None:
        self.image_resource = MediaResource(url=AnyUrl(url=image_url))

    @property
    def image_mimetype(self) -> str | None:
        return self.image_resource.mimetype if self.image_resource else None

    @image_mimetype.setter
    def image_mimetype(self, image_mimetype: str) -> None:
        if self.image_resource:
            self.image_resource.mimetype = image_mimetype

    @property
    def text_embedding(self) -> list[float] | None:
        if self.text_resource and self.text_resource.embeddings:
            return self.text_resource.embeddings.get("dense")
        return None

    @text_embedding.setter
    def text_embedding(self, embeddings: list[float]) -> None:
        if self.text_resource:
            if self.text_resource.embeddings is None:
                self.text_resource.embeddings = {}
            self.text_resource.embeddings["dense"] = embeddings

    @classmethod
    def class_name(cls) -> str:
        return "ImageDocument"

    def resolve_image(self, as_base64: bool = False) -> BytesIO:
        if self.image_resource is None:
            return BytesIO()
        if self.image_resource.data is not None:
            return BytesIO(self.image_resource.data) if as_base64 else BytesIO(base64.b64decode(self.image_resource.data))
        elif self.image_resource.path is not None:
            img_bytes = self.image_resource.path.read_bytes()
            return BytesIO(base64.b64encode(img_bytes)) if as_base64 else BytesIO(img_bytes)
        elif self.image_resource.url is not None:
            response = requests.get(str(self.image_resource.url), timeout=(60, 60))
            img_bytes = response.content
            return BytesIO(base64.b64encode(img_bytes)) if as_base64 else BytesIO(img_bytes)
        else:
            raise ValueError("No image found in the chat message!")


@dataclass
class QueryBundle(DataClassJsonMixin):
    query_str: str
    image_path: Optional[str] = None
    custom_embedding_strs: Optional[List[str]] = None
    embedding: Optional[List[float]] = None

    @property
    def embedding_strs(self) -> List[str]:
        if self.custom_embedding_strs is None:
            return [] if len(self.query_str) == 0 else [self.query_str]
        return self.custom_embedding_strs

    @property
    def embedding_image(self) -> List[ImageType]:
        return [] if self.image_path is None else [self.image_path]

    def __str__(self) -> str:
        return self.query_str


QueryType = Union[str, QueryBundle]