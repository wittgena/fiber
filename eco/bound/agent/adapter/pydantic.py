# eco.bound.agent.adapter.pydantic
## @lineage: bound.eco.agent.adapter.pydantic
## @lineage: bound.agent.adapter.pydantic
## @lineage: ext.router.adapter.pydantic
## @lineage: router.adapter.pydantic
## @lineage: engine.adapter.pydantic
## @lineage: bound.adapter.pydantic
## @lineage: eco.adapter.pydantic
## @lineage: eco.runtime.pydantic
## @lineage: eco.llama.runtime.pydantic
## @lineage: runtime.bound.llama.bridge.pydantic
import pydantic
from pydantic import (
    AnyUrl,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    FilePath,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
    PlainSerializer,
    PrivateAttr,
    Secret,
    SecretStr,
    SerializationInfo,
    SerializeAsAny,
    SerializerFunctionWrapHandler,
    StrictFloat,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
    ValidationInfo,
    WithJsonSchema,
    WrapSerializer,
    create_model,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic.json_schema import JsonSchemaValue
import pydantic_settings
from pydantic_settings import BaseSettings, SettingsConfigDict
import pydantic_core
from pydantic_core import CoreSchema, core_schema