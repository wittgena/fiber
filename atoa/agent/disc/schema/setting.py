# atoa.agent.disc.schema.setting
## @lineage: atoa.disc.schema.setting
## @lineage: agent.disc.schema.setting
## @lineage: agent.disc.setting
## @lineage: agent.disc.config.setting
from enum import Enum
from pathlib import Path
from typing import Any, Literal, TypeVar, get_args, get_origin
from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    field_validator,
)
from pydantic.fields import FieldInfo
from eco.agent.residue.convset import (
    SETTINGS_METADATA_KEY,
    SETTINGS_SECTION_METADATA_KEY,
    SettingProminence,
    SettingsFieldMetadata,
    SettingsSectionMetadata,
)
from atoa.agent.disc.schema.metadata import SettingsSchema, SettingsValueType, SettingsChoiceValue, SettingsChoice

from eco.agent.residue.depre import warn_deprecated
from arch.topos.bound.surge.disc import SurgeBaseModel

ReflectorMode = Literal["finish_and_message", "all_actions"]
SecurityAnalyzerType = Literal["llm", "none"]

class VerificationSettings(SurgeBaseModel):
    """Reflector and iterative-refinement settings for the agent."""
    reflector_enabled: bool = Field(
        default=False,
        description="Enable evaluation for the agent.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(label="Enable reflector", prominence=SettingProminence.CRITICAL,).model_dump()
        },
    )
    reflector_mode: ReflectorMode = Field(
        default="finish_and_message",
        description="When reflector evaluation should run.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Reflector mode",
                prominence=SettingProminence.MINOR,
                depends_on=("reflector_enabled",),
            ).model_dump()
        },
    )
    enable_iterative_refinement: bool = Field(
        default=False,
        description=(
            "Automatically retry tasks when reflector scores fall below the threshold."
        ),
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Enable iterative refinement",
                depends_on=("reflector_enabled",),
            ).model_dump()
        },
    )
    reflector_threshold: float = Field(
        default=0.6,
        ge=0.0,
        le=1.0,
        description="Reflector success threshold used for iterative refinement.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Reflector threshold",
                prominence=SettingProminence.MINOR,
                depends_on=("reflector_enabled", "enable_iterative_refinement"),
            ).model_dump()
        },
    )
    max_refinement_iterations: int = Field(
        default=3,
        ge=1,
        description="Maximum number of refinement attempts after reflector feedback.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Max refinement iterations",
                prominence=SettingProminence.MINOR,
                depends_on=("reflector_enabled", "enable_iterative_refinement"),
            ).model_dump()
        },
    )
    reflector_server_url: str | None = Field(
        default=None,
        description="Override the reflector service URL. When None, the Reflector default is used.",
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Reflector server URL",
                prominence=SettingProminence.MINOR,
                depends_on=("reflector_enabled",),
            ).model_dump()
        },
    )
    reflector_model_name: str | None = Field(
        default=None,
        description=(
            "Override the reflector model name. "
            "When None, the Reflector default is used."
        ),
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Reflector model name",
                prominence=SettingProminence.MINOR,
                depends_on=("reflector_enabled",),
            ).model_dump()
        },
    )

    confirmation_mode: bool = Field(
        default=False,
        description="Require user confirmation before executing risky actions.",
        deprecated=(
            "Deprecated in 1.17.0; use ConversationSettings.confirmation_mode "
            "instead. Will be removed in 1.22.0."
        ),
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Confirmation mode",
                prominence=SettingProminence.MAJOR,
            ).model_dump()
        },
    )
    security_analyzer: SecurityAnalyzerType | None = Field(
        default=None,
        description=("Security analyzer that evaluates actions before execution."),
        deprecated=(
            "Deprecated in 1.17.0; use ConversationSettings.security_analyzer "
            "instead. Will be removed in 1.22.0."
        ),
        json_schema_extra={
            SETTINGS_METADATA_KEY: SettingsFieldMetadata(
                label="Security analyzer",
                prominence=SettingProminence.MAJOR,
                depends_on=("confirmation_mode",),
            ).model_dump()
        },
    )

    @field_validator("confirmation_mode", mode="before")
    @classmethod
    def _warn_confirmation_mode(cls, v: Any) -> Any:
        if v:
            warn_deprecated(
                "VerificationSettings.confirmation_mode",
                deprecated_in="1.17.0",
                removed_in="1.22.0",
                details="Use ConversationSettings.confirmation_mode instead.",
            )
        return v

    @field_validator("security_analyzer", mode="before")
    @classmethod
    def _warn_security_analyzer(cls, v: Any) -> Any:
        if v is not None:
            warn_deprecated(
                "VerificationSettings.security_analyzer",
                deprecated_in="1.17.0",
                removed_in="1.22.0",
                details="Use ConversationSettings.security_analyzer instead.",
            )
        return v

def settings_section_metadata(field: FieldInfo) -> SettingsSectionMetadata | None:
    extra = field.json_schema_extra
    if not isinstance(extra, dict):
        return None

    metadata = extra.get(SETTINGS_SECTION_METADATA_KEY)
    if metadata is None:
        return None
    return SettingsSectionMetadata.model_validate(metadata)


def settings_metadata(field: FieldInfo) -> SettingsFieldMetadata | None:
    extra = field.json_schema_extra
    if not isinstance(extra, dict):
        return None

    metadata = extra.get(SETTINGS_METADATA_KEY)
    if metadata is None:
        return None
    return SettingsFieldMetadata.model_validate(metadata)

_GENERAL_SECTION_KEY = "general"
_GENERAL_SECTION_LABEL = "General"
_GENERAL_SECTION_METADATA = SettingsSectionMetadata(
    key=_GENERAL_SECTION_KEY,
    label=_GENERAL_SECTION_LABEL,
)

def export_settings_schema(model: type[SurgeBaseModel]) -> SettingsSchema:
    sections: list[SettingsSectionSchema] = []
    sections_by_key: dict[str, SettingsSectionSchema] = {}

    def ensure_section(metadata: SettingsSectionMetadata) -> SettingsSectionSchema:
        section = sections_by_key.get(metadata.key)
        if section is not None:
            return section
        section = SettingsSectionSchema(
            key=metadata.key,
            label=metadata.label or _humanize_name(metadata.key),
            fields=[],
        )
        sections_by_key[metadata.key] = section
        sections.append(section)
        return section

    for field_name, field in model.model_fields.items():
        explicit_section_metadata = settings_section_metadata(field)
        section_metadata = explicit_section_metadata or _GENERAL_SECTION_METADATA
        nested_model = _nested_model_type(field.annotation)

        if explicit_section_metadata is not None and nested_model is not None:
            section_default = field.get_default(call_default_factory=True)
            section = ensure_section(explicit_section_metadata)
            for nested_key, nested_field in nested_model.model_fields.items():
                if nested_field.exclude:
                    continue
                metadata = settings_metadata(nested_field)
                default_value = None
                if isinstance(section_default, SurgeBaseModel):
                    default_value = getattr(section_default, nested_key)
                section.fields.append(
                    SettingsFieldSchema(
                        key=f"{explicit_section_metadata.key}.{nested_key}",
                        label=(
                            metadata.label
                            if metadata is not None and metadata.label is not None
                            else _humanize_name(nested_key)
                        ),
                        description=nested_field.description,
                        section=section.key,
                        section_label=section.label,
                        value_type=_infer_value_type(nested_field.annotation),
                        default=_normalize_default(default_value),
                        prominence=(
                            metadata.prominence
                            if metadata is not None
                            else SettingProminence.MINOR
                        ),
                        depends_on=[
                            f"{explicit_section_metadata.key}.{dependency}"
                            for dependency in (
                                metadata.depends_on if metadata is not None else ()
                            )
                        ],
                        secret=_contains_secret(nested_field.annotation),
                        choices=_extract_choices(nested_field.annotation),
                    )
                )
            continue

        metadata = settings_metadata(field)
        if metadata is None:
            continue

        default_value = field.get_default(call_default_factory=True)
        section = ensure_section(section_metadata)
        section.fields.append(
            SettingsFieldSchema(
                key=field_name,
                label=(
                    metadata.label
                    if metadata.label is not None
                    else _humanize_name(field_name)
                ),
                description=field.description,
                section=section.key,
                section_label=section.label,
                value_type=_infer_value_type(field.annotation),
                default=_normalize_default(default_value),
                prominence=metadata.prominence,
                depends_on=list(metadata.depends_on),
                secret=_contains_secret(field.annotation),
                choices=_extract_choices(field.annotation),
            )
        )

    return SettingsSchema(model_name=model.__name__, sections=sections)

def _nested_model_type(annotation: Any) -> type[SurgeBaseModel] | None:
    candidates = _annotation_options(annotation)
    if len(candidates) != 1:
        return None

    candidate = candidates[0]
    if isinstance(candidate, type) and issubclass(candidate, SurgeBaseModel):
        return candidate
    return None


def _annotation_options(annotation: Any) -> tuple[Any, ...]:
    origin = get_origin(annotation)
    if origin is None or origin is Literal:
        return (annotation,)
    if origin in (list, tuple, set, frozenset, dict):
        return (annotation,)

    options: list[Any] = []
    for arg in get_args(annotation):
        if arg is type(None):
            continue
        options.extend(_annotation_options(arg))
    return tuple(options) or (annotation,)

def _contains_secret(annotation: Any) -> bool:
    return any(option is SecretStr for option in _annotation_options(annotation))

def _infer_value_type(annotation: Any) -> SettingsValueType:
    choices = _choice_values(annotation)
    if choices:
        return _value_type_for_values(choices)

    options = _annotation_options(annotation)
    if all(_is_stringish(option) for option in options):
        return "string"
    if all(option is bool for option in options):
        return "boolean"
    if all(option is int for option in options):
        return "integer"
    if all(option in (int, float) for option in options):
        return "number"
    if all(_is_array_annotation(option) for option in options):
        return "array"
    if all(_is_object_annotation(option) for option in options):
        return "object"
    return "string"


def _is_stringish(annotation: Any) -> bool:
    return annotation in (str, SecretStr, Path)

def _is_array_annotation(annotation: Any) -> bool:
    return get_origin(annotation) in (list, tuple, set, frozenset)

def _is_object_annotation(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is dict:
        return True
    return isinstance(annotation, type) and issubclass(annotation, SurgeBaseModel)

def _choice_values(annotation: Any) -> list[SettingsChoiceValue]:
    inner = _annotation_options(annotation)
    if len(inner) != 1:
        return []

    candidate = inner[0]
    origin = get_origin(candidate)
    if origin is Literal:
        return [
            value
            for value in get_args(candidate)
            if isinstance(value, (bool, int, float, str))
        ]
    if isinstance(candidate, type) and issubclass(candidate, Enum):
        return [
            member.value
            for member in candidate
            if isinstance(member.value, (bool, int, float, str))
        ]
    return []

def _value_type_for_values(values: list[SettingsChoiceValue]) -> SettingsValueType:
    if all(isinstance(value, bool) for value in values):
        return "boolean"
    if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return "integer"
    if all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in values
    ):
        return "number"
    return "string"

def _extract_choices(annotation: Any) -> list[SettingsChoice]:
    inner = _annotation_options(annotation)
    if len(inner) != 1:
        return []

    candidate = inner[0]
    origin = get_origin(candidate)
    if origin is Literal:
        return [
            SettingsChoice(value=value, label=str(value))
            for value in get_args(candidate)
            if isinstance(value, (bool, int, float, str))
        ]
    if isinstance(candidate, type) and issubclass(candidate, Enum):
        return [
            SettingsChoice(
                value=member.value,
                label=_humanize_name(member.name),
            )
            for member in candidate
            if isinstance(member.value, (bool, int, float, str))
        ]
    return []

def _normalize_default(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return None
    if isinstance(value, Enum):
        return _normalize_default(value.value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, SurgeBaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _normalize_default(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_normalize_default(item) for item in value]
    if isinstance(value, (bool, int, float, str)) or value is None:
        return value
    return None

def _humanize_name(name: str) -> str:
    acronyms = {"api", "aws", "id", "llm", "url"}
    words = []
    for part in name.split("_"):
        words.append(part.upper() if part in acronyms else part.capitalize())
    return " ".join(words)
