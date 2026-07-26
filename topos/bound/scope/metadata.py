# topos.bound.scope.metadata
## @lineage: ops.scope.topos.metadata
## @lineage: void.extime.web.metadata
from typing import TYPE_CHECKING, Any, Literal
from pydantic import Field

from arch.topos.bound.surge.disc import SurgeBaseModel
from watcher.xe.residue.convset import SettingProminence

SettingsValueType = Literal[
    "string",
    "integer",
    "number",
    "boolean",
    "array",
    "object",
]

SettingsChoiceValue = bool | int | float | str

class SettingsChoice(SurgeBaseModel):
    value: SettingsChoiceValue
    label: str

class SettingsFieldSchema(SurgeBaseModel):
    key: str
    label: str
    description: str | None = None
    section: str
    section_label: str
    value_type: SettingsValueType
    default: Any = None
    prominence: SettingProminence = SettingProminence.MINOR
    depends_on: list[str] = Field(default_factory=list)
    secret: bool = False
    choices: list[SettingsChoice] = Field(default_factory=list)

class SettingsSectionSchema(SurgeBaseModel):
    key: str
    label: str
    fields: list[SettingsFieldSchema]

class SettingsSchema(SurgeBaseModel):
    model_name: str
    sections: list[SettingsSectionSchema]