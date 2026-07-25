# atoa.disc.schema.metadata
## @lineage: atoa.gov.disc.schema.metadata
## @lineage: agent.atoa.disc.schema.metadata
## @lineage: atoa.agent.disc.schema.metadata
## @lineage: agent.disc.schema.metadata
from typing import TYPE_CHECKING, Any, Literal
from pydantic import Field

from eco.fiber.residue.convset import SettingProminence
from arch.topos.bound.surge.disc import SurgeBaseModel

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