# bound.adapter.call.residue.convset
## @lineage: agent.context.conv.metadata
from __future__ import annotations
from enum import Enum
from pydantic import BaseModel
from pydantic.config import JsonDict

SETTINGS_METADATA_KEY = "surgent_settings"
SETTINGS_SECTION_METADATA_KEY = "surgent_settings_section"

class SettingProminence(str, Enum):
    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"

class SettingsSectionMetadata(BaseModel):
    key: str
    label: str | None = None

class SettingsFieldMetadata(BaseModel):
    label: str | None = None
    prominence: SettingProminence = SettingProminence.MINOR
    depends_on: tuple[str, ...] = ()

def field_meta(
    prominence: SettingProminence = SettingProminence.MINOR,
    *,
    label: str | None = None,
    depends_on: tuple[str, ...] = (),
) -> JsonDict:
    metadata: JsonDict = SettingsFieldMetadata(label=label, prominence=prominence, depends_on=depends_on).model_dump(mode="json")
    return {SETTINGS_METADATA_KEY: metadata}
