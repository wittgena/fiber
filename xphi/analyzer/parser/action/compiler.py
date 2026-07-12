# xphi.analyzer.parser.action.compiler
import json
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type
from pydantic import Field
from watcher.plane.emitter import get_emitter

log = get_emitter("action.compiler")

DEFAULT = {
    "finish": {
        "description": "Signal that the assigned task is complete or requires no further action.",
        "fields": {"summary": {"type": "string", "optional": True, "description": "Optional summary of the final outcome."}},
        "hide_observation": True
    },
    "lang": {
        "description": "Communicate directly with the user via natural language.",
        "fields": {"message": {"type": "string", "description": "Message to send."}, "intent": {"type": "enum", "enum_name": "MessageIntent", "values": ["report", "clarify", "summary"], "default": "report", "description": "Control flow intent."}},
        "hide_observation": True
    },
    "bridge": {
        "description": "Use this bridge when stuck, confused, or requiring structural shift.",
        "fields": {"thought": {"type": "string", "description": "The detailed thought, strategy, or plan to log.info."}, "intent": {"type": "enum", "enum_name": "Topolog.infoicalIntent", "values": ["replan", "escalate", "optimize_prompt", "delegate_task", "debug"], "default": "replan", "description": "The primary routing intent."}, "target_aspects": {"type": "list", "item_type": "string", "optional": True, "description": "Optional. Variables needed by next topolog.infoy."}, "tension_level": {"type": "integer", "optional": True, "ge": 1, "le": 5, "description": "Optional. Indicated cognitive tension."}},
        "hide_observation": True
    },
    "think": {
        "description": "Agent's internal reasoning or thinking process.",
        "fields": {
            "thought": {
                "type": "string", 
                "description": "The detailed thought or reasoning process."
            }
        },
        "hide_observation": True
    },
    "signal": {
        "description": "Emit a system signal or control event.",
        "fields": {
            "event_name": {
                "type": "string", 
                "description": "The name or type of the signal to emit."
            }
        },
        "hide_observation": True
    }
}

class ActionSchemaCompiler:
    """@desc: Parses pure JSON schema definitions into Python runtime objects (Pydantic Fields, Types, Enums)"""
    
    # Base type mapping table
    TYPE_MAP: Dict[str, Type] = {
        "string": str,
        "integer": int,
        "float": float,
        "boolean": bool,
    }

    # Global cache to reuse dynamically created Enums (optional)
    _ENUM_CACHE: Dict[str, Type[Enum]] = {}

    @classmethod
    def compile_routes(cls, raw_json: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Compiles entire JSON routes into Python runtime configurations."""
        compiled_routes = {}
        
        for route_name, config in raw_json.items():
            try:
                compiled_routes[route_name] = {
                    "description": config.get("description", ""),
                    "fields": cls.compile_fields(config.get("fields", {})),
                    "obs_fields": cls.compile_fields(config.get("obs_fields", {})),
                    "hide_observation": config.get("hide_observation", False)
                }
                log.debug(f"[Compiler] Successfully compiled route: {route_name}")
            except Exception as e:
                log.error(f"[Compiler] Failed to compile route '{route_name}': {e}")
                
        return compiled_routes

    @classmethod
    def compile_fields(cls, fields_json: Dict[str, Any]) -> Dict[str, Tuple[Type, Any]]:
        """Converts a set of field attributes into Pydantic Tuple (Type, Field object) format."""
        compiled = {}
        for field_name, field_def in fields_json.items():
            py_type = cls._resolve_type(field_name, field_def)
            field_obj = cls._resolve_field_obj(field_def, py_type)
            compiled[field_name] = (py_type, field_obj)
        return compiled

    @classmethod
    def _resolve_type(cls, field_name: str, field_def: Dict[str, Any]) -> Type:
        """Resolves JSON type directives ('string', 'enum', 'list') to actual Python Type objects."""
        type_str = field_def.get("type", "string")

        # 1. Dynamic Enum creation
        if type_str == "enum":
            enum_name = field_def.get("enum_name", f"{field_name.capitalize()}Enum")
            if enum_name in cls._ENUM_CACHE:
                base_type = cls._ENUM_CACHE[enum_name]
            else:
                values = field_def.get("values", [])
                # Mapping format: {"REPORT": "report", "CLARIFY": "clarify"}
                base_type = Enum(enum_name, {str(v).upper(): v for v in values})
                cls._ENUM_CACHE[enum_name] = base_type

        # 2. List type hydration
        elif type_str == "list":
            item_type_str = field_def.get("item_type", "string")
            base_type = List[cls.TYPE_MAP.get(item_type_str, str)]

        # 3. Base scalar types
        else:
            base_type = cls.TYPE_MAP.get(type_str, str)

        # Apply Optional wrapping
        if field_def.get("optional", False):
            return Optional[base_type]
            
        return base_type

    @classmethod
    def _resolve_field_obj(cls, field_def: Dict[str, Any], py_type: Type) -> Any:
        """Creates a pydantic.Field instance based on JSON constraints."""
        kwargs = {}
        
        if "description" in field_def:
            kwargs["description"] = field_def["description"]
            
        # Default value handling
        if "default" in field_def:
            # Pydantic automatically casts raw text (e.g. "report") to Enum members 
            # if the target type is an Enum, so passing the raw value is safe.
            kwargs["default"] = field_def["default"]
        elif field_def.get("optional", False):
            kwargs["default"] = None

        # Validation constraints mapping
        constraints = ["ge", "le", "gt", "lt", "min_length", "max_length"]
        for constraint in constraints:
            if constraint in field_def:
                kwargs[constraint] = field_def[constraint]

        return Field(**kwargs)

if __name__ == "__main__":
    log.info("--- Compiling DEFAULT Schema ---")
    compiled = ActionSchemaCompiler.compile_routes(DEFAULT)
    
    for route, config in compiled.items():
        log.info(f"\nRoute: {route}")
        log.info(f"  Description: {config['description']}")
        log.info(f"  Hide Observation: {config['hide_observation']}")
        log.info("  Fields:")
        for field_name, (field_type, field_obj) in config['fields'].items():
            default_val = field_obj.default if hasattr(field_obj, 'default') else 'N/A'
            desc_val = field_obj.description if hasattr(field_obj, 'description') else 'None'
            log.info(f"    - {field_name}: {field_type} (default={default_val}, desc='{desc_val}')")