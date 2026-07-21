# bound.bridge.rule
## @lineage: bound.transport.bridge.rule
## @lineage: bound.bridge.transport.rule
import json
from typing import Any, Dict, List, Union
from jsonschema import ValidationError, validate
from typing import Optional
from bound.resolver.model.config.resolver import config
from bound.resolver.model.config.constants import DEFAULT_MAX_RECURSE_DEPTH
from bound.exception import JSONSchemaValidationError, APIResponseValidationError

class Rules:
    def __init__(self) -> None:
        pass

    @staticmethod
    def has_pre_call_rules() -> bool:
        """Check if any pre-call rules are configured"""
        return len(config.pre_call_rules) > 0

    def pre_call_rules(self, input: str, model: str):
        for rule in config.pre_call_rules:
            if callable(rule):
                decision = rule(input)
                if decision is False:
                    raise APIResponseValidationError(message="LLM Response failed post-call-rule check", llm_provider="", model=model)  # type: ignore
        return True

    def post_call_rules(self, input: Optional[str], model: str) -> bool:
        if input is None:
            return True
        for rule in config.post_call_rules:
            if callable(rule):
                decision = rule(input)
                if isinstance(decision, bool):
                    if decision is False:
                        raise APIResponseValidationError(message="LLM Response failed post-call-rule check", llm_provider="", model=model)  # type: ignore
                elif isinstance(decision, dict):
                    decision_val = decision.get("decision", True)
                    decision_message = decision.get(
                        "message", "LLM Response failed post-call-rule check"
                    )
                    if decision_val is False:
                        raise APIResponseValidationError(message=decision_message, llm_provider="", model=model)  # type: ignore
        return True

def normalize_json_schema_types(
    schema: Union[Dict[str, Any], List[Any], Any],
    depth: int = 0,
    max_depth: int = DEFAULT_MAX_RECURSE_DEPTH,
) -> Union[Dict[str, Any], List[Any], Any]:
    # Prevent infinite recursion
    if depth >= max_depth:
        return schema

    if not isinstance(schema, (dict, list)):
        return schema

    # Type mapping from uppercase to lowercase
    type_mapping = {
        "BOOLEAN": "boolean",
        "STRING": "string",
        "ARRAY": "array",
        "OBJECT": "object",
        "NUMBER": "number",
        "INTEGER": "integer",
        "NULL": "null",
    }

    if isinstance(schema, list):
        return [
            normalize_json_schema_types(item, depth + 1, max_depth) for item in schema
        ]

    if isinstance(schema, dict):
        normalized_schema: Dict[str, Any] = {}

        for key, value in schema.items():
            if key == "type" and isinstance(value, str) and value in type_mapping:
                normalized_schema[key] = type_mapping[value]
            elif key == "properties" and isinstance(value, dict):
                # Recursively normalize properties
                normalized_schema[key] = {
                    prop_key: normalize_json_schema_types(
                        prop_value, depth + 1, max_depth
                    )
                    for prop_key, prop_value in value.items()
                }
            elif key == "items" and isinstance(value, (dict, list)):
                # Recursively normalize array items
                normalized_schema[key] = normalize_json_schema_types(
                    value, depth + 1, max_depth
                )
            elif isinstance(value, (dict, list)):
                # Recursively normalize any nested dict or list
                normalized_schema[key] = normalize_json_schema_types(
                    value, depth + 1, max_depth
                )
            else:
                normalized_schema[key] = value
        return normalized_schema
    return schema

def normalize_tool_schema(tool: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(tool, dict):
        return tool

    normalized_tool = tool.copy()
    if "function" in tool and isinstance(tool["function"], dict):
        normalized_tool["function"] = tool["function"].copy()
        if "parameters" in tool["function"]:
            normalized_tool["function"]["parameters"] = normalize_json_schema_types(
                tool["function"]["parameters"]
            )

    return normalized_tool


def validate_schema(schema: dict, response: str):
    try:
        response_dict = json.loads(response)
    except json.JSONDecodeError:
        raise JSONSchemaValidationError(
            model="", llm_provider="", raw_response=response, schema=json.dumps(schema)
        )

    try:
        validate(response_dict, schema=schema)
    except ValidationError:
        raise JSONSchemaValidationError(
            model="", llm_provider="", raw_response=response, schema=json.dumps(schema)
        )
