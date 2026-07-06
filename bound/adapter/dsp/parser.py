# bound.adapter.dsp.parser
## @lineage: bound.adapter.opt.parser
import re
from typing import Any, Protocol
import json_repair
import regex

from arch.xor.manifold.sign.signature import Signature
from bound.adapter.dsp.exception import AdapterParseError
from xphi.xor.opt.formatter import parse_value
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class OutputParser(Protocol):
    """Adapter의 텍스트 파싱을 전담하는 프로토콜(인터페이스)"""
    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        ...

class MarkdownRegexParser:
    """기존 ChatAdapter의 마크다운 형식([[ ## field ## ]]) 응답을 파싱하는 클래스입"""
    def __init__(self):
        self.field_header_pattern = re.compile(r"\[\[ ## (\w+) ## \]\]")

    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        sections = [(None, [])]

        for line in completion.splitlines():
            match = self.field_header_pattern.match(line.strip())
            if match:
                header = match.group(1)
                remaining_content = line[match.end() :].strip()
                sections.append((header, [remaining_content] if remaining_content else []))
            else:
                sections[-1][1].append(line)

        sections = [(k, "\n".join(v).strip()) for k, v in sections]

        fields = {}
        for k, v in sections:
            if (k not in fields) and (k in signature.output_fields):
                try:
                    fields[k] = parse_value(v, signature.output_fields[k].annotation)
                except Exception as e:
                    raise AdapterParseError(
                        adapter_name=type(self).__name__,
                        signature=signature,
                        lm_response=completion,
                        message=f"Failed to parse field {k} with value {v} from the LM response. Error message: {e}",
                    )

        if fields.keys() != signature.output_fields.keys():
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                parsed_result=fields,
                message="Parsed fields do not match the expected output fields in the signature."
            )

        return fields


class JSONRepairParser:
    """부분적으로 깨지거나 마크다운 블록(```json)에 감싸진 데이터도 복구하여 추출"""
    def parse(self, signature: type[Signature], completion: str) -> dict[str, Any]:
        if not completion or not completion.strip():
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                message="LM returned an empty response.",
            )

        fields = None
        
        ## 1차 시도: 전체 문자열 직접 파싱
        try:
            fields = json_repair.loads(completion)
        except Exception:
            pass

        ## 2차 시도: 정규식을 통한 JSON 블록 추출 파싱
        if not isinstance(fields, dict):
            pattern = r"\{(?:[^{}]|(?R))*\}"
            match = regex.search(pattern, completion, regex.DOTALL)
            if match:
                try:
                    fields = json_repair.loads(match.group(0))
                except Exception:
                    pass

        ## 파싱 완전 실패 시
        if not isinstance(fields, dict):
            raise AdapterParseError(
                adapter_name=type(self).__name__,
                signature=signature,
                lm_response=completion,
                message="LM response cannot be parsed as a JSON object.",
            )

        filtered_fields = {}
        for k, expected_field in signature.output_fields.items():
            if k in fields:
                try:
                    filtered_fields[k] = parse_value(fields[k], expected_field.annotation)
                except Exception as e:
                    log.debug(f"JSONRepairParser failed to cast field {k}: {e}")
                    # 캐스팅 실패 시 데이터 손실을 막기 위해 원본 할당
                    filtered_fields[k] = fields[k]
            else:
                log.warning(f"JSONRepairParser: Missing field '{k}' in LM response. Filled with None.")
                filtered_fields[k] = None

        return filtered_fields