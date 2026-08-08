# phase.dphi.filter
## @lineage: kernel.dphi.filter
import json
from typing import Dict, Any, List, Tuple

class WasmBinaryFilter:
    """@desc: dphi.wasm 커널로 진입하려는 모든 WASM 바이너리를 분해하고 검사하는 필터"""
    ALLOWED_IMPORTS = {
        "env": {"consume_fuel", "crypto_sha256", "crypto_ed25519_verify", "ledger_read_state"}
    }
    REQUIRED_EXPORTS = {
        "validate_intent", 
        "execute_transition", 
        "verify_parity"
    }

    def __init__(self):
        self.errors: List[str] = []
        self.found_imports: List[Dict[str, str]] = []
        self.found_exports: List[str] = []

    @staticmethod
    def _read_leb128_u32(data: bytes, offset: int) -> Tuple[int, int]:
        """WASM 표준 가변 길이 정수 인코딩(LEB128) 디코더"""
        result = 0
        shift = 0
        while True:
            if offset >= len(data):
                raise ValueError("Unexpected end of binary during LEB128 parsing")
            byte = data[offset]
            offset += 1
            result |= (byte & 0x7f) << shift
            if (byte & 0x80) == 0:
                break
            shift += 7
        return result, offset

    @staticmethod
    def _read_string(data: bytes, offset: int) -> Tuple[str, int]:
        """WASM 표준 문자열 디코더 (LEB128 길이 + UTF-8)"""
        length, offset = WasmBinaryFilter._read_leb128_u32(data, offset)
        if offset + length > len(data):
            raise ValueError("String length exceeds binary size")
        string_val = data[offset:offset+length].decode('utf-8')
        return string_val, offset + length

    def _parse_import_section(self, data: bytes, offset: int, end_offset: int) -> None:
        """Type 2: Import Section 파싱 - 호스트에 어떤 권한을 요구하는가?"""
        count, offset = self._read_leb128_u32(data, offset)
        for _ in range(count):
            module_name, offset = self._read_string(data, offset)
            field_name, offset = self._read_string(data, offset)
            import_kind = data[offset]
            offset += 1
            
            # Import Kind: 0(Function), 1(Table), 2(Memory), 3(Global)
            if import_kind == 0x00:
                _, offset = self._read_leb128_u32(data, offset) # type_idx 건너뛰기
            elif import_kind == 0x01:
                offset += 2 # table type 건너뛰기
            elif import_kind == 0x02:
                flags = data[offset]
                offset += 1
                _, offset = self._read_leb128_u32(data, offset) # initial
                if flags == 1:
                    _, offset = self._read_leb128_u32(data, offset) # maximum
            elif import_kind == 0x03:
                offset += 2 # global type 건너뛰기

            self.found_imports.append({"module": module_name, "field": field_name})
            
            # 🛡️ [보안 검증] 화이트리스트 대조
            if module_name not in self.ALLOWED_IMPORTS or field_name not in self.ALLOWED_IMPORTS.get(module_name, set()):
                self.errors.append(
                    f"SECURITY BREACH: Unauthorized Import requested -> '{module_name}.{field_name}'. "
                    f"Malicious syscalls (like fd_write, socket) are strictly blocked."
                )

    def _parse_export_section(self, data: bytes, offset: int, end_offset: int) -> None:
        """Type 7: Export Section 파싱 - 시스템에 어떤 함수를 제공하는가?"""
        count, offset = self._read_leb128_u32(data, offset)
        for _ in range(count):
            export_name, offset = self._read_string(data, offset)
            export_kind = data[offset]
            offset += 1
            _, offset = self._read_leb128_u32(data, offset) # idx 건너뛰기
            
            if export_kind == 0x00: # Function Export만 추적
                self.found_exports.append(export_name)

    def audit_binary(self, wasm_bytes: bytes) -> Dict[str, Any]:
        """
        @desc: 제출된 WASM 바이너리를 분해하고, 보안 및 구조적 정합성을 감사(Audit)합니다.
        """
        self.errors.clear()
        self.found_imports.clear()
        self.found_exports.clear()

        if not wasm_bytes.startswith(b'\x00asm'):
            return {"success": False, "error": "Invalid Magic Number. Not a valid WASM binary."}
        if wasm_bytes[4:8] != b'\x01\x00\x00\x00':
            return {"success": False, "error": "Unsupported WASM version. Only version 1 is supported."}

        offset = 8
        length = len(wasm_bytes)

        try:
            while offset < length:
                section_id = wasm_bytes[offset]
                offset += 1
                section_size, offset = self._read_leb128_u32(wasm_bytes, offset)
                section_end = offset + section_size

                if section_id == 2:   # Import Section
                    self._parse_import_section(wasm_bytes, offset, section_end)
                elif section_id == 7: # Export Section
                    self._parse_export_section(wasm_bytes, offset, section_end)
                
                offset = section_end

        except Exception as e:
            self.errors.append(f"MALFORMED BINARY: Failed to parse WASM structural sections -> {str(e)}")

        missing_exports = self.REQUIRED_EXPORTS - set(self.found_exports)
        if missing_exports:
            self.errors.append(
                f"STRUCTURAL FAILURE: Contract must export mandatory ABI functions: {missing_exports}"
            )

        is_safe = len(self.errors) == 0
        return {
            "success": is_safe,
            "validation_report": {
                "errors": self.errors,
                "detected_imports": self.found_imports,
                "detected_exports": self.found_exports
            }
        }