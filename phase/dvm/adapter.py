# phase.dvm.adapter
## @lineage: bound.adapter.dvm
import os
from typing import Dict, Any, Optional
from watcher.plane.emitter import get_emitter

log = get_emitter("adapter.dvm")

class DvmAdapter:
    @staticmethod
    def build_evm_account_data(
        balance_wei: int, 
        nonce: int = 0, 
        code_hex: Optional[str] = None, 
        storage: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        account_data: Dict[str, Any] = {
            "balance": hex(balance_wei),
            "nonce": nonce
        }
        if code_hex:
            account_data["code"] = code_hex if code_hex.startswith("0x") else f"0x{code_hex}"
        if storage:
            account_data["storage"] = storage
        return account_data

    @staticmethod
    def build_dvm_payload(
        target_address: str,
        calldata: str,
        state_snapshot: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        @desc: 브로커의 execute(code=...) 파라미터로 들어갈 순수 코드(payload)만 조립
        """
        return {
            "vm_target": "EVM",
            "target_address": target_address.lower(),
            "calldata": calldata,
            "state_snapshot": state_snapshot
        }

    @staticmethod
    def build_erc20_transfer_from_calldata(from_address: str, to_address: str, amount_wei: int) -> str:
        clean_from = from_address.lower().replace("0x", "").rjust(64, "0")
        clean_to = to_address.lower().replace("0x", "").rjust(64, "0")
        clean_amount = hex(amount_wei).replace("0x", "").rjust(64, "0")
        return f"0x23b872dd{clean_from}{clean_to}{clean_amount}"

    @staticmethod
    def build_erc20_transfer_calldata(to_address: str, amount_wei: int) -> str:
        clean_to = to_address.lower().replace("0x", "").rjust(64, "0")
        clean_amount = hex(amount_wei).replace("0x", "").rjust(64, "0")
        return f"0xa9059cbb{clean_to}{clean_amount}"