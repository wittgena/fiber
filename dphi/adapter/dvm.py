# fiber.dphi.adapter.dvm
import time
import json
import hashlib
from typing import Dict, Any

from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("adapter.dvm")

class DvmAdapter:
    """
    [Multi-VM Facade]
    - EVM과 CosmWasm의 파편화된 규격(ABI 인코딩, JSON Schema 등)을 DPhi Broker 및 DVM 엔진이 실행할 수 있는 표준 페이로드로 번역
    """
    @staticmethod
    def build_erc20_transfer_calldata(to_address: str, amount_wei: int) -> str:
        """ERC20 transfer(address,uint256) Calldata 생성"""
        method_id = "a9059cbb"
        to_padded = to_address.replace("0x", "").zfill(64).lower()
        amount_padded = hex(amount_wei).replace("0x", "").zfill(64)
        return f"0x{method_id}{to_padded}{amount_padded}"

    @staticmethod
    def build_erc20_transfer_from_calldata(from_address: str, to_address: str, amount_wei: int) -> str:
        """ERC20 transferFrom(address,address,uint256) Calldata 생성"""
        method_id = "23b872dd"
        from_padded = from_address.replace("0x", "").zfill(64).lower()
        to_padded = to_address.replace("0x", "").zfill(64).lower()
        amount_padded = hex(amount_wei).replace("0x", "").zfill(64)
        return f"0x{method_id}{from_padded}{to_padded}{amount_padded}"

    @staticmethod
    def build_claim_calldata(tenant_address: str, net_debt_usdc: float) -> str:
        """@desc: 정산(Settlement) 시 사용되는 claim(address,uint256,bytes) Calldata 생성"""
        method_signature = "claim(address,uint256,bytes)".encode('utf-8')
        method_id = hashlib.sha3_256(method_signature).hexdigest()[:8]
        tenant_padded = tenant_address.replace("0x", "").zfill(64).lower()
        amount_padded = hex(int(net_debt_usdc)).replace("0x", "").zfill(64)
        
        return f"0x{method_id}{tenant_padded}{amount_padded}"

    @staticmethod
    def build_evm_account_data(balance_wei: int, nonce: int = 0, code_hex: str = "0x") -> Dict[str, Any]:
        """REVM에 주입할 표준 EVM 계정 상태(State Snapshot) 포맷팅"""
        return {
            "balance": hex(balance_wei), 
            "nonce": nonce, 
            "code": code_hex
        }

    """CosmWasm 통제 구역: JSON Schema 페이로드 생성"""
    @staticmethod
    def build_cw20_transfer_payload(
        target_wasm_file: str,
        sender_address: str, 
        recipient_address: str, 
        amount: int,
        cycle: int,
        current_balance: int
    ) -> Dict[str, Any]:
        """CosmWasm (CW20) 특화 JSON 메시지를 DVM 엔진 표준으로 포장"""
        cw20_balance_key = f"balance_{sender_address}"
        return {
            "vm_target": "COSMWASM_EXTERNAL",
            "target_wasm_file": target_wasm_file,
            "env": {
                "block": {
                    "height": 1000 + cycle,
                    "time": str(int(time.time() * 1_000_000_000)), # 나노초 규격
                    "chain_id": "akash-local"
                }
            },
            "info": {
                "sender": sender_address, 
                "funds": []
            },
            "msg": {
                "transfer": {
                    "recipient": recipient_address, 
                    "amount": str(amount)
                }
            },
            "state_snapshot": {
                cw20_balance_key: json.dumps(current_balance)
            }
        }
    
    @staticmethod
    def build_dvm_payload(
        target_address: str, 
        calldata: str, 
        state_snapshot: Dict[str, Any], 
        vm_target: str = "EVM"
    ) -> Dict[str, Any]:
        """EVM 등 Calldata 기반의 가상머신을 위한 최종 DVM Broker 페이로드 조립"""
        return {
            "vm_target": vm_target,
            "target_address": target_address,
            "calldata": calldata,
            "state_snapshot": state_snapshot
        }