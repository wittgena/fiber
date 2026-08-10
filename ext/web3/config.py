# ext.web3.config
## @lineage: ext.evm.config
## @lineage: receptor.ext.evm.config
## @lineage: phase.epoch.config.evm
import time
import random
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from phase.epoch.config.dphi import mock_env

@dataclass
class EvmIntent:
    target: str
    caller: str
    calldata: str
    scenario_type: str
    value: int = 0
    storage_slots: List[str] = field(default_factory=list)
    requires_access_list: bool = False
    allowance_slot_index: Optional[int] = None
    
    def to_workflow_dict(self) -> dict:
        data = asdict(self)
        data.pop("target")
        return {k: v for k, v in data.items() if v is not None}

class EvmBuilder:
    """EVM 관련 인텐트, 상태, 블록 컨텍스트 구성을 전담하는 빌더 클래스"""
    
    @staticmethod
    def build_user_intent(
        scenario_type: str = "ERC20_TRANSFER",
        should_revert: bool = False
    ) -> EvmIntent:
        caller = mock_env.agents.alpha.evm_address
        value = 0
        requires_access_list = False
        
        if scenario_type == "ERC20_TRANSFER":
            target = mock_env.contracts.target_erc20
            calldata = "0xa9059cbb" + "000000000000000000000000" + mock_env.agents.beta.evm_address[2:] + "0000000000000000000000000000000000000000000000000de0b6b3a7640000"
            storage_slots = ["0x0", "0x1", "0x2"]
            requires_access_list = True
            
        elif scenario_type == "ERC4337_HANDLE_OPS":
            target = "0x5FF137D4b0FDCD49DcA30c7CF57E578a026d2789"
            calldata = "0x1fad948c" + "0000000000000000000000000000000000000000000000000000000000000040" + "0000000000000000000000001111111111111111111111111111111111111111" + ("00" * 32)
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "UNISWAP_EXACT_INPUT":
            target = "0x3bFA4769FB09eefC5a80d6E87c3B9C650f7Ae48E"
            
            token_in = mock_env.contracts.target_erc20.replace("0x", "").zfill(64).lower() 
            token_out = "1c7D4B196Cb0C7B01d743Fbc6116a902379C7238".zfill(64).lower()       
            fee = hex(3000).replace("0x", "").zfill(64)                                     
            recipient = mock_env.agents.alpha.evm_address.replace("0x", "").zfill(64).lower()
            deadline = hex(int(time.time()) + 1800).replace("0x", "").zfill(64)             
            amount_in = hex(int(0.001 * 1e18)).replace("0x", "").zfill(64)                  
            amount_out_min = "0000000000000000000000000000000000000000000000000000000000000000"
            sqrt_price_limit = "0000000000000000000000000000000000000000000000000000000000000000"
            
            params_struct = token_in + token_out + fee + recipient + deadline + amount_in + amount_out_min + sqrt_price_limit
            calldata = "0x414bf389" + "0000000000000000000000000000000000000000000000000000000000000020" + params_struct
            
            storage_slots = []
            requires_access_list = True 
            
        elif scenario_type == "MERKLE_VERIFY":
            target = "0x6EDCE65403992e310A62460808c4b910D972f10f"
            calldata = "0xdeadbeef" + ("aa" * 32)
            storage_slots = []
            requires_access_list = False 
        else:
            raise ValueError(f"Unknown EVM scenario_type: {scenario_type}")

        if should_revert:
            calldata = "0xdeadbeef"
            
        return EvmIntent(
            target=target,
            caller=caller,
            calldata=calldata,
            value=value,
            storage_slots=storage_slots,
            requires_access_list=requires_access_list,
            scenario_type=scenario_type
        )

    @staticmethod
    def build_state_snapshot(
        address: str, 
        is_contract: bool = False,
        balance_wei: int = int(10 * 1e18),
        should_revert: bool = False
    ) -> Dict[str, Any]:
        if is_contract:
            mock_code = "0xfd" if should_revert else "0x608060405234801561001057600080fd5b506101"
        else:
            mock_code = "0x"
        
        padded_alpha_address = "0x000000000000000000000000" + mock_env.agents.alpha.evm_address[2:]
        
        return {
            "balance": hex(balance_wei),
            "nonce": random.randint(1, 100) if not is_contract else 1,
            "code": mock_code,
            "storage": {
                "0x0": padded_alpha_address,
                "0x1": hex(int(1000 * 1e18)),
                "0x2": "0x0000000000000000000000000000000000000000000000000000000000000001"
            }
        }

    @staticmethod
    def build_block_context() -> Dict[str, Any]:
        return {
            "timestamp": int(time.time()),
            "block_number": random.randint(19_000_000, 20_000_000),
            "coinbase": "0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5",
            "chain_id": mock_env.network.chain_id
        }