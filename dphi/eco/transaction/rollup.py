# fiber.dphi.eco.transaction.rollup
## @lineage: fiber.dphi.infra.transaction.rollup
import os
import time
import hashlib
import json
from typing import Dict, Any, List, Optional

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

from fiber.dphi.eco.config.exchange import dphi_env

from xphi.arch.model.surge.model import DynamicSurgeModel
from xphi.kernel.wasm.broker import DphiBroker
from xphi.kernel.adapter.state import StateAdapter
from xphi.kernel.adapter.dphi.dvm import DvmAdapter
from xphi.state.ledger.consensus import KernelLedger, ToposBlob
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("transaction.rollup")

class StateOverride(DynamicSurgeModel):
    slot_hash: str
    injected_value: str

class ShadowStateProjection(DynamicSurgeModel):
    target_address: str
    access_list_state: Dict[str, Any]
    overrides: Optional[List[StateOverride]] = None
    projected_at: int

class DeterministicIntent(DynamicSurgeModel):
    caller: str
    calldata: str
    value_wei: str
    gas_limit: int
    scenario_type: str

class ExecutionProofReceipt(DynamicSurgeModel):
    receipt_id: str
    status: str  # "PASS" or "REVERTED"
    canonical_hash: str
    gas_used: int
    sealed_at: int
    witness_signatures: List[str]


# ==========================================
# Shadow Adapter
# ==========================================

class ShadowAdapter:
    @classmethod
    def project_shadow_state(
        cls, 
        target_address: str, 
        base_state: Dict[str, Any], 
        overrides: Optional[List[Dict[str, str]]] = None
    ) -> ShadowStateProjection:
        parsed_overrides = []
        if overrides:
            for override in overrides:
                parsed_overrides.append(StateOverride(
                    slot_hash=override["slot_hash"],
                    injected_value=override["injected_value"]
                ))
                
        return ShadowStateProjection(
            target_address=target_address,
            access_list_state=base_state,
            overrides=parsed_overrides,
            projected_at=int(time.time() * 1000)
        )

    @classmethod
    def forge_intent(
        cls, 
        caller: str, 
        calldata: str, 
        scenario_type: str, 
        gas_limit: int = 30_000_000
    ) -> DeterministicIntent:
        return DeterministicIntent(
            caller=caller,
            calldata=calldata,
            value_wei="0",
            gas_limit=gas_limit,
            scenario_type=scenario_type
        )

    @classmethod
    def seal_execution_proof(
        cls, 
        execution_output: Dict[str, Any], 
        notary_keys: List[ed25519.Ed25519PrivateKey]
    ) -> ExecutionProofReceipt:
        canonical_bytes = json.dumps(execution_output, sort_keys=True, separators=(',', ':')).encode('utf-8')
        canonical_hash = hashlib.sha256(canonical_bytes).hexdigest()
        
        signatures = []
        for key in notary_keys:
            sig = key.sign(hashlib.sha256(canonical_bytes).digest()).hex()
            signatures.append(sig)

        is_success = execution_output.get("success", False)
        status_str = "PASS" if is_success else f"REVERTED_{execution_output.get('revert_reason', 'UNKNOWN')}"

        return ExecutionProofReceipt(
            receipt_id=f"proof_{canonical_hash[:12]}",
            status=status_str,
            canonical_hash=canonical_hash,
            gas_used=int(execution_output.get("gas_used", 0)),
            sealed_at=int(time.time() * 1000),
            witness_signatures=signatures
        )

    @classmethod
    def embed_shadow_context(
        cls, 
        base_cached_states: Dict[str, Any], 
        projection: Optional[ShadowStateProjection] = None, 
        proof: Optional[ExecutionProofReceipt] = None
    ) -> Dict[str, Any]:
        updated_state = dict(base_cached_states) if base_cached_states else {}
        if projection is not None:
            updated_state["shadow_projection"] = projection.model_dump(exclude_none=True)
            
        if proof is not None:
            updated_state["execution_proof"] = proof.model_dump(exclude_none=True)
            
        return updated_state


# ==========================================
# Rollup Adapter
# ==========================================

class RollupAdapter:
    def __init__(self, agent_alias: str = "system_clearing", simulate: bool = False):
        self.agent_alias = agent_alias
        self.simulate = simulate
        
        agent_config = getattr(dphi_env.agents, agent_alias)
        self.clearing_address = agent_config.evm_address.lower()
        self.wallet_address = self.clearing_address
        self.network_id = "dvm-rollup-chain"
        
        self.ledger = KernelLedger()
        self.broker = DphiBroker()
        log.info(f"[RollupAdapter] Initialized for {agent_alias}: {self.clearing_address} (Simulate: {simulate})")

    async def transfer(self, to_address: str, amount_str: str, asset: str = "usdc") -> str:
        if self.simulate:
            log.warning(f"[RollupAdapter] Simulated transfer: {amount_str} {asset} to {to_address}")
            return f"0x_simulated_transfer_{os.urandom(8).hex()}"

        log.info(f"[RollupAdapter] Initiating DVM Transfer for {amount_str} {asset.upper()} to {to_address}...")

        decimals = 6 if asset.lower() == "usdc" else 18
        amount_wei = int(float(amount_str) * (10 ** decimals))
        
        target_contract = getattr(dphi_env.contracts, f"target_{asset.lower()}").lower()
        valid_mock_erc20_bytecode = "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806323b872dd14602d575b600080fd5b00"

        calldata = DvmAdapter.build_erc20_transfer_calldata(to_address, amount_wei)
        
        state_snapshot = {
            self.wallet_address: DvmAdapter.build_evm_account_data(balance_wei=10**18, nonce=1),
            target_contract: DvmAdapter.build_evm_account_data(balance_wei=0, code_hex=valid_mock_erc20_bytecode),
            to_address.lower(): DvmAdapter.build_evm_account_data(balance_wei=0, nonce=0)
        }

        dvm_payload = DvmAdapter.build_dvm_payload(
            target_address=target_contract,
            calldata=calldata,
            state_snapshot=state_snapshot
        )
        
        execution_context = {"caller_address": self.wallet_address}
        
        try:
            result = await self.broker.execute(
                code=dvm_payload, 
                tier="STANDARD", 
                timeout=5.0, 
                context=execution_context
            )
            if not result.success:
                err_msg = str(result.error) if result.error else "Unknown Broker Error"
                if result.output:
                    err_msg += f" | DVM Output: {result.output}"
                raise RuntimeError(f"Broker Execution Failed: {err_msg}")
                
            data = json.loads(result.output)
            if not data.get("success"):
                raise RuntimeError(f"REVM Halted: {data.get('revert_reason', 'Unknown')}")
        except Exception as e:
            log.error(f"[RollupAdapter] Broker Execution Exception: {e}")
            raise RuntimeError(f"Internal DVM transfer failed: {str(e)}")

        state_diff = data.get("state_diff", {})
        gas_used = data.get("gas_used", 0)
        payload_dict = {
            "caller": self.wallet_address,
            "contract": target_contract,
            "state_diff": state_diff,
            "gas": gas_used,
            "timestamp": time.time()
        }

        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        rollup_hash = f"0x{hashlib.sha256(canonical_bytes).hexdigest()}"

        blob = ToposBlob(
            action="DVM_INTERNAL_TRANSFER",
            from_state="dvm.wasm.execution",
            to_state="ledger.sealed",
            tension=0.5,
            details=f"Gas: {gas_used} | Modified: {len(state_diff)}"
        )
        self.ledger.save_transition(blob)
        log.info(f"✨ [RollupAdapter] Local Rollup Hash generated for Transfer: {rollup_hash[:18]}...")

        return rollup_hash

    async def process_x402_settlement(self, invoice: Any) -> Any:
        from fiber.dphi.eco.transaction.settlement import X402SettlementReceipt
        
        tx_hash = await self.transfer(
            to_address=invoice.pay_to,
            amount_str=invoice.amount_usdc,
            asset="usdc"
        )
        
        return X402SettlementReceipt(
            receipt_id=f"rcpt_dvm_{tx_hash[2:14]}",
            receipt_type="DVM_INSTANT_PUSH",
            tx_hash=tx_hash,
            network=self.network_id,
            amount_usdc=invoice.amount_usdc,
            payer_wallet=self.wallet_address,
            settled_at=int(time.time() * 1000)
        )

    async def process_deferred_charge(self, agent_address: str, amount_str: str, asset: str = "usdc") -> str:
        if self.simulate:
            log.warning(f"[RollupAdapter] Simulated deferred charge: {amount_str} {asset} from {agent_address}")
            return f"0x_simulated_charge_{os.urandom(8).hex()}"

        log.info(f"[RollupAdapter] Initiating DVM Deferred Charge for {amount_str} {asset.upper()} from {agent_address}...")

        decimals = 6 if asset.lower() == "usdc" else 18
        amount_wei = int(float(amount_str) * (10 ** decimals))
        
        target_contract = getattr(dphi_env.contracts, f"target_{asset.lower()}").lower()
        valid_mock_erc20_bytecode = "0x6080604052348015600f57600080fd5b506004361060285760003560e01c806323b872dd14602d575b600080fd5b00"

        calldata = DvmAdapter.build_erc20_transfer_from_calldata(agent_address, self.clearing_address, amount_wei)
        
        state_snapshot = {
            self.clearing_address: DvmAdapter.build_evm_account_data(balance_wei=10**18, nonce=1),
            target_contract: DvmAdapter.build_evm_account_data(balance_wei=0, code_hex=valid_mock_erc20_bytecode),
            agent_address.lower(): DvmAdapter.build_evm_account_data(balance_wei=amount_wei * 100, nonce=0)
        }

        dvm_payload = DvmAdapter.build_dvm_payload(
            target_address=target_contract,
            calldata=calldata,
            state_snapshot=state_snapshot
        )
        
        execution_context = {"caller_address": self.clearing_address}
        
        try:
            result = await self.broker.execute(
                code=dvm_payload, 
                tier="STANDARD", 
                timeout=5.0, 
                context=execution_context
            )
            if not result.success:
                err_msg = str(result.error) if result.error else "Unknown Broker Error"
                if result.output:
                    err_msg += f" | DVM Output: {result.output}"
                raise RuntimeError(f"Broker Execution Failed: {err_msg}")
                
            data = json.loads(result.output)
            if not data.get("success"):
                raise RuntimeError(f"REVM Halted: {data.get('revert_reason', 'Unknown')}")
                
        except Exception as e:
            log.error(f"[RollupAdapter] Broker Execution Exception: {e}")
            raise RuntimeError(f"Off-chain deferred charge failed: {str(e)}")

        state_diff = data.get("state_diff", {})
        gas_used = data.get("gas_used", 0)

        payload_dict = {
            "caller": self.clearing_address,
            "contract": target_contract,
            "state_diff": state_diff,
            "gas": gas_used,
            "timestamp": time.time()
        }

        canonical_bytes = StateAdapter.to_canonical_bytes(payload_dict)
        rollup_hash = f"0x{hashlib.sha256(canonical_bytes).hexdigest()}"

        blob = ToposBlob(
            action="DEFERRED_SETTLEMENT_CHARGE",
            from_state="dvm.wasm.execution",
            to_state="ledger.sealed",
            tension=0.5,
            details=f"Gas: {gas_used} | Modified: {len(state_diff)}"
        )
        
        self.ledger.save_transition(blob)
        log.info(f"✨ [RollupAdapter] Local Rollup Hash generated for Charge: {rollup_hash[:18]}...")

        return rollup_hash