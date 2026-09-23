# fiber.infra.rpc.ext
import time
from typing import Dict, Any, Optional
from pydantic import ValidationError

from fiber.infra.adapter.evm import Web3Adapter
from fiber.infra.adapter.wallet import EthWalletAdapter
from fiber.infra.transaction.rollup import RollupAdapter
from fiber.infra.adapter.config.exchange import exchange_config, NetEnv
from fiber.gateway.rest.serv.ext import X402PaymentRequest, DeferredSettlementRequest, WrapRequest

from xphi.arch.bound.adapter.settlement import MandateAdapter, X402SettlementReceipt
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("rpc.ext")

class ExtRpcService:
    def __init__(self):
        self._web3: Optional[Web3Adapter] = None
        self._eth: Optional[EthWalletAdapter] = None
        self._rollup: Optional[RollupAdapter] = None
        self._clearing: Optional[EthWalletAdapter] = None

    async def get_web3(self) -> Web3Adapter:
        if not self._web3:
            log.info("[ExtRpc] Initializing Web3Adapter...")
            self._web3 = Web3Adapter()
        return self._web3

    async def get_eth(self) -> EthWalletAdapter:
        if not self._eth:
            web3 = await self.get_web3()
            self._eth = EthWalletAdapter(web3_adapter=web3, agent_alias="alpha")
        return self._eth

    async def get_rollup(self) -> RollupAdapter:
        if not self._rollup:
            self._rollup = RollupAdapter(agent_alias="system_clearing")
        return self._rollup

    async def get_clearing(self) -> EthWalletAdapter:
        if not self._clearing:
            web3 = await self.get_web3()
            self._clearing = EthWalletAdapter(web3_adapter=web3, agent_alias="system_clearing")
        return self._clearing

    async def handle_wallet_info(self, params: Dict[str, Any], worker_ctx=None) -> Dict[str, Any]:
        use_ledger = params.get("use_ledger", False)
        wallet = await self.get_rollup() if use_ledger else await self.get_eth()
        
        is_simulated = (exchange_config.mode == NetEnv.LOCAL)
        
        return {
            "network_id": wallet.network_id,
            "wallet_address": wallet.wallet_address,
            "is_simulated": is_simulated,
            "mode": "DVM_LEDGER" if use_ledger else "NATIVE_EVM"
        }

    async def handle_pay_x402(self, params: Dict[str, Any], worker_ctx=None) -> Dict[str, Any]:
        try:
            req = X402PaymentRequest(**params)
        except ValidationError as e:
            return {"error": True, "code": 400, "message": f"Invalid format: {e.errors()}"}

        request_id = f"pay_{int(time.time() * 1000)}"
        mode_tag = "DVM Ledger" if req.use_ledger else "Native EVM"

        with flow_scope(phase="X402_PAYMENT", bound="rpc.ext", req_id=request_id):
            try:
                invoice = MandateAdapter.build_x402_invoice(
                    payee_address=req.payee_address, amount_usdc=req.amount_usdc, resource_id=req.resource_id
                )
                
                if req.use_ledger:
                    ledger_wallet = await self.get_rollup()
                    eth_wallet = await self.get_eth()
                    
                    agent_payer_address = eth_wallet.wallet_address
                    tx_hash = await ledger_wallet.process_deferred_charge(
                        agent_address=agent_payer_address,
                        amount_str=req.amount_usdc,
                        asset="usdc"
                    )
                    
                    receipt = X402SettlementReceipt(
                        receipt_id=f"rcpt_dvm_{tx_hash[2:14]}",
                        receipt_type="DVM_DEFERRED_CHARGE",
                        tx_hash=tx_hash,
                        network=ledger_wallet.network_id,
                        amount_usdc=req.amount_usdc,
                        payer_wallet=agent_payer_address,
                        settled_at=int(time.time() * 1000)
                    )
                else:
                    eth_wallet = await self.get_eth()
                    receipt = await MandateAdapter.process_instant_settlement(
                        invoice=invoice, agent_wallet_address=eth_wallet.wallet_address, wallet_adapter=eth_wallet
                    )

                return {
                    "status": "SUCCESS",
                    "message": f"Settlement Completed via {mode_tag}",
                    "receipt": receipt.model_dump(exclude_none=True)
                }
            except Exception as e:
                log.error(f"Payment failed via [{mode_tag}]: {e}", exc_info=True)
                return {"error": True, "code": 500, "message": str(e)}

    async def handle_deferred_settlement(self, params: Dict[str, Any], worker_ctx=None) -> Dict[str, Any]:
        try:
            req = DeferredSettlementRequest(**params)
        except ValidationError as e:
            return {"error": True, "code": 400, "message": f"Invalid format: {e.errors()}"}

        request_id = f"stl_{int(time.time() * 1000)}"
        with flow_scope(phase="DEFERRED_SETTLEMENT", bound="rpc.ext", req_id=request_id):
            log.info(f"Initiating L1 Pull Settlement: Pulling {req.accrued_debt_usdc} USDC from {req.agent_address}")
            
            try:
                clearing_wallet = await self.get_clearing()
                tx_hash = await clearing_wallet.transfer_from(
                    from_address=req.agent_address,
                    amount_str=req.accrued_debt_usdc,
                    asset="usdc"
                )
                
                log.info(f"Deferred Settlement successful. L1 TxHash: {tx_hash}")
                return {
                    "status": "SUCCESS",
                    "message": "L1 Settlement Complete via transferFrom",
                    "tx_hash": tx_hash,
                    "settled_amount": req.accrued_debt_usdc
                }
            except Exception as e:
                log.error(f"L1 Settlement Pull failed: {str(e)}", exc_info=True)
                return {"error": True, "code": 500, "message": f"L1 transferFrom Failed: {str(e)}"}

    async def handle_evm_balance(self, params: Dict[str, Any], worker_ctx=None) -> Dict[str, Any]:
        address = params.get("address")
        if not address:
            return {"error": True, "code": 400, "message": "address is required"}
            
        try:
            web3 = await self.get_web3()
            balances = await web3.get_balances(address)
            return {"address": address, "eth_wei": balances["eth_wei"], "weth_wei": balances["weth_wei"]}
        except Exception as e:
            return {"error": True, "code": 500, "message": str(e)}
            
    async def handle_evm_wrap(self, params: Dict[str, Any], worker_ctx=None) -> Dict[str, Any]:
        try:
            req = WrapRequest(**params)
            web3 = await self.get_web3()
            private_key = exchange_config.get_agent_pkey(req.agent_alias)
            tx_hash = await web3.wrap_weth(
                caller_address=req.caller_address, 
                amount_wei=int(req.amount_wei), 
                private_key=private_key
            )
            return {"status": "SUCCESS", "tx_hash": tx_hash, "message": "Wrapped successfully"}
        except Exception as e:
            return {"error": True, "code": 500, "message": str(e)}