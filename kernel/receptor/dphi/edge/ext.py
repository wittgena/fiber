# fiber.kernel.receptor.dphi.edge.ext
## @lineage: fiber.receptor.dphi.edge.ext
## @lineage: fiber.dphi.receptor.edge.ext
import json
import time
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Header
from pydantic import BaseModel, Field

from fiber.dphi.client.ext.evm import Web3Adapter
from fiber.dphi.client.ext.wallet import EthWalletAdapter
from fiber.dphi.adapter.rollup import RollupAdapter
from fiber.dphi.adapter.config import dphi_env

from xphi.arch.contract.interface import ContractRouter
from xphi.arch.eco.adapter.settlment import EcoAdapter, X402Invoice, X402SettlementReceipt
from xphi.watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.ext")

wallet_edge = ContractRouter(namespace="ext.wallet", prefix="/wallet", tags=["Ext Wallet"])
identity_edge = ContractRouter(namespace="ext.identity", prefix="/identity", tags=["Ext Identity"])
evm_edge = ContractRouter(namespace="ext.evm", prefix="/evm", tags=["Ext EVM (Web3)"])

_global_web3_adapter: Optional[Web3Adapter] = None
_global_eth_adapter: Optional[EthWalletAdapter] = None
_global_rollup_adapter: Optional[RollupAdapter] = None
_global_clearing_adapter: Optional[EthWalletAdapter] = None

async def get_web3_adapter() -> Web3Adapter:
    global _global_web3_adapter
    if _global_web3_adapter is None:
        log.info("[Edge Ext] Initializing Web3Adapter (Async RPC Connection)...")
        _global_web3_adapter = Web3Adapter()
    return _global_web3_adapter

async def get_eth_adapter(web3: Web3Adapter = Depends(get_web3_adapter)) -> EthWalletAdapter:
    global _global_eth_adapter
    if _global_eth_adapter is None:
        log.info("[Edge Ext] Initializing Native EthWalletAdapter (Agent)...")
        try:
            _global_eth_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="alpha", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init native wallet, falling back to simulation mode: {e}")
            _global_eth_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="alpha", simulate=True)
    return _global_eth_adapter

async def get_ledger_adapter() -> RollupAdapter:
    global _global_rollup_adapter
    if _global_rollup_adapter is None:
        log.info("[Edge Ext] Initializing DVM RollupAdapter for System Clearing...")
        try:
            _global_rollup_adapter = RollupAdapter(agent_alias="system_clearing", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init ledger wallet, falling back to simulation: {e}")
            _global_rollup_adapter = RollupAdapter(agent_alias="system_clearing", simulate=True)
    return _global_rollup_adapter

async def get_clearing_adapter(web3: Web3Adapter = Depends(get_web3_adapter)) -> EthWalletAdapter:
    global _global_clearing_adapter
    if _global_clearing_adapter is None:
        log.info("[Edge Ext] Initializing Native EthWalletAdapter (Clearinghouse Master)...")
        try:
            _global_clearing_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="system_clearing", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init clearing wallet, simulation mode: {e}")
            _global_clearing_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="system_clearing", simulate=True)
    return _global_clearing_adapter

class WalletInfoResponse(BaseModel):
    network_id: str
    wallet_address: str
    is_simulated: bool
    mode: str

class X402PaymentRequest(BaseModel):
    payee_address: str
    amount_usdc: str
    resource_id: str
    use_ledger: bool = False

class PaymentStatusResponse(BaseModel):
    status: str
    message: str
    receipt: Optional[Dict[str, Any]] = None

class BalanceResponse(BaseModel):
    address: str
    eth_wei: str
    weth_wei: str

class WrapRequest(BaseModel):
    caller_address: str
    amount_wei: str
    agent_alias: str = "beta"

class WrapResponse(BaseModel):
    status: str
    tx_hash: str
    message: str

class DeferredSettlementRequest(BaseModel):
    agent_address: str = Field(..., description="과금을 승인했던 에이전트의 L1 지갑 주소")
    accrued_debt_usdc: str = Field(..., description="징수할 누적 금액 (예: '15.5')")
    receipt_id: str = Field(..., description="부채가 기록된 X402 내부 영수증 ID")

class DeferredSettlementResponse(BaseModel):
    status: str
    message: str
    tx_hash: Optional[str] = None
    settled_amount: str

@wallet_edge.get(
    "/info",
    summary="Agent 지갑 상태 및 주소 조회",
    response_model=WalletInfoResponse
)
async def get_wallet_info(
    use_ledger: bool = False,
    eth_wallet: EthWalletAdapter = Depends(get_eth_adapter),
    ledger_wallet: RollupAdapter = Depends(get_ledger_adapter)
):
    wallet = ledger_wallet if use_ledger else eth_wallet
    mode_str = "DVM_LEDGER" if use_ledger else "NATIVE_EVM"
    
    return WalletInfoResponse(
        network_id=wallet.network_id,
        wallet_address=wallet.wallet_address,
        is_simulated=wallet.simulate,
        mode=mode_str
    )


@wallet_edge.post(
    "/pay/x402",
    summary="X402 HTTP 결제 정산 처리 (Agent 주도의 능동 결제)",
    response_model=PaymentStatusResponse
)
async def process_x402_payment(
    req: X402PaymentRequest,
    eth_wallet: EthWalletAdapter = Depends(get_eth_adapter),
    ledger_wallet: RollupAdapter = Depends(get_ledger_adapter)
):
    request_id = f"pay_{int(time.time() * 1000)}"
    mode_tag = "DVM Ledger" if req.use_ledger else "Native EVM"
    
    with flow_scope(phase="X402_PAYMENT", bound="edge.ext", req_id=request_id):
        log.info(f"Processing X402 payment via [{mode_tag}]: {req.amount_usdc} USDC to {req.payee_address}")
        
        try:
            invoice = EcoAdapter.build_x402_invoice(
                payee_address=req.payee_address,
                amount_usdc=req.amount_usdc,
                resource_id=req.resource_id
            )
            
            if req.use_ledger:
                # 🌟 [교정] DVM 지연 정산: 청산소(RollupAdapter)가 에이전트(eth_wallet)의 자금을 차감 (transferFrom 연산)
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
                # 🌟 외부망(EVM) 로직: 기존대로 EcoAdapter에 위임하여 즉시 전송
                receipt = await EcoAdapter.process_instant_settlement(
                    invoice=invoice,
                    agent_wallet_address=eth_wallet.wallet_address,
                    wallet_adapter=eth_wallet
                )
            
            log.info(f"[{mode_tag}] Payment successful. Tx/Rollup Hash: {receipt.tx_hash}")
            return PaymentStatusResponse(
                status="SUCCESS",
                message=f"X402 Settlement Completed via {mode_tag}",
                receipt=receipt.model_dump(exclude_none=True)
            )
            
        except Exception as e:
            log.error(f"X402 Payment failed via [{mode_tag}]: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Transaction/Rollup Failed: {str(e)}"
            )

@wallet_edge.post(
    "/settle/deferred",
    summary="[Internal/Worker] 지연 정산: 에이전트의 L1 지갑에서 누적 부채를 강제 징수 (transferFrom)",
    response_model=DeferredSettlementResponse
)
async def process_deferred_settlement(
    req: DeferredSettlementRequest,
    internal_auth: str = Header(..., description="내부 시스템 통신 인증 토큰"),
    clearing_wallet: EthWalletAdapter = Depends(get_clearing_adapter)
):
    request_id = f"stl_{int(time.time() * 1000)}"
    with flow_scope(phase="DEFERRED_SETTLEMENT", bound="edge.ext", req_id=request_id):
        log.info(f"Initiating L1 Pull Settlement: Pulling {req.accrued_debt_usdc} USDC from {req.agent_address}")
        
        try:
            tx_hash = await clearing_wallet.transfer_from(
                from_address=req.agent_address,
                amount_str=req.accrued_debt_usdc,
                asset="usdc"
            )
            
            log.info(f"Deferred Settlement successful. L1 TxHash: {tx_hash}")
            return DeferredSettlementResponse(
                status="SUCCESS",
                message="L1 Settlement Complete via transferFrom",
                tx_hash=tx_hash,
                settled_amount=req.accrued_debt_usdc
            )
        except Exception as e:
            log.error(f"L1 Settlement Pull failed: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"L1 transferFrom Failed: {str(e)}"
            )

@evm_edge.get("/balance", summary="EVM Native(ETH) 및 ERC20(WETH) 잔고 조회", response_model=BalanceResponse)
async def get_evm_balance(address: str, web3: Web3Adapter = Depends(get_web3_adapter)):
    try:
        balances = await web3.get_balances(address)
        return BalanceResponse(
            address=address,
            eth_wei=balances["eth_wei"],
            weth_wei=balances["weth_wei"]
        )
    except Exception as e:
        log.error(f"[EVM Edge] Balance fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@evm_edge.post("/wrap", summary="ETH -> WETH 변환 (Auto-Wrap 스마트 컨트랙트 호출)", response_model=WrapResponse)
async def wrap_native_to_weth(req: WrapRequest, web3: Web3Adapter = Depends(get_web3_adapter)):
    try:
        private_key = dphi_env.get_agent_pkey(req.agent_alias)
        amount_int = int(req.amount_wei)
        
        tx_hash = await web3.wrap_weth(
            caller_address=req.caller_address, 
            amount_wei=amount_int, 
            private_key=private_key
        )
        return WrapResponse(status="SUCCESS", tx_hash=tx_hash, message="Successfully wrapped WETH.")
        
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid amount format: {ve}")
    except RuntimeError as re:
        raise HTTPException(status_code=409, detail=str(re))
    except Exception as e:
        log.error(f"[EVM Edge] Wrap execution failed: {e}")
        raise HTTPException(status_code=500, detail="Internal Web3 Error")

ext_router = APIRouter(prefix="/v1/ext")
ext_router.include_router(wallet_edge)
ext_router.include_router(identity_edge)
ext_router.include_router(evm_edge)