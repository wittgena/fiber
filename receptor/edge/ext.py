# receptor.edge.ext
import json
import time
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Header
from pydantic import BaseModel, Field

from phase.anchor.adapter.web3 import Web3Adapter
from bound.client.wallet.eth import EthWalletAdapter
from bound.client.wallet.ledger import LedgerWalletAdapter
from phase.anchor.config.dphi import dphi_env

from arch.contract.interface import ContractRouter
from phase.anchor.adapter.eco import EcoAdapter, X402Invoice, X402SettlementReceipt
from watcher.plane.emitter import get_emitter, flow_scope

log = get_emitter("edge.ext")

wallet_edge = ContractRouter(namespace="ext.wallet", prefix="/wallet", tags=["Ext Wallet"])
identity_edge = ContractRouter(namespace="ext.identity", prefix="/identity", tags=["Ext Identity"])
evm_edge = ContractRouter(namespace="ext.evm", prefix="/evm", tags=["Ext EVM (Web3)"])

_global_web3_adapter: Optional[Web3Adapter] = None
_global_eth_adapter: Optional[EthWalletAdapter] = None
_global_ledger_adapter: Optional[LedgerWalletAdapter] = None
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

async def get_ledger_adapter() -> LedgerWalletAdapter:
    global _global_ledger_adapter
    if _global_ledger_adapter is None:
        log.info("[Edge Ext] Initializing DVM LedgerWalletAdapter...")
        try:
            _global_ledger_adapter = LedgerWalletAdapter(agent_alias="alpha", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init ledger wallet, falling back to simulation: {e}")
            _global_ledger_adapter = LedgerWalletAdapter(agent_alias="alpha", simulate=True)
    return _global_ledger_adapter

# 🌟 신규: 지연 정산을 수행할 DPHI 시스템 마스터 어댑터 의존성
async def get_clearing_adapter(web3: Web3Adapter = Depends(get_web3_adapter)) -> EthWalletAdapter:
    global _global_clearing_adapter
    if _global_clearing_adapter is None:
        log.info("[Edge Ext] Initializing Native EthWalletAdapter (Clearinghouse Master)...")
        try:
            # "system_clearing" 키는 DPHI가 에이전트 자금을 Pull할 권한(transferFrom)을 가진 메인 키
            _global_clearing_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="system_clearing", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init clearing wallet, simulation mode: {e}")
            _global_clearing_adapter = EthWalletAdapter(web3_adapter=web3, agent_alias="system_clearing", simulate=True)
    return _global_clearing_adapter


# =====================================================================
# [Data Models] 
# =====================================================================
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

# 🌟 신규 모델: 지연 정산 징수 요청
class DeferredSettlementRequest(BaseModel):
    """오프체인에서 마이너스(부채) 상태가 된 에이전트의 잔고를 L1에서 강제 징수(Pull)하는 요청"""
    agent_address: str = Field(..., description="과금을 승인했던 에이전트의 L1 지갑 주소")
    accrued_debt_usdc: str = Field(..., description="징수할 누적 금액 (예: '15.5')")
    receipt_id: str = Field(..., description="부채가 기록된 X402 내부 영수증 ID")

class DeferredSettlementResponse(BaseModel):
    status: str
    message: str
    tx_hash: Optional[str] = None
    settled_amount: str


# =====================================================================
# 1. Ext Wallet Endpoints
# =====================================================================
@wallet_edge.get(
    "/info",
    summary="Agent 지갑 상태 및 주소 조회",
    response_model=WalletInfoResponse
)
async def get_wallet_info(
    use_ledger: bool = False,
    eth_wallet: EthWalletAdapter = Depends(get_eth_adapter),
    ledger_wallet: LedgerWalletAdapter = Depends(get_ledger_adapter)
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
    ledger_wallet: LedgerWalletAdapter = Depends(get_ledger_adapter)
):
    request_id = f"pay_{int(time.time() * 1000)}"
    mode_tag = "DVM Ledger" if req.use_ledger else "Native EVM"
    
    with flow_scope(phase="X402_PAYMENT", bound="edge.ext", req_id=request_id):
        log.info(f"Processing X402 payment via [{mode_tag}]: {req.amount_usdc} USDC to {req.payee_address}")
        active_wallet = ledger_wallet if req.use_ledger else eth_wallet
        
        try:
            invoice = EcoAdapter.build_x402_invoice(
                payee_address=req.payee_address,
                amount_usdc=req.amount_usdc,
                resource_id=req.resource_id
            )
            
            agent_address = active_wallet.wallet_address
            
            receipt: X402SettlementReceipt = await EcoAdapter.process_x402_settlement(
                invoice=invoice,
                agent_wallet_address=agent_address,
                wallet_adapter=active_wallet
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


# 🌟 신규 추가 API: 지연 정산용 Pull API
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
    """
    에이전트가 DPHI를 호출하는 API가 아닙니다.
    DPHI 내부의 배치(Batch) 프로세스나 커널이 특정 에이전트의 오프체인 빚을 
    실제 L1/L2 블록체인에서 청산(수금)하고자 할 때 호출합니다.
    """
    request_id = f"stl_{int(time.time() * 1000)}"
    with flow_scope(phase="DEFERRED_SETTLEMENT", bound="edge.ext", req_id=request_id):
        log.info(f"Initiating L1 Pull Settlement: Pulling {req.accrued_debt_usdc} USDC from {req.agent_address}")
        
        try:
            # 에이전트가 사전에 DPHI 시스템 계정에 approve를 해둔 금액 한도 내에서 
            # clearing_wallet (DPHI 시스템 계정)이 transferFrom 트랜잭션을 발생시킴
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


# =====================================================================
# 2. Ext EVM (Web3) Endpoints
# =====================================================================
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