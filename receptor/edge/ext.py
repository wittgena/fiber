# receptor.edge.ext
import json
import time
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from arch.contract.interface import ContractRouter
from watcher.plane.emitter import get_emitter, flow_scope
from dphi.adapter.eco import EcoAdapter, X402Invoice, X402SettlementReceipt
from receptor.ext.wallet import WalletAdapter
from receptor.ext.web3 import Web3Adapter
from phase.epoch.config.dphi import mock_env

log = get_emitter("edge.ext")

wallet_edge = ContractRouter(namespace="ext.wallet", prefix="/wallet", tags=["Ext Wallet"])
identity_edge = ContractRouter(namespace="ext.identity", prefix="/identity", tags=["Ext Identity"])
evm_edge = ContractRouter(namespace="ext.evm", prefix="/evm", tags=["Ext EVM (Web3)"])

_global_wallet_adapter: Optional[WalletAdapter] = None
_global_web3_adapter: Optional[Web3Adapter] = None

def get_wallet_adapter() -> WalletAdapter:
    global _global_wallet_adapter
    if _global_wallet_adapter is None:
        log.info("[Edge Ext] Initializing Secure WalletAdapter...")
        try:
            # 실제 운영 시 환경변수나 Secret Manager에서 키를 주입받아 simulate=False로 구동
            _global_wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=False)
        except Exception as e:
            log.warning(f"[Edge Ext] Failed to init secure wallet, falling back to simulation mode: {e}")
            _global_wallet_adapter = WalletAdapter(network_id="base-sepolia", simulate=True)
            
    return _global_wallet_adapter

# 🌟 Web3 어댑터 주입기 추가
async def get_web3_adapter() -> Web3Adapter:
    global _global_web3_adapter
    if _global_web3_adapter is None:
        log.info("[Edge Ext] Initializing Web3Adapter (Async RPC Connection)...")
        _global_web3_adapter = Web3Adapter()
    return _global_web3_adapter

# =====================================================================
# Models
# =====================================================================
class WalletInfoResponse(BaseModel):
    network_id: str
    wallet_address: str
    is_simulated: bool

class X402PaymentRequest(BaseModel):
    payee_address: str
    amount_usdc: str
    resource_id: str

class PaymentStatusResponse(BaseModel):
    status: str
    message: str
    receipt: Optional[Dict[str, Any]] = None

# 🌟 신규: EVM 관련 Pydantic 모델
class BalanceResponse(BaseModel):
    address: str
    eth_wei: str
    weth_wei: str

class WrapRequest(BaseModel):
    caller_address: str
    amount_wei: str
    agent_alias: str = "beta"  # 보안 상 API로 Private Key를 직접 받지 않고 서버 단에서 조회

class WrapResponse(BaseModel):
    status: str
    tx_hash: str
    message: str

# =====================================================================
# 1. Ext Wallet Endpoints
# =====================================================================
@wallet_edge.get(
    "/info",
    summary="Agent 지갑 상태 및 주소 조회",
    response_model=WalletInfoResponse
)
async def get_wallet_info(wallet: WalletAdapter = Depends(get_wallet_adapter)):
    address = "simulate_address_0x000"
    if wallet.wallet and hasattr(wallet.wallet, 'default_address'):
        address = wallet.wallet.default_address.address_id

    return WalletInfoResponse(
        network_id=wallet.network_id,
        wallet_address=address,
        is_simulated=wallet.simulate
    )

@wallet_edge.post(
    "/pay/x402",
    summary="X402 HTTP 결제 정산 처리 (CDP 트랜잭션)",
    response_model=PaymentStatusResponse
)
async def process_x402_payment(
    req: X402PaymentRequest,
    wallet: WalletAdapter = Depends(get_wallet_adapter)
):
    request_id = f"pay_{int(time.time() * 1000)}"
    
    with flow_scope(phase="X402_PAYMENT", bound="edge.ext", req_id=request_id):
        log.info(f"Processing X402 payment: {req.amount_usdc} USDC to {req.payee_address}")
        
        try:
            # 1. 청구서(Invoice) 생성 - 도메인 로직(EcoAdapter) 사용
            invoice = EcoAdapter.build_x402_invoice(
                payee_address=req.payee_address,
                amount_usdc=req.amount_usdc,
                resource_id=req.resource_id
            )
            
            # 2. 송금 주소 확보
            agent_address = "simulate_address_0x000"
            if wallet.wallet and hasattr(wallet.wallet, 'default_address'):
                agent_address = wallet.wallet.default_address.address_id
                
            # 3. 실제 지갑을 통한 온체인 트랜잭션 전송 및 영수증 발급
            receipt: X402SettlementReceipt = EcoAdapter.process_x402_settlement(
                invoice=invoice,
                agent_wallet_address=agent_address,
                wallet_adapter=wallet
            )
            
            log.info(f"Payment successful. TxHash: {receipt.tx_hash}")
            
            return PaymentStatusResponse(
                status="SUCCESS",
                message="X402 Settlement Completed",
                receipt=receipt.model_dump(exclude_none=True)
            )
            
        except Exception as e:
            log.error(f"X402 Payment failed: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Transaction Failed: {str(e)}"
            )

# =====================================================================
# 2. Ext EVM (Web3) Endpoints 🌟 신규 라우팅 블록
# =====================================================================
@evm_edge.get(
    "/balance", 
    summary="EVM Native(ETH) 및 ERC20(WETH) 잔고 조회", 
    response_model=BalanceResponse
)
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


@evm_edge.post(
    "/wrap", 
    summary="ETH -> WETH 변환 (Auto-Wrap 스마트 컨트랙트 호출)", 
    response_model=WrapResponse
)
async def wrap_native_to_weth(req: WrapRequest, web3: Web3Adapter = Depends(get_web3_adapter)):
    try:
        # 엣지 서버(안전 영역)에서 Private Key를 획득합니다.
        private_key = mock_env.get_agent_pkey(req.agent_alias)
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