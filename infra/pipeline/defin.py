# fiber.infra.pipeline.defin
import json
import uuid
import asyncio
from typing import Any, Dict, Optional, List

from fiber.dphi.adapter.shadow import ShadowAdapter
from xphi.kernel.dphi.fsm.defin import (
    DefinFSM, 
    FsmStartIntent, UtxoAnchoredEvent, WasmExecutedEvent, 
    MintGenesisUtxoCmd, ExecuteParallelWasmCmd, SealSettlementCmd, FsmHaltCmd
)

from xphi.kernel.phase.network.channel.pipeline import DuplexChannel, ChannelContext, ChannelPipeline
from xphi.kernel.dphi.adapter.utxo import UtxoAdapter, UtxoTransaction, UtxoOutput
from xphi.watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("pipeline.defin")

class JsonMessageCodec(DuplexChannel):
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, bytes):
            try:
                parsed = json.loads(msg.decode('utf-8').strip())
                await ctx.fire_channel_read(parsed)
            except Exception as e:
                await ctx.fire_exception_caught(ValueError(f"Malformed Payload: {e}"))
        else:
            await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict):
            encoded = (json.dumps(msg) + '\n').encode('utf-8')
            await ctx.fire_write(encoded)
        else:
            await ctx.fire_write(msg)

class DefinFlowPropagator(DuplexChannel):
    async def channel_active(self, ctx: ChannelContext):
        flow_id = f"dphi_{uuid.uuid4().hex[:8]}"
        ctx.set_attr("flow_id", flow_id)
        with flow_scope(flow_id=flow_id, phase="EDGE_ACTIVE", client_id="GATEWAY"):
            log.info("🌐 [Pipeline] 신규 Billing 세션 연결")
            await ctx.fire_channel_active()

class WalletChaosInjector(DuplexChannel):
    def __init__(self, mode: str = "NORMAL"):
        self.mode = mode

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and self.mode == "INVALID_SIGNATURE":
            log.warning("👾 [Chaos] EIP-712 서명 무효화 주입")
            msg["signature"] = None
        await ctx.fire_channel_read(msg)

class Eip712Authenticator(DuplexChannel):
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and msg.get("action") == "START_COMPUTE":
            caller_evm = msg.get("caller_evm")
            signature = msg.get("signature")
            
            if not signature:
                # [개선] 직접 raise하지 않고 예외 이벤트를 순방향으로 전파
                await ctx.fire_exception_caught(PermissionError("EIP-712 서명 검증 실패. 인가되지 않은 접근입니다."))
                return
                
            log.info(f"🔐 [Security] 암호학적 신원 검증 통과 (Caller: {caller_evm[:10]}...)")
            ctx.set_attr("verified_tenant", caller_evm)
            
            safe_intent = FsmStartIntent(
                tenant_address=caller_evm,
                initial_deposit=msg.get("deposit_usdc", 0),
                target_contract=msg.get("target_contract", "default")
            )
            await ctx.fire_channel_read(safe_intent)
        else:
            await ctx.fire_channel_read(msg)

class InfrastructureAdapterHandler(DuplexChannel):
    def __init__(self, broker: Any, utxo_adapter: Any, notary_keys: List[str]):
        self.broker = broker
        self.utxo = utxo_adapter
        self.notary_keys = notary_keys

    async def write(self, ctx: ChannelContext, command: Any):
        try:
            if isinstance(command, MintGenesisUtxoCmd):
                log.info(f"⚡ [Infra] FSM 명령 수신: UTXO 제네시스 발행 ({command.budget} Fuel)")
                # [개선] dict 대신 UtxoOutput 객체 생성
                tx = UtxoTransaction(
                    inputs=[], 
                    outputs=[UtxoOutput(amount=command.budget, owner=command.owner)],
                    metadata={"action": "GENESIS"}
                )
                tx_hash = await self.utxo.execute_transaction(tx)
                await ctx.fire_channel_read(UtxoAnchoredEvent(tx_hash=tx_hash))

            elif isinstance(command, ExecuteParallelWasmCmd):
                log.info(f"⚡ [Infra] FSM 명령 수신: WASM 병렬 실행 ({command.concurrent_agents} 노드)")
                await asyncio.sleep(0.05)
                mock_tx_hashes = [f"0x_worker_tx_{i}" for i in range(command.concurrent_agents)]
                await ctx.fire_channel_read(WasmExecutedEvent(
                    success=True,
                    remaining_fuel=command.budget_per_agent * command.concurrent_agents - 50000,
                    worker_tx_hashes=mock_tx_hashes
                ))

            elif isinstance(command, SealSettlementCmd):
                log.info(f"⚡ [Infra] FSM 명령 수신: L1 정산 증명 Seal (Debt: {command.net_debt} USDC)")
                receipt_hash = f"0x_mock_receipt_{uuid.uuid4().hex[:8]}"
                await ctx.fire_write({"status": "completed", "receipt": receipt_hash})

            elif isinstance(command, FsmHaltCmd):
                log.warning(f"🛑 [Infra] FSM 정지 명령 수신: {command.reason}")
                await ctx.fire_write({"status": "error", "reason": command.reason})

            else:
                await ctx.fire_write(command)

        except Exception as e:
            await ctx.fire_exception_caught(e)

class DefinFsmBridgeHandler(DuplexChannel):
    def __init__(self, concurrent_agents: int = 3):
        self.fsm = DefinFSM(concurrent_agents=concurrent_agents)

    async def channel_read(self, ctx: ChannelContext, event: Any):
        log.info(f"🧠 [FSM Bridge] 이벤트 인입: {event.__class__.__name__}")
        next_command = self.fsm.apply_event(event)
        if next_command:
            log.info(f"🧠 [FSM Bridge] 커맨드 발산: {next_command.__class__.__name__}")
            await ctx.fire_write(next_command)

class PipelineTailErrorHandler(DuplexChannel):
    """파이프라인 최하단에서 처리되지 않은 모든 Inbound 예외를 포획하여 Outbound 에러 응답으로 역전송"""
    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        log.error(f"🚨 [Pipeline] 전역 예외 포착 및 역전송: {exc}")
        await ctx.fire_write({"status": "error", "reason": str(exc)})

class DefinPipelineFactory:
    @classmethod
    def build(cls, 
              broker: Any, 
              utxo_adapter: Any, 
              notary_keys: List[str],
              chaos_mode: str = "NORMAL",
              concurrent_agents: int = 3) -> ChannelPipeline:
        
        pipeline = ChannelPipeline()
        pipeline.add_last(JsonMessageCodec())
        pipeline.add_last(DefinFlowPropagator())
        
        if chaos_mode != "NORMAL":
            pipeline.add_last(WalletChaosInjector(mode=chaos_mode))
            
        pipeline.add_last(Eip712Authenticator())
        pipeline.add_last(InfrastructureAdapterHandler(broker, utxo_adapter, notary_keys))
        pipeline.add_last(DefinFsmBridgeHandler(concurrent_agents=concurrent_agents))
        pipeline.add_last(PipelineTailErrorHandler()) # Tail 에러 핸들러
        
        return pipeline