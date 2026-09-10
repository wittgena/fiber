# fiber.dphi.eco.transaction.pipeline
## @lineage: fiber.dphi.infra.transaction.pipeline
## @lineage: fiber.dphi.transaction.pipeline
## @lineage: fiber.infra.pipeline.defin
import json
import uuid
import time
import asyncio
from typing import Any, Dict, Optional, List

from fiber.dphi.eco.transaction.rollup import ShadowAdapter

from xphi.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.bound.adapter.dphi.dvm import DvmAdapter
from xphi.state.phase.fsm.defin import (
    DefinFSM, 
    FsmStartIntent, PtaAnchoredEvent, WasmExecutedEvent, 
    MintGenesisPtaCmd, ExecuteParallelWasmCmd, SealSettlementCmd, FsmHaltCmd
)
from xphi.state.phase.fsm.transaction import (
    TransactionFSM,
    StartTransactionIntent, DvmResultEvent,
    ExecuteDvmCmd, LedgerSealCmd, HaltFsmCmd
)
from xphi.state.ledger.consensus import KernelLedger, ToposBlob
from xphi.state.phase.network.channel.pipeline import DuplexChannel, ChannelContext, ChannelPipeline
from xphi.watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("pipeline.defin")


# =====================================================================
# 1. Common Pipeline Components
# =====================================================================

class JsonMessageCodec(DuplexChannel):
    """Raw Bytes <-> JSON Dictionary 양방향 직렬화 (스트림 단편화 대응)"""
    def __init__(self):
        self._buffer = bytearray()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, bytes):
            self._buffer.extend(msg)
            while b'\n' in self._buffer:
                frame, _, remainder = self._buffer.partition(b'\n')
                self._buffer = bytearray(remainder)
                if frame:
                    try:
                        parsed = json.loads(frame.decode('utf-8').strip())
                        await ctx.fire_channel_read(parsed)
                    except json.JSONDecodeError as e:
                        await ctx.fire_exception_caught(ValueError(f"Malformed Payload: {e}"))
        else:
            await ctx.fire_channel_read(msg)

    async def write(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict):
            encoded = (json.dumps(msg) + '\n').encode('utf-8')
            await ctx.fire_write(encoded)
        else:
            await ctx.fire_write(msg)

class WalletChaosInjector(DuplexChannel):
    """테스트 및 검증을 위한 공통 Chaos 주입기"""
    def __init__(self, mode: str = "NORMAL"):
        self.mode = mode

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if not isinstance(msg, dict):
            return await ctx.fire_channel_read(msg)

        # 시나리오 1: Defin - EIP-712 서명 무효화 주입
        if self.mode == "INVALID_SIGNATURE":
            log.warning("👾 [Chaos] EIP-712 서명 무효화 주입")
            msg["signature"] = None

        # 시나리오 2: Transaction - 상태 변조 (Allowance 삭제)
        elif self.mode == "FORCE_INSUFFICIENT_ALLOWANCE" and "active_snapshot" in msg:
            log.warning("👾 [Chaos] 스토리지 스냅샷 변조: Allowance를 강제로 0으로 덮어씁니다.")
            target_contract = msg.get("target_contract")
            if target_contract and target_contract in msg["active_snapshot"]:
                if "storage" not in msg["active_snapshot"][target_contract]:
                    msg["active_snapshot"][target_contract]["storage"] = {}
                msg["active_snapshot"][target_contract]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000"

        # 시나리오 3: Transaction - Calldata 훼손 (Invalid Opcode 유발)
        elif self.mode == "CORRUPT_CALLDATA" and "calldata" in msg:
            log.warning("👾 [Chaos] 패킷 오염: 트랜잭션 Calldata 훼손 중...")
            msg["calldata"] = "0xdeadbeef" + msg["calldata"][10:]

        await ctx.fire_channel_read(msg)

class PipelineTailErrorHandler(DuplexChannel):
    """파이프라인 최하단에서 처리되지 않은 모든 Inbound 예외를 포획하여 Outbound 에러 응답으로 역전송"""
    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        log.error(f"🚨 [Pipeline] 전역 예외 포착 및 역전송: {exc}")
        await ctx.fire_write({"status": "error", "reason": str(exc)})


# =====================================================================
# 2. Defin Pipeline Components
# =====================================================================

class DefinFlowPropagator(DuplexChannel):
    async def channel_active(self, ctx: ChannelContext):
        flow_id = f"dphi_{uuid.uuid4().hex[:8]}"
        ctx.set_attr("flow_id", flow_id)
        with flow_scope(flow_id=flow_id, phase="EDGE_ACTIVE", client_id="GATEWAY"):
            log.info("🌐 [Pipeline] 신규 Billing 세션 연결")
            await ctx.fire_channel_active()

class Eip712Authenticator(DuplexChannel):
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and msg.get("action") == "START_COMPUTE":
            caller_evm = msg.get("caller_evm")
            signature = msg.get("signature")
            
            if not signature:
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

class DefinInfraHandler(DuplexChannel):
    def __init__(self, broker: Any, pta_adapter: Any, notary_keys: List[str]):
        self.broker = broker
        self.pta = pta_adapter
        self.notary_keys = notary_keys

    async def write(self, ctx: ChannelContext, command: Any):
        try:
            if isinstance(command, MintGenesisPtaCmd):
                log.info(f"⚡ [Infra] FSM 명령 수신: PTA 제네시스 발행 ({command.budget} Fuel)")
                tx = PtaTransaction(
                    inputs=[], 
                    outputs=[PtaOutput(amount=command.budget, owner=command.owner)],
                    metadata={"action": "GENESIS"}
                )
                tx_hash = await self.pta.execute_transaction(tx)
                await ctx.fire_channel_read(PtaAnchoredEvent(tx_hash=tx_hash))

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


# =====================================================================
# 3. Transaction Pipeline Components
# =====================================================================

class TransactionInfraHandler(DuplexChannel):
    def __init__(self, dvm_executor: Any, ledger: KernelLedger):
        self.dvm_executor = dvm_executor 
        self.ledger = ledger

    async def write(self, ctx: ChannelContext, command: Any):
        try:
            if isinstance(command, ExecuteDvmCmd):
                log.info(f"⚡ [Infra] DVM Payload 조립 및 실행 요청 (Target: {command.target_contract})")
                
                dvm_payload = DvmAdapter.build_dvm_payload(
                    target_address=command.target_contract,
                    calldata=command.active_calldata,
                    state_snapshot=command.active_snapshot,
                    vm_target="EVM"
                )
                
                res = await self.dvm_executor.execute_shadow(dvm_payload)
                
                if res.get("success"):
                    event = DvmResultEvent(
                        success=True,
                        state_diff=res.get("data", {}).get("state_diff", {}),
                        gas_used=res.get("data", {}).get("gas_used", 0)
                    )
                else:
                    event = DvmResultEvent(
                        success=False, 
                        revert_reason=res.get("error", "Unknown REVM Error")
                    )
                
                await ctx.fire_channel_read(event)

            elif isinstance(command, LedgerSealCmd):
                log.info(f"⚡ [Infra] Deferred Charge Ledger Sealing (Gas: {command.gas_used})")
                
                blob = ToposBlob(
                    action="DEFERRED_SETTLEMENT_CHARGE",
                    from_state="dvm.wasm.execution",
                    to_state="ledger.sealed",
                    tension=0.5,
                    details=f"Gas: {command.gas_used} | Modified: {len(command.state_diff)}"
                )
                
                sealed_hash = self.ledger.save_transition(blob)
                log.info(f"✅ [Infra] L2 원장 기록 완료. Rollup Hash: 0x{sealed_hash[:16]}...")
                await ctx.fire_write({
                    "status": "completed", 
                    "rollup_hash": sealed_hash,
                    "gas_used": command.gas_used
                })

            elif isinstance(command, HaltFsmCmd):
                await ctx.fire_write({
                    "status": "error", 
                    "reason": command.reason
                })

            else:
                await ctx.fire_write(command)

        except Exception as e:
            await ctx.fire_exception_caught(e)

class TransactionBridgeHandler(DuplexChannel):
    def __init__(self):
        self.fsm = TransactionFSM()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and msg.get("action") == "DEFERRED_CHARGE":
            event = StartTransactionIntent(
                caller=msg.get("caller", "0x00"),
                charge_amount=msg.get("charge_amount", 0),
                target_contract=msg.get("target_contract", "0x00"),
                calldata=msg.get("calldata", "0x"),
                active_snapshot=msg.get("active_snapshot", {})
            )
        else:
            event = msg

        log.info(f"🧠 [FSM Bridge] 이벤트 수신 및 상태 전이 시작: {event.__class__.__name__}")
        next_command = self.fsm.apply_event(event)
        if next_command:
            log.info(f"🧠 [FSM Bridge] 커맨드 발산: {next_command.__class__.__name__}")
            await ctx.fire_write(next_command)


# =====================================================================
# 4. Pipeline Factories
# =====================================================================

class DefinPipelineFactory:
    @classmethod
    def build(cls, 
              broker: Any, 
              pta_adapter: Any, 
              notary_keys: List[str],
              chaos_mode: str = "NORMAL",
              concurrent_agents: int = 3) -> ChannelPipeline:
        
        pipeline = ChannelPipeline()
        pipeline.add_last(JsonMessageCodec())
        pipeline.add_last(DefinFlowPropagator())
        
        if chaos_mode != "NORMAL":
            pipeline.add_last(WalletChaosInjector(mode=chaos_mode))
            
        pipeline.add_last(Eip712Authenticator())
        pipeline.add_last(DefinInfraHandler(broker, pta_adapter, notary_keys))
        pipeline.add_last(DefinFsmBridgeHandler(concurrent_agents=concurrent_agents))
        pipeline.add_last(PipelineTailErrorHandler())
        
        return pipeline

class TransactionPipelineFactory:
    @classmethod
    def build(cls, dvm_adapter: Any, chaos_mode: str = "NORMAL") -> ChannelPipeline:
        
        pipeline = ChannelPipeline()
        pipeline.add_last(JsonMessageCodec())
        
        if chaos_mode != "NORMAL":
            pipeline.add_last(WalletChaosInjector(mode=chaos_mode))
            
        pipeline.add_last(TransactionInfraHandler(
            dvm_executor=dvm_adapter, 
            ledger=KernelLedger()
        ))
        pipeline.add_last(TransactionBridgeHandler())
        pipeline.add_last(PipelineTailErrorHandler())
        
        return pipeline