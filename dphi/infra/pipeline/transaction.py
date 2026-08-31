# fiber.dphi.infra.pipeline.transaction
## @lineage: fiber.dphi.workflow.pipeline.transaction
import json
import time
import asyncio
from typing import Any, Dict, Optional

from fiber.dphi.infra.adapter.dvm import DvmAdapter
from xphi.kernel.dphi.fsm.transaction import (
    TransactionFSM,
    StartTransactionIntent, DvmResultEvent,
    ExecuteDvmCmd, LedgerSealCmd, HaltFsmCmd
)

from xphi.kernel.dphi.ledger.consensus import KernelLedger, ToposBlob
from xphi.kernel.phase.network.channel.pipeline import DuplexChannel, ChannelContext, ChannelPipeline
from xphi.watcher.plane.emitter import get_emitter

log = get_emitter("pipeline.transaction")

class JsonMessageCodec(DuplexChannel):
    """Raw Bytes <-> JSON Dictionary 양방향 직렬화"""
    def __init__(self):
        self._buffer = bytearray()

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, bytes):
            self._buffer.extend(msg)
            while b'\n' in self._buffer:
                frame, _, remainder = self._buffer.partition(b'\n')
                self._buffer = bytearray(remainder)
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
    def __init__(self, mode: str = "NORMAL"):
        self.mode = mode

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if not isinstance(msg, dict):
            return await ctx.fire_channel_read(msg)

        # 시나리오 2: 상태 변조 (Allowance 삭제)
        if self.mode == "FORCE_INSUFFICIENT_ALLOWANCE" and "active_snapshot" in msg:
            log.warning("👾 [Chaos] 스토리지 스냅샷 변조: Allowance를 강제로 0으로 덮어씁니다.")
            target_contract = msg.get("target_contract")
            if target_contract and target_contract in msg["active_snapshot"]:
                if "storage" not in msg["active_snapshot"][target_contract]:
                    msg["active_snapshot"][target_contract]["storage"] = {}
                # Allowance 슬롯을 0으로 오염
                msg["active_snapshot"][target_contract]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000"

        # 시나리오 3: Calldata 훼손 (Invalid Opcode 유발)
        elif self.mode == "CORRUPT_CALLDATA" and "calldata" in msg:
            log.warning("👾 [Chaos] 패킷 오염: 트랜잭션 Calldata 훼손 중...")
            msg["calldata"] = "0xdeadbeef" + msg["calldata"][10:]

        await ctx.fire_channel_read(msg)

class TransactionInfraHandler(DuplexChannel):
    def __init__(self, dvm_executor: Any, ledger: KernelLedger):
        self.dvm_executor = dvm_executor 
        self.ledger = ledger

    async def write(self, ctx: ChannelContext, command: Any):
        try:
            if isinstance(command, ExecuteDvmCmd):
                log.info(f"⚡ [Infra] DVM Payload 조립 및 실행 요청 (Target: {command.target_contract})")
                
                # DvmAdapter를 통해 타겟 VM에 맞는 표준 페이로드로 조립
                dvm_payload = DvmAdapter.build_dvm_payload(
                    target_address=command.target_contract,
                    calldata=command.active_calldata,
                    state_snapshot=command.active_snapshot,
                    vm_target="EVM"
                )
                
                # 가상머신 I/O 실행 대기
                res = await self.dvm_executor.execute_shadow(dvm_payload)
                
                # 실행 결과를 이벤트 객체로 파싱하여 FSM으로 콜백(역전송)
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
                
                # FSM으로 이벤트 Push
                await ctx.fire_channel_read(event)

            # -------------------------------------------------------------
            # 명령 2. L2 Rollup 원장(Ledger)에 증명 데이터 기록(Seal)
            # -------------------------------------------------------------
            elif isinstance(command, LedgerSealCmd):
                log.info(f"⚡ [Infra] Deferred Charge Ledger Sealing (Gas: {command.gas_used})")
                
                # ToposBlob 생성 및 원장 I/O
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
                # 클라이언트에게 에러 응답 반환
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

        # 2. FSM에 이벤트 주입 및 동기적(Deterministic) 상태 전이 실행
        log.info(f"🧠 [FSM Bridge] 이벤트 수신 및 상태 전이 시작: {event.__class__.__name__}")
        next_command = self.fsm.apply_event(event)
        if next_command:
            log.info(f"🧠 [FSM Bridge] 커맨드 발산: {next_command.__class__.__name__}")
            await ctx.fire_write(next_command)

class PipelineTailErrorHandler(DuplexChannel):
    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        log.error(f"🚨 [Pipeline] 전역 예외 포착 및 역전송: {exc}")
        await ctx.fire_write({"status": "error", "reason": str(exc)})

class TransactionPipelineFactory:
    """애플리케이션 구동 시 또는 E2E 테스트에서 파이프라인을 찍어내는 팩토리"""
    
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