# fiber.dphi.eco.transaction.pipeline
import json
import uuid
import time
import asyncio
import base64
from typing import Any, Dict, Optional, List

from fiber.dphi.eco.transaction.rollup import ShadowAdapter

from xphi.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.bound.adapter.dphi.dvm import DvmAdapter
from xphi.bound.adapter.state import StateAdapter

from xphi.kernel.wasm.gateway import GatewayWasm
from xphi.state.ledger.consensus import KernelLedger, ToposBlob
from xphi.state.network.channel.pipeline import DuplexChannel, ChannelContext, ChannelPipeline
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

        if self.mode == "INVALID_SIGNATURE":
            log.warning("👾 [Chaos] EIP-712 서명 무효화 주입")
            msg["signature"] = None

        elif self.mode == "FORCE_INSUFFICIENT_ALLOWANCE" and "active_snapshot" in msg:
            log.warning("👾 [Chaos] 스토리지 스냅샷 변조: Allowance를 강제로 0으로 덮어씁니다.")
            target_contract = msg.get("target_contract")
            if target_contract and target_contract in msg["active_snapshot"]:
                if "storage" not in msg["active_snapshot"][target_contract]:
                    msg["active_snapshot"][target_contract]["storage"] = {}
                msg["active_snapshot"][target_contract]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000"

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
# 2. Defin Pipeline Components (WASM-Ready)
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

            safe_intent_dict = {
                "event_type": "FsmStartIntent",
                "tenant_address": caller_evm,
                "initial_deposit": int(msg.get("deposit_usdc", 0)),  
                "target_contract": msg.get("target_contract", "default")
            }
            await ctx.fire_channel_read(safe_intent_dict)
        else:
            await ctx.fire_channel_read(msg)

class DefinInfraHandler(DuplexChannel):
    """[OUTBOUND] 순수 Dict 포맷의 WASM 커맨드를 인프라로 구동"""
    def __init__(self, broker: Any, pta_adapter: Any, notary_keys: List[str]):
        self.broker = broker
        self.pta = pta_adapter
        self.notary_keys = notary_keys

    async def write(self, ctx: ChannelContext, command: Any):
        if not isinstance(command, dict):
            return await ctx.fire_write(command)

        cmd_type = command.get("command_type")

        try:
            if cmd_type == "MintGenesisPtaCmd":
                budget = command.get("budget", 0)
                owner = command.get("owner", "")
                log.info(f"⚡ [Infra] FSM 명령 수신: PTA 제네시스 발행 ({budget} Fuel)")

                tx = PtaTransaction(
                    inputs=[], 
                    outputs=[PtaOutput(amount=budget, owner=owner)],
                    metadata={"action": "GENESIS"}
                )
                tx_hash = await self.pta.execute_transaction(tx)

                await ctx.fire_channel_read({
                    "event_type": "PtaAnchoredEvent",
                    "tx_hash": tx_hash
                })

            elif cmd_type == "ExecuteParallelWasmCmd":
                agents = command.get("concurrent_agents", 1)
                budget_per_agent = command.get("budget_per_agent", 0)
                log.info(f"⚡ [Infra] FSM 명령 수신: WASM 병렬 실행 ({agents} 노드)")

                await asyncio.sleep(0.05)
                mock_tx_hashes = [f"0x_worker_tx_{i}" for i in range(agents)]

                await ctx.fire_channel_read({
                    "event_type": "WasmExecutedEvent",
                    "success": True,
                    "remaining_fuel": (budget_per_agent * agents) - 50000,
                    "worker_tx_hashes": mock_tx_hashes
                })

            elif cmd_type == "SealSettlementCmd":
                net_debt = command.get("net_debt", 0)
                log.info(f"⚡ [Infra] FSM 명령 수신: L1 정산 증명 Seal (Debt: {net_debt} micro-USDC)")

                receipt_hash = f"0x_mock_receipt_{uuid.uuid4().hex[:8]}"
                await ctx.fire_write({"status": "completed", "receipt": receipt_hash})

            elif cmd_type == "FsmHaltCmd":
                reason = command.get("reason", "Unknown Halt Reason")
                log.warning(f"🛑 [Infra] FSM 정지 명령 수신: {reason}")
                await ctx.fire_write({"status": "error", "reason": reason})

            else:
                await ctx.fire_write(command)

        except Exception as e:
            await ctx.fire_exception_caught(e)

class DefinFsmBridgeHandler(DuplexChannel):
    """[INBOUND] Defin 인프라 이벤트를 WASM FFI에 던지고, 상태를 관리하는 브릿지"""
    def __init__(self, wasm_gateway: GatewayWasm, concurrent_agents: int = 3):
        self.gateway = wasm_gateway
        self.concurrent_agents = concurrent_agents

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if not isinstance(msg, dict) or "event_type" not in msg:
            return await ctx.fire_channel_read(msg)

        event_payload = msg
        log.info(f"🧠 [WASM Bridge] Defin 이벤트 수신 및 상태 전이 요청: {event_payload['event_type']}")

        # 1. 컨텍스트에서 FSM 상태 복구 (없으면 Rust DefinFSM 스키마에 맞춰 초기화)
        fsm_state = ctx.get_attr("defin_fsm_state")
        if not fsm_state:
            fsm_state = {
                "state": "Init",
                "concurrent_agents": self.concurrent_agents,
                "tenant": "",
                "initial_deposit": 0,
                "authorized_fuel_budget": 0,
                "root_pta_hash": "",
                "all_tx_hashes": []
            }

        # 2. WASM 게이트웨이 타격 (O(1) FFI)
        try:
            receipt = self.gateway.execute_defin_fsm(
                fsm_state=fsm_state, 
                event=event_payload
            )
        except Exception as e:
            log.error(f"❌ [WASM Gateway Error] {e}")
            return await ctx.fire_exception_caught(e)

        if not receipt.get("success"):
            error_msg = receipt.get('revert_reason', 'Unknown WASM Error')
            return await ctx.fire_exception_caught(RuntimeError(f"WASM Defin Execution Reverted: {error_msg}"))

        # 3. 업데이트된 상태 보존
        ctx.set_attr("defin_fsm_state", receipt.get("next_fsm_state"))
        
        # 4. 커맨드 발산
        command = receipt.get("command")
        if command:
            log.info(f"🧠 [WASM Bridge] Defin 커맨드 발산: {command.get('command_type')}")
            await ctx.fire_write(command)


# =====================================================================
# 3. Transaction Pipeline Components (WASM-Ready)
# =====================================================================

class TransactionInfraHandler(DuplexChannel):
    def __init__(self, dvm_executor: Any, ledger: KernelLedger):
        self.dvm_executor = dvm_executor 
        self.ledger = ledger

    async def write(self, ctx: ChannelContext, command: Any):
        if not isinstance(command, dict):
            return await ctx.fire_write(command)

        cmd_type = command.get("command_type")

        try:
            if cmd_type == "ExecuteDvmCmd":
                log.info(f"⚡ [Infra] DVM Payload 조립 및 실행 요청 (Target: {command.get('target_contract')})")

                snapshot_bytes = base64.b64decode(command.get("active_snapshot", ""))
                snapshot_dict = json.loads(snapshot_bytes) if snapshot_bytes else {}

                dvm_payload = DvmAdapter.build_dvm_payload(
                    target_address=command.get("target_contract"),
                    calldata=command.get("active_calldata"),
                    state_snapshot=snapshot_dict,
                    vm_target="EVM"
                )

                res = await self.dvm_executor.execute_shadow(dvm_payload)

                if res.get("success"):
                    diff_dict = res.get("data", {}).get("state_diff", {})
                    diff_b64 = base64.b64encode(StateAdapter.to_canonical_bytes(diff_dict)).decode('utf-8') if diff_dict else ""

                    event_dict = {
                        "event_type": "DvmResultEvent",
                        "success": True,
                        "state_diff": diff_b64,
                        "gas_used": res.get("data", {}).get("gas_used", 0)
                    }
                else:
                    event_dict = {
                        "event_type": "DvmResultEvent",
                        "success": False, 
                        "revert_reason": res.get("error", "Unknown REVM Error")
                    }

                await ctx.fire_channel_read(event_dict)

            elif cmd_type == "LedgerSealCmd":
                gas_used = command.get("gas_used", 0)
                log.info(f"⚡ [Infra] Deferred Charge Ledger Sealing (Gas: {gas_used})")

                diff_dict = json.loads(base64.b64decode(command.get("state_diff", ""))) if command.get("state_diff") else {}

                blob = ToposBlob(
                    action="DEFERRED_SETTLEMENT_CHARGE",
                    from_state="dvm.wasm.execution",
                    to_state="ledger.sealed",
                    tension=0.5,
                    details=f"Gas: {gas_used} | Modified: {len(diff_dict)}"
                )

                sealed_hash = self.ledger.save_transition(blob)
                log.info(f"✅ [Infra] L2 원장 기록 완료. Rollup Hash: 0x{sealed_hash[:16]}...")

                await ctx.fire_write({
                    "status": "completed", 
                    "rollup_hash": sealed_hash,
                    "gas_used": gas_used
                })

            elif cmd_type == "HaltFsmCmd":
                await ctx.fire_write({
                    "status": "error", 
                    "reason": command.get("reason", "Unknown Halt Reason")
                })

            else:
                await ctx.fire_write(command)

        except Exception as e:
            await ctx.fire_exception_caught(e)

class TransactionBridgeHandler(DuplexChannel):
    def __init__(self, wasm_gateway: GatewayWasm):
        self.gateway = wasm_gateway

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        event_payload = None

        if isinstance(msg, dict) and msg.get("action") == "DEFERRED_CHARGE":
            raw_snapshot = msg.get("active_snapshot", {})
            canonical_bytes = StateAdapter.to_canonical_bytes(raw_snapshot)
            safe_snapshot_b64 = base64.b64encode(canonical_bytes).decode('utf-8')

            event_payload = {
                "event_type": "StartTransactionIntent",
                "caller": msg.get("caller", "0x00"),
                "charge_amount": msg.get("charge_amount", 0),
                "target_contract": msg.get("target_contract", "0x00"),
                "calldata": msg.get("calldata", "0x"),
                "active_snapshot": safe_snapshot_b64
            }

        elif isinstance(msg, dict) and msg.get("event_type") == "DvmResultEvent":
            event_payload = msg

        else:
            return await ctx.fire_channel_read(msg)

        log.info(f"🧠 [WASM Bridge] Transaction 이벤트 수신 및 상태 전이 요청: {event_payload['event_type']}")

        fsm_state = ctx.get_attr("transaction_fsm_state")
        if not fsm_state:
            fsm_state = {
                "state": "Init",
                "max_gas_limit": 200_000,
                "caller": "",
                "target_contract": "",
                "charge_amount": 0
            }

        try:
            receipt = self.gateway.execute_transaction_fsm(
                fsm_state=fsm_state, 
                event=event_payload
            )
        except Exception as e:
            log.error(f"❌ [WASM Gateway Error] {e}")
            return await ctx.fire_exception_caught(e)

        if not receipt.get("success"):
            error_msg = receipt.get('revert_reason', 'Unknown WASM Error')
            return await ctx.fire_exception_caught(RuntimeError(f"WASM Execution Reverted: {error_msg}"))

        ctx.set_attr("transaction_fsm_state", receipt.get("next_fsm_state"))

        command = receipt.get("command")
        if command:
            log.info(f"🧠 [WASM Bridge] Transaction 커맨드 발산: {command.get('command_type')}")
            await ctx.fire_write(command)


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
        
        # [NEW] WASM 런타임 인스턴스를 Defin 브릿지에 주입
        wasm_gateway = GatewayWasm()
        pipeline.add_last(DefinFsmBridgeHandler(
            wasm_gateway=wasm_gateway, 
            concurrent_agents=concurrent_agents
        ))
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

        # WASM 런타임 인스턴스를 Transaction 브릿지에 주입
        wasm_gateway = GatewayWasm()
        pipeline.add_last(TransactionBridgeHandler(wasm_gateway=wasm_gateway))
        pipeline.add_last(PipelineTailErrorHandler())

        return pipeline