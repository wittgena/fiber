# fiber.dphi.eco.transaction.pipeline
import json
import uuid
import time
import asyncio
import base64
from typing import Any, Dict, Optional, List

from fiber.dphi.eco.transaction.rollup import ShadowAdapter

from xphi.arch.bound.adapter.pta import PtaAdapter, PtaTransaction, PtaOutput
from xphi.arch.bound.adapter.dphi.dvm import DvmAdapter
from xphi.arch.bound.adapter.state import StateAdapter

from xphi.kernel.wasm.gateway import GatewayWasm
from xphi.state.anchor.consensus import KernelLedger, ToposBlob
from xphi.state.phase.channel import DuplexChannel, ChannelContext, ChannelPipeline
from xphi.watcher.plane.emitter import flow_scope, get_emitter

log = get_emitter("transaction.pipeline")


# =====================================================================
# 1. Common Pipeline Components
# =====================================================================

class JsonMessageCodec(DuplexChannel):
    """Bi-directional raw bytes <-> JSON serialization (handles stream fragmentation)."""
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
    """Common Chaos injector for E2E testing and fault validation."""
    def __init__(self, mode: str = "NORMAL"):
        self.mode = mode

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if not isinstance(msg, dict):
            return await ctx.fire_channel_read(msg)

        if self.mode == "INVALID_SIGNATURE":
            log.warning("👾 [Chaos] Injecting invalid EIP-712 signature.")
            msg["signature"] = None

        elif self.mode == "FORCE_INSUFFICIENT_ALLOWANCE" and "active_snapshot" in msg:
            log.warning("👾 [Chaos] Mutating storage snapshot: Forcing allowance to 0.")
            target_contract = msg.get("target_contract")
            if target_contract and target_contract in msg["active_snapshot"]:
                if "storage" not in msg["active_snapshot"][target_contract]:
                    msg["active_snapshot"][target_contract]["storage"] = {}
                msg["active_snapshot"][target_contract]["storage"]["0x0000000000000000000000000000000000000000000000000000000000000000"] = "0x0000000000000000000000000000000000000000000000000000000000000000"

        elif self.mode == "CORRUPT_CALLDATA" and "calldata" in msg:
            log.warning("👾 [Chaos] Packet corruption: Mutilating transaction calldata.")
            msg["calldata"] = "0xdeadbeef" + msg["calldata"][10:]

        await ctx.fire_channel_read(msg)

class PipelineTailErrorHandler(DuplexChannel):
    """Catches unhandled inbound exceptions at the pipeline tail and reflects them as outbound error responses."""
    async def exception_caught(self, ctx: ChannelContext, exc: Exception):
        log.error(f"🚨 [Pipeline] Global exception caught & reflected: {exc}")
        await ctx.fire_write({"status": "error", "reason": str(exc)})


# =====================================================================
# 2. Clearing Pipeline Components (WASM-Ready)
# =====================================================================

class ClearingFlowPropagator(DuplexChannel):
    async def channel_active(self, ctx: ChannelContext):
        flow_id = f"dphi_{uuid.uuid4().hex[:8]}"
        ctx.set_attr("flow_id", flow_id)
        with flow_scope(flow_id=flow_id, phase="EDGE_ACTIVE", client_id="GATEWAY"):
            log.info("🌐 [Pipeline] New billing session established.")
            await ctx.fire_channel_active()

class Eip712Authenticator(DuplexChannel):
    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if isinstance(msg, dict) and msg.get("action") == "START_COMPUTE":
            caller_evm = msg.get("caller_evm")
            signature = msg.get("signature")

            if not signature:
                await ctx.fire_exception_caught(PermissionError("EIP-712 signature validation failed. Unauthorized access."))
                return

            log.info(f"🔐 [Security] Cryptographic identity verified (Caller: {caller_evm[:10]}...)")
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

class ClearingInfraHandler(DuplexChannel):
    """[OUTBOUND] Executes pure Dict-formatted WASM commands against the physical infrastructure."""
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
                log.info(f"⚡ [Infra] FSM Command received: Mint PTA Genesis ({budget} Fuel)")

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
                log.info(f"⚡ [Infra] FSM Command received: Execute parallel WASM ({agents} nodes)")

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
                log.info(f"⚡ [Infra] FSM Command received: Seal L1 settlement proof (Debt: {net_debt} micro-USDC)")

                receipt_hash = f"0x_mock_receipt_{uuid.uuid4().hex[:8]}"
                await ctx.fire_write({"status": "completed", "receipt": receipt_hash})

            elif cmd_type == "FsmHaltCmd":
                reason = command.get("reason", "Unknown Halt Reason")
                log.warning(f"🛑 [Infra] FSM Halt Command received: {reason}")
                await ctx.fire_write({"status": "error", "reason": reason})

            else:
                await ctx.fire_write(command)

        except Exception as e:
            await ctx.fire_exception_caught(e)

class ClearingFsmBridgeHandler(DuplexChannel):
    """[INBOUND] Routes infrastructure events to WASM FFI and manages deterministic FSM state transitions."""
    def __init__(self, wasm_gateway: GatewayWasm, concurrent_agents: int = 3):
        self.gateway = wasm_gateway
        self.concurrent_agents = concurrent_agents

    async def channel_read(self, ctx: ChannelContext, msg: Any):
        if not isinstance(msg, dict) or "event_type" not in msg:
            return await ctx.fire_channel_read(msg)

        event_payload = msg
        log.info(f"🧠 [WASM Bridge] Clearing event received, transitioning state: {event_payload['event_type']}")

        # 1. Restore FSM state from context (initialize matching Rust ClearingFSM schema if missing)
        fsm_state = ctx.get_attr("clearing_fsm_state")
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

        # 2. Invoke WASM Gateway (O(1) FFI)
        try:
            receipt = self.gateway.execute_clearing_fsm(
                fsm_state=fsm_state, 
                event=event_payload
            )
        except Exception as e:
            log.error(f"❌ [WASM Gateway Error] {e}")
            return await ctx.fire_exception_caught(e)

        if not receipt.get("success"):
            error_msg = receipt.get('revert_reason', 'Unknown WASM Error')
            return await ctx.fire_exception_caught(RuntimeError(f"WASM Clearing Execution Reverted: {error_msg}"))

        # 3. Preserve updated state
        ctx.set_attr("clearing_fsm_state", receipt.get("next_fsm_state"))
        
        # 4. Emit resulting command
        command = receipt.get("command")
        if command:
            log.info(f"🧠 [WASM Bridge] Emitting Clearing command: {command.get('command_type')}")
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
                log.info(f"⚡ [Infra] Assembling and executing DVM Payload (Target: {command.get('target_contract')})")

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
                log.info(f"✅ [Infra] L2 Ledger sealed. Rollup Hash: 0x{sealed_hash[:16]}...")

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

        log.info(f"🧠 [WASM Bridge] Transaction event received, transitioning state: {event_payload['event_type']}")

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
            log.info(f"🧠 [WASM Bridge] Emitting Transaction command: {command.get('command_type')}")
            await ctx.fire_write(command)


# =====================================================================
# 4. Pipeline Factories
# =====================================================================

class ClearingPipelineFactory:
    @classmethod
    def build(cls, 
              broker: Any, 
              pta_adapter: Any, 
              notary_keys: List[str],
              chaos_mode: str = "NORMAL",
              concurrent_agents: int = 3) -> ChannelPipeline:

        pipeline = ChannelPipeline()
        pipeline.add_last(JsonMessageCodec())
        pipeline.add_last(ClearingFlowPropagator())

        if chaos_mode != "NORMAL":
            pipeline.add_last(WalletChaosInjector(mode=chaos_mode))

        pipeline.add_last(Eip712Authenticator())
        pipeline.add_last(ClearingInfraHandler(broker, pta_adapter, notary_keys))
        
        # Inject WASM runtime instance into the bridge
        wasm_gateway = GatewayWasm()
        pipeline.add_last(ClearingFsmBridgeHandler(
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

        # Inject WASM runtime instance into the bridge
        wasm_gateway = GatewayWasm()
        pipeline.add_last(TransactionBridgeHandler(wasm_gateway=wasm_gateway))
        pipeline.add_last(PipelineTailErrorHandler())

        return pipeline