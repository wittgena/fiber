# fiber.phase.abc.spec.align.question
@lineage: phase.abc.spec.align.question

The zero-point that aligns Fiber’s core with its surface rests on four self-imposed questions:

* *Can you onboard a new LLM's async streams and tool calls onto your existing codebase using only external JSON rules—without touching a line of Python or redeploying packages?*  
  It eliminates brittle `if/elif` parsers and repeated production redeployments every time a provider shifts its parameters or streaming chunk schema.

* *While hundreds of LLM streams are pouring in, can you physically sever a single stream mid-flight (`HTTP 402`) within 1ms at the exact token chunk its budget runs out?*  
  Post-request billing is too late to stop runaway agent loops or streaming bursts from draining API budgets and triggering OOM crashes in real time.

* *Can you coalesce thousands of raw SSE micro-chunks into 10ms or 100ms time-windows on record, and replay them chunk-for-chunk at either 0ms (`max`) or exact historical pacing (`real`)?*  
  It replaces bloated packet dumps and flaky live API calls with a single deterministic fixture—covering instant CI assertions, real-time latency tests, and mid-stream kill-switch cutoffs.

* *Without touching third-party MCP server code, can you freeze high-risk tool calls for Slack approval before the OS process even spawns (`Zero-Spawn`)—and guarantee single execution across ten agent retries?*  
  It stops untrusted tools from blindly spawning local processes and prevents timeout-induced agent retries from corrupting or wiping out production state.