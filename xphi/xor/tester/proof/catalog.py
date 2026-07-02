# xphi.xor.tester.proof.catalog
## @lineage: anchor.tester.proof.catalog
## @lineage: anchor.tester.catalog

TEST_PROOF_CATALOG = {
    # --- [Cross / General] ---
    "test_hello_world": {"intent": "Verify basic end-to-end connectivity and simple text response generation.", "complexity": "low"},
    "test_remote_conversation_live_server": {"intent": "Verify agent maintains context across a live remote session.", "complexity": "medium"},
    "test_event_loss_repro": {"intent": "Verify no streaming chunks/events are dropped during high-latency transmission.", "complexity": "high"},

    # --- [LLM Core & Configuration] ---
    "test_pydantic_warning_suppression": {"intent": "Verify internal Pydantic serialization warnings are cleanly suppressed.", "complexity": "low"},
    "test_llm": {"intent": "Verify general LLM completion parameters and payload structure.", "complexity": "low"},
    "test_api_key_validation": {"intent": "Verify system accurately catches and reports malformed or missing API keys.", "complexity": "low"},
    "test_llm_litellm_extra_body": {"intent": "Verify custom extra_body parameters are safely passed to the provider.", "complexity": "medium"},
    "test_llm_completion": {"intent": "Verify base completion routing without tool calls or streams.", "complexity": "low"},
    "test_responses_parsing_and_kwargs": {"intent": "Verify provider-specific kwargs are parsed and merged into the response object.", "complexity": "medium"},
    "test_exception_classifier": {"intent": "Verify raw provider errors are correctly mapped to standardized internal exceptions.", "complexity": "medium"},
    "test_exception_mapping": {"intent": "Verify BadRequest and other specific exceptions trigger correct error codes.", "complexity": "medium"},
    "test_llm_fncall_converter": {"intent": "Verify universal tool schema converts correctly to provider-specific formats.", "complexity": "high"},

    # --- [Network & Resilience (Timeout/Retry/Fallback)] ---
    "test_llm_timeout": {"intent": "Verify system triggers a timeout exception when provider delays response.", "complexity": "low"},
    "test_llm_fallback": {"intent": "Verify traffic routes to secondary model on 502/503 errors from primary model.", "complexity": "high"},
    "test_llm_no_response_retry": {"intent": "Verify system retries specified times when receiving empty/None responses.", "complexity": "medium"},
    "test_api_connection_error_retry": {"intent": "Verify exponential backoff and retry logic on APIConnectionError.", "complexity": "medium"},
    "test_llm_retry_telemetry": {"intent": "Verify retry attempts are accurately logged in telemetry.", "complexity": "medium"},

    # --- [Telemetry & Token Usage] ---
    "test_telemetry_policy": {"intent": "Verify telemetry accurately redacts sensitive user data based on policy.", "complexity": "medium"},
    "test_llm_telemetry": {"intent": "Verify prompt and completion tokens are correctly aggregated.", "complexity": "low"},
    "test_llm_log_completions_integration": {"intent": "Verify completion payloads are successfully hooked into the logging system.", "complexity": "medium"},

    # --- [Advanced Reasoning & Tools] ---
    "test_thinking_blocks": {"intent": "Verify o1/Claude 3.5 thinking blocks are successfully separated from the final text.", "complexity": "high"},
    "test_reasoning_content": {"intent": "Verify reasoning tokens are parsed and preserved in the response schema.", "complexity": "medium"},
    "test_message": {"intent": "Verify standard Message object construction and roles.", "complexity": "low"},
    "test_message_tool_call": {"intent": "Verify ToolCall message blocks handle parallel execution schemas.", "complexity": "medium"},
    "test_discriminated_union": {"intent": "Verify robust Pydantic union parsing across varying tool schemas.", "complexity": "medium"},

    # --- [Agent Behavior & Tool Handling] ---
    "test_nonexistent_tool_handling": {"intent": "Verify agent recovers gracefully by injecting an error when calling an invalid tool.", "complexity": "medium"},
    "test_message_while_finishing": {"intent": "Verify agent handles incoming user messages while finalizing its current step.", "complexity": "high"},
    "test_tool_execution_error_handling": {"intent": "Verify agent logic loop doesn't crash when tool execution returns an error trace.", "complexity": "high"},
    "test_tool_call_compatibility": {"intent": "Verify agent supports legacy and latest tool call schema mappings.", "complexity": "medium"},
    "test_agent_step_responses_gating": {"intent": "Verify agent halts action execution if security policy flags the response.", "complexity": "high"},
    "test_security_policy_integration": {"intent": "Verify injected security blocks override dangerous tool calls.", "complexity": "high"},
    "test_tool_validation_error_message": {"intent": "Verify schema validation errors are fed back into the agent prompt.", "complexity": "medium"},
    "test_reasoning_only_responses": {"intent": "Verify agent loop continues correctly if the LLM only returns thinking blocks without tool calls.", "complexity": "high"},
    "test_non_executable_action_emission": {"intent": "Verify agent handles non-executable natural language commands as no-ops.", "complexity": "low"},
    "test_tool_call_recovery": {"intent": "Verify agent successfully completes the task after recovering from an initial tool call failure.", "complexity": "high"},

    # --- [Conversation & Context Condensing] ---
    "test_llm_summarizing_condenser": {"intent": "Verify condenser accurately summarizes long context arrays into dense representations.", "complexity": "high"},
    "test_ask_agent": {"intent": "Verify agent delegates clarification questions to the user when uncertain.", "complexity": "medium"},
    "test_generate_title": {"intent": "Verify context is accurately analyzed to generate a concise conversation title.", "complexity": "low"},
    "test_condense": {"intent": "Verify eviction logic when context window limit is reached.", "complexity": "high"},
    "test_get_unmatched_actions": {"intent": "Verify parsing catches residual actions missing from the primary routing flow.", "complexity": "medium"},
    "test_confirmation_mode": {"intent": "Verify agent blocks high-risk actions pending explicit user confirmation.", "complexity": "high"},
    "test_conversation_pause_functionality": {"intent": "Verify the execution loop safely suspends state when paused.", "complexity": "high"}
}