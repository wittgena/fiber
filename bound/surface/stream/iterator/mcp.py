# bound.surface.stream.iterator.mcp
## @lineage: bound.transport.stream.iterator.mcp
## @lineage: bound.transport.stream.mcp
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, cast
from starlette.datastructures import Headers

from bound.adapter.switch.params import ResponsesAPIResponse, ResponsesAPIStreamingResponse
from bound.surface.legacy.openai.types import OutputItemDoneEvent
from bound.surface.legacy.openai.types import ResponsesAPIStreamEvents, BaseOpenAIResponse, MCPCallCompletedEvent

from bound.bridge.protocol.mcp.handler import MCPHandler
from bound.bridge.protocol.mcp.parser.header import MCPHeaderParser
from bound.bridge.protocol.mcp.parser.payload import MCPPayloadParser
from bound.bridge.protocol.mcp.event.call import create_mcp_call_events
from bound.surface.stream.iterator.response import ResponseStreamIterator

from phase.gov.proto.gate import uuid
from watcher.plane.emitter import get_emitter

log = get_emitter("mcp.stream")

class MCPStreamIterator(ResponseStreamIterator):
    """
    A complete MCP streaming iterator that handles the entire flow:
    1. Immediately emits MCP discovery events
    2. Makes the first LLM call and streams its response
    3. Handles tool execution and follow-up calls for auto-execute tools
    4. Emits tool execution events in the stream
    """

    def __init__(
        self,
        base_iterator: Any,  # Can be None - will be created internally
        mcp_events: List[ResponsesAPIStreamingResponse],
        tool_server_map: dict[str, str],
        mcp_tools_with_litellm_proxy: Optional[List[Any]] = None,
        user_api_key_auth: Any = None,
        original_request_params: Optional[Dict[str, Any]] = None,
    ):
        # MCP setup
        self.mcp_tools_with_litellm_proxy = mcp_tools_with_litellm_proxy or []
        self.user_api_key_auth = user_api_key_auth
        self.original_request_params = original_request_params or {}
        self.should_auto_execute = self._should_auto_execute_tools()

        # Streaming state management
        self.phase = "initial_response"  # initial_response -> mcp_discovery -> tool_execution -> follow_up_response -> finished
        self.finished = False

        # Event queues and generation flags
        self.mcp_discovery_events: List[ResponsesAPIStreamingResponse] = (
            mcp_events  # Pre-generated MCP discovery events
        )
        self.tool_execution_events: List[ResponsesAPIStreamingResponse] = []
        self.mcp_discovery_generated = True  # Events are already generated
        self.mcp_events = (
            mcp_events  # Store the initial MCP events for backward compatibility
        )
        self.tool_server_map = tool_server_map

        # Iterator references
        self.base_iterator: Optional[Union[Any, ResponsesAPIResponse]] = (
            base_iterator  # Will be created when needed
        )
        self.follow_up_iterator: Optional[Any] = None

        # Response collection for tool execution
        self.collected_response: Optional[ResponsesAPIResponse] = None

        # Set up model metadata (will be updated when we get the real iterator)
        self.model = self.original_request_params.get("model", "unknown")
        self.litellm_metadata = {}
        self.custom_llm_provider = self.original_request_params.get(
            "custom_llm_provider", None
        )
        self.call_id = self.original_request_params.get("call_id")
        self.litellm_trace_id = self.original_request_params.get("litellm_trace_id")

        self._extract_mcp_headers_from_params()

        # Mark as async iterator
        self.is_async = True

        # Track if we've emitted initial OpenAI lifecycle events
        self.initial_events_emitted = False

        # Cache the response ID to ensure consistency across all events
        self._cached_response_id: Optional[str] = None

    def _extract_mcp_headers_from_params(self) -> None:
        """Extract MCP headers from original request params to pass to tool calls"""
        # Extract headers from secret_fields in original_request_params
        raw_headers_from_request: Optional[Dict[str, str]] = None
        secret_fields = self.original_request_params.get("secret_fields")
        if secret_fields and isinstance(secret_fields, dict):
            raw_headers_from_request = secret_fields.get("raw_headers")

        # Extract MCP-specific headers
        self.mcp_auth_header: Optional[str] = None
        self.mcp_server_auth_headers: Optional[Dict[str, Dict[str, str]]] = None
        self.oauth2_headers: Optional[Dict[str, str]] = None
        self.raw_headers: Optional[Dict[str, str]] = raw_headers_from_request

        if raw_headers_from_request:
            headers_obj = Headers(raw_headers_from_request)
            self.mcp_auth_header = MCPHeaderParser.get_mcp_auth_headers(headers_obj)
            self.mcp_server_auth_headers = MCPHeaderParser.get_mcp_server_auth_headers(headers_obj)
            self.oauth2_headers = MCPHeaderParser.get_oauth2_headers(headers_obj)

        # Also check if headers are provided in tools array (from request body)
        tools = self.original_request_params.get("tools")
        if tools:
            for tool in tools:
                if isinstance(tool, dict) and tool.get("type") == "mcp":
                    tool_headers = tool.get("headers", {})
                    if tool_headers and isinstance(tool_headers, dict):
                        # Merge tool headers into mcp_server_auth_headers
                        headers_obj_from_tool = Headers(tool_headers)
                        tool_mcp_server_auth_headers = MCPHeaderParser.get_mcp_server_auth_headers(headers_obj_from_tool)

                        if tool_mcp_server_auth_headers:
                            if self.mcp_server_auth_headers is None:
                                self.mcp_server_auth_headers = {}
                            # Merge the headers from tool into existing headers
                            for (
                                server_alias,
                                headers_dict,
                            ) in tool_mcp_server_auth_headers.items():
                                if server_alias not in self.mcp_server_auth_headers:
                                    self.mcp_server_auth_headers[server_alias] = {}
                                self.mcp_server_auth_headers[server_alias].update(
                                    headers_dict
                                )

                        # Also merge raw headers
                        if self.raw_headers is None:
                            self.raw_headers = {}
                        self.raw_headers.update(tool_headers)

    def _should_auto_execute_tools(self) -> bool:
        """Check if tools should be auto-executed"""
        return MCPHandler._should_auto_execute_tools(self.mcp_tools_with_litellm_proxy)

    def __aiter__(self):
        return self

    async def __anext__(self) -> ResponsesAPIStreamingResponse:
        """
        Phase-based streaming:
        1. initial_response - Stream the first LLM response (includes response.created, response.in_progress, response.output_item.added)
        2. mcp_discovery - Emit MCP discovery events (after response.output_item.added)
        3. continue_initial_response - Continue streaming the initial response content
        4. tool_execution - Emit tool execution events
        5. follow_up_response - Stream the follow-up response
        6. finished - End iteration
        """

        # Phase 1: Initial Response Stream (emit standard OpenAI events first)
        if self.phase == "initial_response":
            result = await self._handle_initial_response_phase()
            if result is not None:
                return result

        # Phase 2: MCP Discovery Events (after response.output_item.added)
        if self.phase == "mcp_discovery":
            # Emit MCP discovery events
            if self.mcp_discovery_events:
                return self.mcp_discovery_events.pop(0)
            self.phase = "continue_initial_response"
            # Fall through to continue processing the initial response

        # Phase 3: Continue Initial Response (after MCP discovery events)
        if self.phase == "continue_initial_response":
            try:
                return await self._process_base_iterator_chunk()
            except StopAsyncIteration:
                # Initial response ended, move to next phase
                if self.should_auto_execute and self.collected_response:
                    self.phase = "tool_execution"
                    await self._generate_tool_execution_events()
                else:
                    self.phase = "finished"
                    raise

        # Phase 4: Tool Execution Events
        if self.phase == "tool_execution":
            # Emit any queued tool execution events
            if self.tool_execution_events:
                return self.tool_execution_events.pop(0)

            # Move to follow-up response phase
            self.phase = "follow_up_response"
            await self._create_follow_up_iterator()

        # Phase 5: Follow-up Response Stream
        if self.phase == "follow_up_response":
            if self.follow_up_iterator:
                try:
                    return await cast(Any, self.follow_up_iterator).__anext__()  # type: ignore[attr-defined]
                except StopAsyncIteration:
                    self.phase = "finished"
                    raise
            else:
                self.phase = "finished"
                raise StopAsyncIteration

        # Phase 6: Finished
        if self.phase == "finished":
            raise StopAsyncIteration

        # Should not reach here
        raise StopAsyncIteration

    async def _handle_initial_response_phase(
        self,
    ) -> Optional[ResponsesAPIStreamingResponse]:
        """
        Handle Phase 1: Initial Response Stream.

        Returns a chunk to emit, or None to fall through to the next phase.
        Raises StopAsyncIteration when the stream is exhausted with no auto-execution.
        """
        if self.base_iterator is None:
            await self._create_initial_response_iterator()

        if self.base_iterator is None:
            # LLM call failed — still emit MCP discovery events before finishing
            if self.mcp_discovery_events:
                self.phase = "mcp_discovery"
            else:
                self.phase = "finished"
                raise StopAsyncIteration
            return None

        if self.base_iterator:
            if hasattr(self.base_iterator, "__anext__"):
                try:
                    chunk = await cast(Any, self.base_iterator).__anext__()  # type: ignore[attr-defined]

                    # Capture the response ID from the first event to ensure consistency
                    if self._cached_response_id is None and hasattr(chunk, "response"):
                        response_obj = getattr(chunk, "response", None)
                        if response_obj and hasattr(response_obj, "id"):
                            self._cached_response_id = response_obj.id
                            log.debug(
                                f"Cached response ID: {self._cached_response_id}"
                            )

                    # After emitting response.output_item.added, transition to MCP discovery
                    if not self.initial_events_emitted and hasattr(chunk, "type"):
                        chunk_type = getattr(chunk, "type", None)
                        if chunk_type == ResponsesAPIStreamEvents.OUTPUT_ITEM_ADDED:
                            self.initial_events_emitted = True
                            self.phase = "mcp_discovery"
                            return chunk

                    # If auto-execution is enabled, check for completed responses
                    if self.should_auto_execute and self._is_response_completed(chunk):
                        response_obj = getattr(chunk, "response", None)
                        if isinstance(response_obj, ResponsesAPIResponse):
                            self.collected_response = response_obj
                        self.phase = "tool_execution"
                        await self._generate_tool_execution_events()

                    return chunk
                except StopAsyncIteration:
                    if self.should_auto_execute and self.collected_response:
                        self.phase = "tool_execution"
                        await self._generate_tool_execution_events()
                    else:
                        self.phase = "finished"
                        raise
            else:
                # base_iterator is not async iterable (likely a ResponsesAPIResponse)
                if self.should_auto_execute and isinstance(
                    self.base_iterator, ResponsesAPIResponse
                ):
                    self.collected_response = self.base_iterator
                    self.phase = "tool_execution"
                    await self._generate_tool_execution_events()
                else:
                    self.phase = "finished"
                    raise StopAsyncIteration
        return None

    def _is_response_completed(self, chunk: ResponsesAPIStreamingResponse) -> bool:
        """Check if this chunk indicates the response is completed"""
        return (
            getattr(chunk, "type", None) == ResponsesAPIStreamEvents.RESPONSE_COMPLETED
        )

    async def _process_base_iterator_chunk(self) -> ResponsesAPIStreamingResponse:
        """
        Process a chunk from the base iterator with response ID consistency enforcement.
        """
        if not self.base_iterator or not hasattr(self.base_iterator, "__anext__"):
            raise StopAsyncIteration

        chunk = await cast(Any, self.base_iterator).__anext__()  # type: ignore[attr-defined]

        # Ensure response ID consistency - update chunk if needed
        if self._cached_response_id and hasattr(chunk, "response"):
            response_obj = getattr(chunk, "response", None)
            if response_obj and hasattr(response_obj, "id"):
                if response_obj.id != self._cached_response_id:
                    log.debug(
                        f"Updating response ID from {response_obj.id} to {self._cached_response_id}"
                    )
                    response_obj.id = self._cached_response_id

        # If auto-execution is enabled, check for completed responses
        if self.should_auto_execute and self._is_response_completed(chunk):
            # Collect the response for tool execution
            response_obj = getattr(chunk, "response", None)
            if isinstance(response_obj, ResponsesAPIResponse):
                self.collected_response = response_obj
            # Move to tool execution phase after emitting this chunk
            self.phase = "tool_execution"
            await self._generate_tool_execution_events()

        return chunk

    async def _create_initial_response_iterator(self) -> None:
        from bound.surface.legacy.action.response import aresponses
        """Create the initial response iterator by making the first LLM call"""
        try:
            # Make the initial response API call - but avoid the MCP wrapper
            params = self.original_request_params.copy()
            params["stream"] = True  # Ensure streaming

            # Use the pre-fetched all_tools from original_request_params (no re-processing needed)
            params_for_llm = {}
            for key, value in params.items():
                params_for_llm[key] = (
                    value  # Copy all params as-is since tools are already processed
                )

            tools_count = (
                len(params_for_llm.get("tools", []))
                if params_for_llm.get("tools")
                else 0
            )
            log.debug(f"Making LLM call with {tools_count} tools")
            response = await aresponses(**params_for_llm)

            # Set the base iterator
            if hasattr(response, "__aiter__") or hasattr(response, "__iter__"):
                self.base_iterator = response
                # Copy metadata from the real iterator
                self.model = getattr(response, "model", self.model)
                self.litellm_metadata = getattr(response, "litellm_metadata", {})
                self.custom_llm_provider = getattr(
                    response, "custom_llm_provider", self.custom_llm_provider
                )
                log.debug(
                    f"Created base iterator: {type(self.base_iterator)}"
                )
            else:
                # Non-streaming response - this shouldn't happen but handle it
                log.warning(f"Got non-streaming response: {type(response)}")
                self.base_iterator = None
                self.phase = "finished"

        except Exception as e:
            log.error(f"Error creating initial response iterator: {e}")
            import traceback

            traceback.print_exc()
            self.base_iterator = None
            # Don't set phase to "finished" here — let __anext__ emit any
            # pre-generated MCP discovery events before ending the iteration.

    async def _generate_tool_execution_events(self) -> None:
        """Generate tool execution events and execute tools"""
        if not self.collected_response:
            return

        try:
            # Extract tool calls from the response
            if self.collected_response is not None:
                tool_calls = MCPPayloadParser._extract_tool_calls_from_response(self.collected_response)  # type: ignore[arg-type]
            else:
                tool_calls = []
            if not tool_calls:
                return

            for tool_call in tool_calls:
                (
                    tool_name,
                    tool_arguments,
                    tool_call_id,
                ) = MCPPayloadParser._extract_tool_call_details(tool_call)
                if tool_name and tool_call_id:
                    # Create MCP call events for this tool execution
                    call_events = create_mcp_call_events(
                        tool_name=tool_name,
                        tool_call_id=tool_call_id,
                        arguments=tool_arguments or "{}",  # JSON string with arguments
                        result=None,  # Will be set after execution
                        base_item_id=f"mcp_{uuid.uuid4().hex[:8]}",
                        sequence_start=len(self.tool_execution_events) + 1,
                    )
                    # Add the in_progress and arguments events (not the completed event yet)
                    self.tool_execution_events.extend(call_events[:-1])

            # Execute the tools
            tool_results = await MCPHandler._execute_tool_calls(
                tool_server_map=self.tool_server_map,
                tool_calls=tool_calls,
                user_api_key_auth=self.user_api_key_auth,
                mcp_auth_header=self.mcp_auth_header,
                mcp_server_auth_headers=self.mcp_server_auth_headers,
                oauth2_headers=self.oauth2_headers,
                raw_headers=self.raw_headers,
                call_id=self.call_id,
                litellm_trace_id=self.litellm_trace_id,
            )

            # Create completion events and output_item.done events for tool execution
            for tool_result in tool_results:
                tool_call_id = tool_result.get("tool_call_id", "unknown")
                result_text = tool_result.get("result", "")

                # Find matching tool name and arguments
                tool_name = "unknown"
                tool_arguments = "{}"
                for tool_call in tool_calls:
                    (
                        name,
                        args,
                        call_id,
                    ) = MCPPayloadParser._extract_tool_call_details(tool_call)
                    if call_id == tool_call_id:
                        tool_name = name or "unknown"
                        tool_arguments = args or "{}"
                        break

                item_id = f"mcp_{uuid.uuid4().hex[:8]}"

                # Create the completion event
                completed_event = MCPCallCompletedEvent(
                    type=ResponsesAPIStreamEvents.MCP_CALL_COMPLETED,
                    sequence_number=len(self.tool_execution_events) + 1,
                    item_id=item_id,
                    output_index=0,
                )
                self.tool_execution_events.append(completed_event)
                output_item_done_event = OutputItemDoneEvent(
                    type=ResponsesAPIStreamEvents.OUTPUT_ITEM_DONE,
                    output_index=0,
                    item=BaseOpenAIResponse(
                        **{
                            "id": item_id,
                            "type": "mcp_call",
                            "approval_request_id": f"mcpr_{uuid.uuid4().hex[:8]}",
                            "arguments": tool_arguments,
                            "error": None,
                            "name": tool_name,
                            "output": result_text,
                            "server_label": "litellm",  # or extract from tool config
                        }
                    ),
                )
                self.tool_execution_events.append(output_item_done_event)

            # Store tool results for follow-up call
            self.tool_results = tool_results

        except Exception as e:
            log.error(f"Error in tool execution: {e}")
            import traceback

            traceback.print_exc()
            self.tool_results = []

    async def _create_follow_up_iterator(self) -> None:
        from bound.surface.legacy.action.response import aresponses
        """Create the follow-up response iterator with tool results"""
        if not self.collected_response or not hasattr(self, "tool_results"):
            return

        try:
            # Create follow-up input
            if self.collected_response is not None:
                follow_up_input = MCPPayloadParser._create_follow_up_input(
                    response=self.collected_response,  # type: ignore[arg-type]
                    tool_results=self.tool_results,
                    original_input=self.original_request_params.get("input"),
                )

                # Make follow-up call with streaming
                follow_up_params = self.original_request_params.copy()
                follow_up_params.update(
                    {
                        "input": follow_up_input,
                        "stream": True,
                    }
                )
            else:
                return
            # Remove tool_choice to avoid forcing more tool calls
            follow_up_params.pop("tool_choice", None)

            follow_up_response = await aresponses(**follow_up_params)

            # Set up the follow-up iterator
            if hasattr(follow_up_response, "__aiter__"):
                self.follow_up_iterator = follow_up_response

        except Exception as e:
            log.error(f"Error creating follow-up iterator: {e}")
            import traceback

            traceback.print_exc()
            self.follow_up_iterator = None

    def __iter__(self):
        return self

    def __next__(self) -> ResponsesAPIStreamingResponse:
        # First, emit any queued MCP events
        if self.mcp_events:  # type: ignore[attr-defined]
            return self.mcp_events.pop(0)  # type: ignore[attr-defined]

        # Then delegate to the base iterator
        if not self.is_async:
            try:
                if self.base_iterator and hasattr(self.base_iterator, "__next__"):
                    return next(cast(Any, self.base_iterator))  # type: ignore[arg-type]
                else:
                    raise StopIteration
            except StopIteration:
                self.finished = True
                raise
        else:
            raise RuntimeError("Cannot use sync iteration on async iterator")
