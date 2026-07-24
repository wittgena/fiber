# eco.tenant.call.type
from enum import Enum
from typing import Literal

class CallTypes(str, Enum):
    embedding = "embedding"
    aembedding = "aembedding"
    completion = "completion"
    acompletion = "acompletion"
    atext_completion = "atext_completion"
    text_completion = "text_completion"
    image_generation = "image_generation"
    aimage_generation = "aimage_generation"
    image_edit = "image_edit"
    aimage_edit = "aimage_edit"
    moderation = "moderation"
    amoderation = "amoderation"
    atranscription = "atranscription"
    transcription = "transcription"
    aspeech = "aspeech"
    speech = "speech"
    rerank = "rerank"
    arerank = "arerank"
    search = "search"
    asearch = "asearch"
    arealtime = "_arealtime"
    aresponses_websocket = "_aresponses_websocket"
    create_batch = "create_batch"
    acreate_batch = "acreate_batch"
    aretrieve_batch = "aretrieve_batch"
    retrieve_batch = "retrieve_batch"
    acancel_batch = "acancel_batch"
    cancel_batch = "cancel_batch"
    pass_through = "pass_through_endpoint"
    anthropic_messages = "anthropic_messages"
    get_assistants = "get_assistants"
    aget_assistants = "aget_assistants"
    create_assistants = "create_assistants"
    acreate_assistants = "acreate_assistants"
    delete_assistant = "delete_assistant"
    adelete_assistant = "adelete_assistant"
    acreate_thread = "acreate_thread"
    create_thread = "create_thread"
    aget_thread = "aget_thread"
    get_thread = "get_thread"
    a_add_message = "a_add_message"
    add_message = "add_message"
    aget_messages = "aget_messages"
    get_messages = "get_messages"
    arun_thread = "arun_thread"
    run_thread = "run_thread"
    arun_thread_stream = "arun_thread_stream"
    run_thread_stream = "run_thread_stream"
    afile_retrieve = "afile_retrieve"
    file_retrieve = "file_retrieve"
    afile_delete = "afile_delete"
    file_delete = "file_delete"
    afile_list = "afile_list"
    file_list = "file_list"
    acreate_file = "acreate_file"
    create_file = "create_file"
    afile_content = "afile_content"
    file_content = "file_content"
    create_fine_tuning_job = "create_fine_tuning_job"
    acreate_fine_tuning_job = "acreate_fine_tuning_job"

    # Video Generation Call Types
    create_video = "create_video"
    acreate_video = "acreate_video"
    avideo_retrieve = "avideo_retrieve"
    video_retrieve = "video_retrieve"
    avideo_content = "avideo_content"
    video_content = "video_content"
    video_remix = "video_remix"
    avideo_remix = "avideo_remix"
    video_list = "video_list"
    avideo_list = "avideo_list"
    video_retrieve_job = "video_retrieve_job"
    avideo_retrieve_job = "avideo_retrieve_job"
    video_delete = "video_delete"
    avideo_delete = "avideo_delete"
    video_create_character = "video_create_character"
    avideo_create_character = "avideo_create_character"
    video_get_character = "video_get_character"
    avideo_get_character = "avideo_get_character"
    video_edit = "video_edit"
    avideo_edit = "avideo_edit"
    video_extension = "video_extension"
    avideo_extension = "avideo_extension"
    vector_store_file_create = "vector_store_file_create"
    avector_store_file_create = "avector_store_file_create"
    vector_store_file_list = "vector_store_file_list"
    avector_store_file_list = "avector_store_file_list"
    vector_store_file_retrieve = "vector_store_file_retrieve"
    avector_store_file_retrieve = "avector_store_file_retrieve"
    vector_store_file_content = "vector_store_file_content"
    avector_store_file_content = "avector_store_file_content"
    vector_store_file_update = "vector_store_file_update"
    avector_store_file_update = "avector_store_file_update"
    vector_store_file_delete = "vector_store_file_delete"
    avector_store_file_delete = "avector_store_file_delete"
    vector_store_create = "vector_store_create"
    avector_store_create = "avector_store_create"
    vector_store_search = "vector_store_search"
    avector_store_search = "avector_store_search"

    # Container Call Types
    create_container = "create_container"
    acreate_container = "acreate_container"
    list_containers = "list_containers"
    alist_containers = "alist_containers"
    retrieve_container = "retrieve_container"
    aretrieve_container = "aretrieve_container"
    delete_container = "delete_container"
    adelete_container = "adelete_container"
    list_container_files = "list_container_files"
    alist_container_files = "alist_container_files"
    upload_container_file = "upload_container_file"
    aupload_container_file = "aupload_container_file"

    acancel_fine_tuning_job = "acancel_fine_tuning_job"
    cancel_fine_tuning_job = "cancel_fine_tuning_job"
    alist_fine_tuning_jobs = "alist_fine_tuning_jobs"
    list_fine_tuning_jobs = "list_fine_tuning_jobs"
    aretrieve_fine_tuning_job = "aretrieve_fine_tuning_job"
    retrieve_fine_tuning_job = "retrieve_fine_tuning_job"
    responses = "responses"
    aresponses = "aresponses"
    alist_input_items = "alist_input_items"
    llm_passthrough_route = "llm_passthrough_route"
    allm_passthrough_route = "allm_passthrough_route"

    # Google GenAI Native Call Types
    generate_content = "generate_content"
    agenerate_content = "agenerate_content"
    generate_content_stream = "generate_content_stream"
    agenerate_content_stream = "agenerate_content_stream"

    # OCR Call Types
    ocr = "ocr"
    aocr = "aocr"

    # MCP Call Types
    call_mcp_tool = "call_mcp_tool"
    list_mcp_tools = "list_mcp_tools"

    # A2A Call Types
    asend_message = "asend_message"
    send_message = "send_message"

    ## Claude Code Call Types
    acreate_skill = "acreate_skill"

CallTypesLiteral = Literal[
    "embedding",
    "aembedding",
    "completion",
    "acompletion",
    "atext_completion",
    "text_completion",
    "image_generation",
    "aimage_generation",
    "image_edit",
    "aimage_edit",
    "moderation",
    "amoderation",
    "atranscription",
    "transcription",
    "aspeech",
    "speech",
    "rerank",
    "arerank",
    "search",
    "asearch",
    "_arealtime",
    "_aresponses_websocket",
    "create_batch",
    "acreate_batch",
    "aretrieve_batch",
    "retrieve_batch",
    "acancel_batch",
    "cancel_batch",
    "pass_through_endpoint",
    "anthropic_messages",
    "get_assistants",
    "aget_assistants",
    "create_assistants",
    "acreate_assistants",
    "delete_assistant",
    "adelete_assistant",
    "acreate_thread",
    "create_thread",
    "aget_thread",
    "get_thread",
    "a_add_message",
    "add_message",
    "aget_messages",
    "get_messages",
    "arun_thread",
    "run_thread",
    "arun_thread_stream",
    "run_thread_stream",
    "afile_retrieve",
    "file_retrieve",
    "afile_delete",
    "file_delete",
    "afile_list",
    "file_list",
    "acreate_file",
    "create_file",
    "afile_content",
    "file_content",
    "create_fine_tuning_job",
    "acreate_fine_tuning_job",
    "create_video",
    "acreate_video",
    "avideo_retrieve",
    "video_retrieve",
    "avideo_content",
    "video_content",
    "video_remix",
    "avideo_remix",
    "video_list",
    "avideo_list",
    "video_retrieve_job",
    "avideo_retrieve_job",
    "video_delete",
    "avideo_delete",
    "video_create_character",
    "avideo_create_character",
    "video_get_character",
    "avideo_get_character",
    "video_edit",
    "avideo_edit",
    "video_extension",
    "avideo_extension",
    "vector_store_file_create",
    "avector_store_file_create",
    "vector_store_file_list",
    "avector_store_file_list",
    "vector_store_file_retrieve",
    "avector_store_file_retrieve",
    "vector_store_file_content",
    "avector_store_file_content",
    "vector_store_file_update",
    "avector_store_file_update",
    "vector_store_file_delete",
    "avector_store_file_delete",
    "vector_store_create",
    "avector_store_create",
    "vector_store_search",
    "avector_store_search",
    "create_container",
    "acreate_container",
    "list_containers",
    "alist_containers",
    "retrieve_container",
    "aretrieve_container",
    "delete_container",
    "adelete_container",
    "list_container_files",
    "alist_container_files",
    "upload_container_file",
    "aupload_container_file",
    "acancel_fine_tuning_job",
    "cancel_fine_tuning_job",
    "alist_fine_tuning_jobs",
    "list_fine_tuning_jobs",
    "aretrieve_fine_tuning_job",
    "retrieve_fine_tuning_job",
    "responses",
    "aresponses",
    "alist_input_items",
    "llm_passthrough_route",
    "allm_passthrough_route",
    "generate_content",
    "agenerate_content",
    "generate_content_stream",
    "agenerate_content_stream",
    "ocr",
    "aocr",
    "call_mcp_tool",
    "list_mcp_tools",
    "asend_message",
    "send_message",
    "acreate_skill"
]