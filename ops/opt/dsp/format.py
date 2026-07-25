# ops.opt.dsp.format
## @lineage: void.extime.opt.sign.format
import os
import hashlib
from pathlib import Path
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, TypedDict
import orjson

CACHEDIR = os.environ.get("DSP_CACHEDIR")

class TrainingStatus(str, Enum):
    not_started = "not_started"
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class TrainDataFormat(str, Enum):
    CHAT = "chat"
    COMPLETION = "completion"
    GRPO_CHAT = "grpo_chat"

class Message(TypedDict):
    role: Literal["user"] | Literal["assistant"] | Literal["system"]
    content: str


class MessageAssistant(TypedDict):
    role: Literal["assistant"]
    content: str


class GRPOChatData(TypedDict):
    messages: list[Message]
    completion: MessageAssistant
    reward: float


class GRPOGroup(TypedDict):
    batch_id: int | None
    group: list[GRPOChatData]

class GRPOStatus(TypedDict):
    job_id: str
    status: str | None = None
    current_model: str
    checkpoints: dict[str, str]
    last_checkpoint: str | None = None
    pending_batch_ids: list[int] = []

def get_finetune_directory() -> str:
    default_finetunedir = os.path.join(CACHEDIR, "finetune")
    finetune_dir = os.environ.get("DSPY_FINETUNEDIR") or default_finetunedir
    finetune_dir = os.path.abspath(finetune_dir)
    os.makedirs(finetune_dir, exist_ok=True)
    return finetune_dir

def save_data(
    data: list[dict[str, Any]],
) -> str:
    data_bytes = orjson.dumps(data, option=orjson.OPT_SORT_KEYS)
    hash_str = hashlib.sha256(data_bytes).hexdigest()[:16]
    file_name = f"{hash_str}.jsonl"
    
    finetune_dir = get_finetune_directory()
    file_path = os.path.join(finetune_dir, file_name)
    file_path = os.path.abspath(file_path)
    with open(file_path, "wb") as f:
        for item in data:
            f.write(orjson.dumps(item) + b"\n")
    return file_path