# xphi.xor.opt.module.prompter
## @lineage: xphi.xor.module.prompter
## @lineage: xphi.xor.opt.prompter
import random
from typing import Any, Literal, get_args, get_origin
from pydantic import BaseModel
from pydantic_core import PydanticUndefined
from typeguard import TypeCheckError, check_type

from anchor.dsp.base import BaseLM
from anchor.dsp.instance import DSPInstance
from bound.adapter.dsp.signature import SignatureAdapter

from xphi.scope.dsp.context import runtime
from xphi.xor.opt.manifold.parameter import Parameter
from xphi.xor.opt.callback.base import BaseCallback
from xphi.xor.opt.module.meta import Module

from arch.xor.sample import Prediction
from arch.xor.sign.signature import Signature, ensure_signature
from arch.gov.gate import uuid4
from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)
UNSAFE_LM_STATE_KEYS = {"api_base", "base_url", "model_list"}
IS_TYPE_UNDEFINED = "IS_TYPE_UNDEFINED"

def _sanitize_lm_state(lm_state: dict, allow_unsafe_lm_state: bool) -> dict:
    if allow_unsafe_lm_state:
        return lm_state

    unsafe_keys = sorted(UNSAFE_LM_STATE_KEYS.intersection(lm_state))
    if not unsafe_keys:
        return lm_state

    sanitized_lm_state = {k: v for k, v in lm_state.items() if k not in UNSAFE_LM_STATE_KEYS}
    log.warning(
        "Ignoring unsafe LM config key(s) during state load: %s. "
        "Pass allow_unsafe_lm_state=True to preserve these keys for trusted files.",
        unsafe_keys,
    )
    return sanitized_lm_state

class Predict(Module, Parameter):
    def __init__(self, signature: str | type[Signature], callbacks: list[BaseCallback] | None = None, **config):
        super().__init__(callbacks=callbacks)
        self.stage = random.randbytes(8).hex()
        self.signature = ensure_signature(signature)
        self.config = config
        self.reset()

    def reset(self):
        self.lm = None
        self.traces = []
        self.train = []
        self.demos = []

    def dump_state(self, json_mode=True):
        state_keys = ["traces", "train"]
        state = {k: getattr(self, k) for k in state_keys}

        state["demos"] = []
        for demo in self.demos:
            demo = demo.copy()
            for field in demo:
                demo[field] = serialize_object(demo[field])

            if isinstance(demo, dict) or not json_mode:
                state["demos"].append(demo)
            else:
                state["demos"].append(demo.toDict())

        state["signature"] = self.signature.dump_state()
        state["lm"] = self.lm.dump_state() if self.lm else None
        return state

    def load_state(self, state: dict, *, allow_unsafe_lm_state: bool = False) -> "Predict":
        """Load the saved state of a `Predict` object"""
        excluded_keys = ["signature", "extended_signature", "lm"]
        for name, value in state.items():
            if name not in excluded_keys:
                setattr(self, name, value)

        self.signature = self.signature.load_state(state["signature"])
        sanitized_lm_state = _sanitize_lm_state(state["lm"], allow_unsafe_lm_state) if state["lm"] else None
        self.lm = DSPInstance(**sanitized_lm_state) if sanitized_lm_state else None

        if "extended_signature" in state:
            raise NotImplementedError("Loading extended_signature is no longer supported")

        return self

    def _get_positional_args_error_message(self):
        input_fields = list(self.signature.input_fields.keys())
        return (
            "Positional arguments are not allowed when calling `Predict`, must use keyword arguments "
            f"that match your signature input fields: '{', '.join(input_fields)}'. For example: "
            f"`predict({input_fields[0]}=input_value, ...)`."
        )

    def __call__(self, *args, **kwargs):
        if args:
            raise ValueError(self._get_positional_args_error_message())

        return super().__call__(**kwargs)

    async def acall(self, *args, **kwargs):
        if args:
            raise ValueError(self._get_positional_args_error_message())

        return await super().acall(**kwargs)

    def _forward_preprocess(self, **kwargs):
        ## Extract the three privileged keyword arguments.
        assert "new_signature" not in kwargs, "new_signature is no longer a valid keyword argument."
        signature = ensure_signature(kwargs.pop("signature", self.signature))
        demos = kwargs.pop("demos", self.demos)
        config = {**self.config, **kwargs.pop("config", {})}

        lm = kwargs.pop("lm", self.lm) or runtime.lm
        if lm is None:
            raise ValueError(
                "No LM is loaded. Please configure the LM using `settings.configure(lm=settings.LM(...))`. e.g, "
                "`settings.configure(lm=settings.LM('openai/gpt-4o-mini'))`"
            )

        if isinstance(lm, str):
            raise ValueError(
                f"LM must be an instance of `BaseLM`, not a string. Instead of using a string like "
                f"'settings.configure(lm=\"{lm}\")', please configure the LM like 'settings.configure(lm=settings.LM(\"{lm}\"))'"
            )
        elif not isinstance(lm, BaseLM):
            raise ValueError(f"LM must be an instance of `BaseLM`, not {type(lm)}. Received `lm={lm}`.")

        ## If temperature is unset or <=0.15, and n > 1, set temperature to 0.7 to keep randomness.
        temperature = config.get("temperature") or lm.kwargs.get("temperature")
        num_generations = config.get("n") or lm.kwargs.get("n") or lm.kwargs.get("num_generations") or 1

        if (temperature is None or temperature <= 0.15) and num_generations > 1:
            config["temperature"] = 0.7

        if "prediction" in kwargs:
            if (
                isinstance(kwargs["prediction"], dict)
                and kwargs["prediction"].get("type") == "content"
                and "content" in kwargs["prediction"]
            ):
                # If the `prediction` is the standard predicted outputs format
                # (https://platform.openai.com/docs/guides/predicted-outputs), we remove it from input kwargs and add it
                # to the lm kwargs.
                config["prediction"] = kwargs.pop("prediction")

        ## Populate default values for missing input fields.
        for k, v in signature.input_fields.items():
            if k not in kwargs and v.default is not PydanticUndefined:
                kwargs[k] = v.default

        # Check and warn for extra fields not in signature
        extra_fields = [k for k in kwargs if k not in signature.input_fields]
        if extra_fields:
            log.warning(
                "Input contains fields not in signature. These fields will be ignored: %s. "
                "Expected fields: %s.",
                extra_fields,
                list(signature.input_fields.keys()),
            )

        # Validate input field types match signature
        if runtime.warn_on_type_mismatch:
            for field_name, field_info in signature.input_fields.items():
                if field_name in kwargs:
                    value = kwargs[field_name]
                    expected_type: type = field_info.annotation

                    if value is None or field_info.json_schema_extra.get(IS_TYPE_UNDEFINED, False):
                        continue

                    if not _is_value_compatible_with_type(value, expected_type):
                        log.warning(
                            "Type mismatch for field '%s': expected %s based on given Signature, "
                            "but the provided value is incompatible: %s.",
                            field_name,
                            _get_type_name(expected_type),
                            value,
                        )

        if not all(k in kwargs for k in signature.input_fields):
            present = [k for k in signature.input_fields if k in kwargs]
            missing = [k for k in signature.input_fields if k not in kwargs]
            log.warning(
                "Not all input fields were provided to module. Present: %s. Missing: %s.",
                present,
                missing,
            )
        return lm, config, signature, demos, kwargs

    def _forward_postprocess(self, completions, signature, **kwargs):
        pred = Prediction.from_completions(completions, signature=signature)
        if kwargs.pop("_trace", True) and runtime.trace is not None and runtime.max_trace_size > 0:
            trace = runtime.trace
            if len(trace) >= runtime.max_trace_size:
                trace.pop(0)
            trace.append((self, {**kwargs}, pred))
        return pred

    def _should_stream(self):
        stream_listeners = runtime.stream_listeners or []
        should_stream = runtime.send_stream is not None
        if should_stream and len(stream_listeners) > 0:
            should_stream = any(stream_listener.predict == self for stream_listener in stream_listeners)

        return should_stream

    def forward(self, **kwargs):
        req_id = str(uuid4())[:8]
        log.debug(f"[Predict-{req_id}] 🚀 forward START | signature={self.signature.__class__.__name__}")
        
        lm, config, signature, demos, kwargs = self._forward_preprocess(**kwargs)
        log.debug(f"[Predict-{req_id}] ⚙️ Preprocess complete. LM: {type(lm).__name__}, Stream: {self._should_stream()}")
        adapter = runtime.adapter or SignatureAdapter()

        if self._should_stream():
            log.debug(f"[Predict-{req_id}] 🌊 Executing STREAMING adapter call...")
            with runtime.bind(caller_predict=self):
                completions = adapter(lm, lm_kwargs=config, signature=signature, demos=demos, inputs=kwargs)
        else:
            log.debug(f"[Predict-{req_id}] ⚡ Executing STANDARD adapter call...")
            with runtime.bind(send_stream=None):
                completions = adapter(lm, lm_kwargs=config, signature=signature, demos=demos, inputs=kwargs)

        log.debug(f"[Predict-{req_id}] ✅ Adapter call completed. Initiating postprocess...")
        result = self._forward_postprocess(completions, signature, **kwargs)
        
        log.debug(f"[Predict-{req_id}] 🏁 forward END")
        return result

    async def aforward(self, **kwargs):
        req_id = str(uuid4())[:8]
        log.debug(f"[Predict-{req_id}] 🚀 aforward START | signature={self.signature.__class__.__name__}")
        
        lm, config, signature, demos, kwargs = self._forward_preprocess(**kwargs)
        log.debug(f"[Predict-{req_id}] ⚙️ Preprocess complete. LM: {type(lm).__name__}, Stream: {self._should_stream()}")

        adapter = runtime.adapter or SignatureAdapter()
        if self._should_stream():
            log.debug(f"[Predict-{req_id}] 🌊 Executing ASYNC STREAMING adapter call...")
            with runtime.bind(caller_predict=self):
                completions = await adapter.acall(lm, lm_kwargs=config, signature=signature, demos=demos, inputs=kwargs)
        else:
            log.debug(f"[Predict-{req_id}] ⚡ Executing ASYNC STANDARD adapter call...")
            with runtime.bind(send_stream=None):
                completions = await adapter.acall(lm, lm_kwargs=config, signature=signature, demos=demos, inputs=kwargs)

        log.debug(f"[Predict-{req_id}] ✅ Adapter acall completed. Initiating postprocess...")
        result = self._forward_postprocess(completions, signature, **kwargs)
        
        log.debug(f"[Predict-{req_id}] 🏁 aforward END")
        return result

    def update_config(self, **kwargs):
        self.config = {**self.config, **kwargs}

    def get_config(self):
        return self.config

    def __repr__(self):
        return f"{self.__class__.__name__}({self.signature})"

def _get_type_name(type_annotation) -> str:
    """Helper method to get the name for a type annotation."""

    origin = get_origin(type_annotation)
    args = get_args(type_annotation)

    if origin is None:
        # Primitives like str, int, etc.
        if hasattr(type_annotation, "__name__"):
            return type_annotation.__name__
        return str(type_annotation)

    # Handle Literal types
    if origin is Literal:
        literal_values = ", ".join(repr(arg) for arg in args)
        return f"Literal[{literal_values}]"

    # Types like list[str], dict[str, int], generics, etc.
    if args:
        # Handle Ellipsis in tuples (e.g., tuple[int, ...])
        args_str = ", ".join("..." if arg is ... else _get_type_name(arg) for arg in args)
        origin_name = getattr(origin, "__name__", str(origin))
        return f"{origin_name}[{args_str}]"

    return getattr(origin, "__name__", str(origin))

def _is_value_compatible_with_type(value: Any, expected: type) -> bool:
    """Return True if the value matches the expected type hint."""
    try:
        if expected is str and isinstance(value, list):
            if all(isinstance(item, str) for item in value):
                return True

        check_type(value, expected)
        return True
    except TypeCheckError:
        return False

def serialize_object(obj):
    """@desc: Recursively serialize a given object into a JSON-compatible format"""
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    elif isinstance(obj, list):
        return [serialize_object(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(serialize_object(item) for item in obj)
    elif isinstance(obj, dict):
        return {key: serialize_object(value) for key, value in obj.items()}
    else:
        return obj