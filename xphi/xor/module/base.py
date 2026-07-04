# xphi.xor.opt.manifold.module.base
## @lineage: xphi.xor.opt.module.base
import sys
import copy
import logging
from collections import deque
from collections.abc import Generator
from pathlib import Path
import cloudpickle
import orjson

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

def get_dependency_versions():
    cloudpickle_version = ".".join(cloudpickle.__version__.split(".")[:2])
    return {
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "spi": __version__,
        "cloudpickle": cloudpickle_version,
    }

class BaseModule:
    def __init__(self):
        pass

    def named_parameters(self):
        """
        Unlike PyTorch, handles (non-recursive) lists of parameters too.
        """

        from xphi.xor.opt.manifold.parameter import Parameter

        visited = set()
        named_parameters = []

        def add_parameter(param_name, param_value):
            if isinstance(param_value, Parameter):
                if id(param_value) not in visited:
                    visited.add(id(param_value))
                    named_parameters.append((param_name, param_value))

            # [수정됨] Module 대신 자기 자신인 BaseModule을 기준으로 타입 검사
            elif isinstance(param_value, BaseModule):
                # When a sub-module is pre-compiled, keep it frozen.
                if not getattr(param_value, "_compiled", False):
                    for sub_name, param in param_value.named_parameters():
                        add_parameter(f"{param_name}.{sub_name}", param)

        if isinstance(self, Parameter):
            add_parameter("self", self)

        for name, value in self.__dict__.items():
            if isinstance(value, Parameter):
                add_parameter(name, value)

            # [수정됨] Module 대신 자기 자신인 BaseModule을 기준으로 타입 검사
            elif isinstance(value, BaseModule):
                # When a sub-module is pre-compiled, keep it frozen.
                if not getattr(value, "_compiled", False):
                    for sub_name, param in value.named_parameters():
                        add_parameter(f"{name}.{sub_name}", param)

            elif isinstance(value, (list, tuple)):
                for idx, item in enumerate(value):
                    add_parameter(f"{name}[{idx}]", item)

            elif isinstance(value, dict):
                for key, item in value.items():
                    add_parameter(f"{name}['{key}']", item)

        return named_parameters

    def named_sub_modules(self, type_=None, skip_compiled=False) -> Generator[tuple[str, "BaseModule"], None, None]:
        """Find all sub-modules in the module, as well as their names.

        Say `self.children[4]['key'].sub_module` is a sub-module. Then the name will be
        `children[4]['key'].sub_module`. But if the sub-module is accessible at different
        paths, only one of the paths will be returned.
        """
        if type_ is None:
            type_ = BaseModule

        queue = deque([("self", self)])
        seen = {id(self)}

        def add_to_queue(name, item):
            if id(item) not in seen:
                seen.add(id(item))
                queue.append((name, item))

        while queue:
            name, item = queue.popleft()

            if isinstance(item, type_):
                yield name, item

            if isinstance(item, BaseModule):
                if skip_compiled and getattr(item, "_compiled", False):
                    continue
                for sub_name, sub_item in item.__dict__.items():
                    add_to_queue(f"{name}.{sub_name}", sub_item)

            elif isinstance(item, (list, tuple)):
                for i, sub_item in enumerate(item):
                    add_to_queue(f"{name}[{i}]", sub_item)

            elif isinstance(item, dict):
                for key, sub_item in item.items():
                    add_to_queue(f"{name}[{key}]", sub_item)

    def parameters(self):
        return [param for _, param in self.named_parameters()]

    def deepcopy(self):
        """Deep copy the module.

        This is a tweak to the default python deepcopy that only deep copies `self.parameters()`, and for other
        attributes, we just do the shallow copy.
        """
        try:
            # If the instance itself is copyable, we can just deep copy it.
            # Otherwise we will have to create a new instance and copy over the attributes one by one.
            return copy.deepcopy(self)
        except Exception:
            pass

        # Create an empty instance.
        new_instance = self.__class__.__new__(self.__class__)
        # Set attribuetes of the copied instance.
        for attr, value in self.__dict__.items():
            if isinstance(value, BaseModule):
                setattr(new_instance, attr, value.deepcopy())
            else:
                try:
                    # Try to deep copy the attribute
                    setattr(new_instance, attr, copy.deepcopy(value))
                except Exception:
                    logging.warning(
                        f"Failed to deep copy attribute '{attr}' of {self.__class__.__name__}, "
                        "falling back to shallow copy or reference copy."
                    )
                    try:
                        # Fallback to shallow copy if deep copy fails
                        setattr(new_instance, attr, copy.copy(value))
                    except Exception:
                        # If even the shallow copy fails, we just copy over the reference.
                        setattr(new_instance, attr, value)

        return new_instance

    def reset_copy(self):
        """Deep copy the module and reset all parameters."""
        new_instance = self.deepcopy()

        for param in new_instance.parameters():
            param.reset()

        return new_instance

    def dump_state(self, json_mode=True):
        return {name: param.dump_state(json_mode=json_mode) for name, param in self.named_parameters()}

    def load_state(self, state, *, allow_unsafe_lm_state=False):
        from xphi.xor.module.prompter import Predict

        for name, param in self.named_parameters():
            if isinstance(param, Predict):
                param.load_state(state[name], allow_unsafe_lm_state=allow_unsafe_lm_state)
            else:
                param.load_state(state[name])

    def save(self, path, save_program=False, modules_to_serialize=None):
        metadata = {}
        metadata["dependency_versions"] = get_dependency_versions()
        path = Path(path)

        if save_program:
            if path.suffix:
                raise ValueError(
                    f"`path` must point to a directory without a suffix when `save_program=True`, but received: {path}"
                )
            if path.exists() and not path.is_dir():
                raise NotADirectoryError(f"The path '{path}' exists but is not a directory.")

            if not path.exists():
                # Create the directory (and any parent directories)
                path.mkdir(parents=True)
            log.warning("Loading untrusted .pkl files can run arbitrary code, which may be dangerous. To avoid "
                           'this, prefer saving using json format using module.save("module.json").')
            try:
                modules_to_serialize = modules_to_serialize or []
                for module in modules_to_serialize:
                    cloudpickle.register_pickle_by_value(module)

                with open(path / "program.pkl", "wb") as f:
                    cloudpickle.dump(self, f)
            except Exception as e:
                raise RuntimeError(
                    f"Saving failed with error: {e}. Please remove the non-picklable attributes from your Spi program, "
                    "or consider using state-only saving by setting `save_program=False`."
                )
            with open(path / "metadata.json", "wb") as f:
                f.write(orjson.dumps(metadata, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))

            return

        if path.suffix == ".json":
            state = self.dump_state()
            state["metadata"] = metadata
            try:
                with open(path, "wb") as f:
                    f.write(orjson.dumps(state, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE))
            except Exception as e:
                raise RuntimeError(
                    f"Failed to save state to {path} with error: {e}. Your Spi program may contain non "
                    "json-serializable objects, please consider saving the state in .pkl by using `path` ending "
                    "with `.pkl`, or saving the whole program by setting `save_program=True`."
                )
        elif path.suffix == ".pkl":
            log.warning("Loading untrusted .pkl files can run arbitrary code, which may be dangerous. To avoid "
                           'this, prefer saving using json format using module.save("module.json").')
            state = self.dump_state(json_mode=False)
            state["metadata"] = metadata
            with open(path, "wb") as f:
                cloudpickle.dump(state, f)
        else:
            raise ValueError(f"`path` must end with `.json` or `.pkl` when `save_program=False`, but received: {path}")

    def load(self, path, allow_pickle=False, allow_unsafe_lm_state=False):
        path = Path(path)

        if path.suffix == ".json":
            with open(path, "rb") as f:
                state = orjson.loads(f.read())
        elif path.suffix == ".pkl":
            if not allow_pickle:
                raise ValueError("Loading .pkl files can run arbitrary code, which may be dangerous. Prefer "
                                 "saving with .json files if possible. Set `allow_pickle=True` "
                                 "if you are sure about the source of the file and in a trusted environment.")
            with open(path, "rb") as f:
                state = cloudpickle.load(f)
        else:
            raise ValueError(f"`path` must end with `.json` or `.pkl`, but received: {path}")

        dependency_versions = get_dependency_versions()
        saved_dependency_versions = state["metadata"]["dependency_versions"]
        for key, saved_version in saved_dependency_versions.items():
            if dependency_versions[key] != saved_version:
                log.warning(
                    f"There is a mismatch of {key} version between saved model and current environment. "
                    f"You saved with `{key}=={saved_version}`, but now you have "
                    f"`{key}=={dependency_versions[key]}`. This might cause errors or performance downgrade "
                    "on the loaded model, please consider loading the model in the same environment as the "
                    "saving environment."
                )
        self.load_state(state, allow_unsafe_lm_state=allow_unsafe_lm_state)