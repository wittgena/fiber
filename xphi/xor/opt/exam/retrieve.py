# xphi.xor.opt.exam.retrieve
## @lineage: xphi.opt.exam.retrieve
## @lineage: bound.xor.exam.retrieve
## @lineage: xor.exam.retrieve
## @lineage: anchor.xor.exam.retrieve
## @lineage: meta.xor.adapter.exam.retrieve
## @lineage: xor.adapter.exam.retrieve
## @lineage: xor.dsp.adapter.exam.retrieve
import random
from xphi.xor.opt.manifold.parameter import Parameter
from xphi.xor.opt.exam.prediction import Prediction
from xphi.xor.dsp.handler.stream.callback import with_callbacks

def single_query_passage(passages):
    passages_dict = {key: [] for key in list(passages[0].keys())}
    for docs in passages:
        for key, value in docs.items():
            passages_dict[key].append(value)
    if "long_text" in passages_dict:
        passages_dict["passages"] = passages_dict.pop("long_text")
    return Prediction(**passages_dict)


class Retrieve(Parameter):
    name = "Search"
    input_variable = "query"
    desc = "takes a search query and returns one or more potentially relevant passages from a corpus"

    def __init__(self, k=3, callbacks=None):
        self.stage = random.randbytes(8).hex()
        self.k = k
        self.callbacks = callbacks or []

    def reset(self):
        pass

    def dump_state(self):
        state_keys = ["k"]
        return {k: getattr(self, k) for k in state_keys}

    def load_state(self, state):
        for name, value in state.items():
            setattr(self, name, value)

    @with_callbacks
    def __call__(self, *args, **kwargs):
        return self.forward(*args, **kwargs)

    def forward(
        self,
        query: str,
        k: int | None = None,
        **kwargs,
    ) -> list[str] | Prediction | list[Prediction]:
        k = k if k is not None else self.k
        if not settings.rm:
            raise AssertionError("No RM is loaded.")

        passages = settings.rm(query, k=k, **kwargs)

        from collections.abc import Iterable
        if not isinstance(passages, Iterable):
            passages = [passages]
        passages = [psg.long_text for psg in passages]

        return Prediction(passages=passages)

# TODO: Consider doing Prediction.from_completions with the individual sets of passages (per query) too.
