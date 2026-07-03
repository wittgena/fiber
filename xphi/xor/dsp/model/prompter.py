# xphi.xor.dsp.model.prompter
## @lineage: xphi.reflect.dsp.model.prompter
## @lineage: xphi.opt.dsp.model.prompter
## @lineage: bound.xor.dsp.model.prompter
## @lineage: xor.dsp.model.prompter
## @lineage: meta.xor.opt.model.prompter
from typing import Any
from xphi.xor.opt.exam.example import Example
from xphi.xor.opt.module.meta import Module

class Prompter:
    def __init__(self):
        pass

    def compile(self, student: Module, *, trainset: list[Example], teacher: Module | None = None, valset: list[Example] | None = None, **kwargs) -> Module:
        raise NotImplementedError

    def get_params(self) -> dict[str, Any]:
        return self.__dict__
