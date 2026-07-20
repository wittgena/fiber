# xor.opt.manifold.model.cot
## @lineage: xphi.xor.opt.manifold.model.cot
## @lineage: bound.adapter.opt.model.cot
from typing import Any
from pydantic.fields import FieldInfo

from arch.xor.sign.field import InputField, OutputField
from arch.xor.sign.signature import Signature, ensure_signature
from xor.opt.module.meta import Module
from xor.opt.module.prompter import Predict

class ChainOfThought(Module):
    def __init__(
        self,
        signature: str | type[Signature],
        rationale_field: FieldInfo | None = None,
        rationale_field_type: type = str,
        **config: dict[str, Any],
    ):
        super().__init__()
        signature = ensure_signature(signature)
        desc = "${reasoning}"
        rationale_field_type = rationale_field.annotation if rationale_field else rationale_field_type
        rationale_field = rationale_field if rationale_field else OutputField(desc=desc)
        extended_signature = signature.prepend(name="reasoning", field=rationale_field, type_=rationale_field_type)
        self.predict = Predict(extended_signature, **config)

    def forward(self, **kwargs):
        return self.predict(**kwargs)

    async def aforward(self, **kwargs):
        return await self.predict.acall(**kwargs)
