# xor.opt.dsp.base
## @lineage: anchor.dsp.training.base
## @lineage: bound.surface.dsp.training.base
## @lineage: bound.surface.client.dsp.training.base
## @lineage: anchor.provider.training.base
## @lineage: anchor.registry.provider.training.base
## @lineage: anchor.provider.dsp.training.base
## @lineage: xphi.xor.dsp.training.base
## @lineage: anchor.model.dsp.training.base
## @lineage: anchor.model.dsp.base
## @lineage: anchor.provider.dsp.base
from abc import abstractmethod
from concurrent.futures import Future
from threading import Thread
from typing import TYPE_CHECKING, Any
from xor.opt.dsp.format import TrainDataFormat

class TrainingJob(Future):
    def __init__(
        self,
        thread: Thread | None = None,
        model: str | None = None,
        train_data: list[dict[str, Any]] | None = None,
        train_data_format: TrainDataFormat | None = None,
        train_kwargs: dict[str, Any] | None = None,
    ):
        self.thread = thread
        self.model = model
        self.train_data = train_data
        self.train_data_format = train_data_format
        self.train_kwargs = train_kwargs or {}
        super().__init__()

    def cancel(self) -> bool:
        super().cancel()

    @abstractmethod
    def status(self) -> Any:
        raise NotImplementedError

class ReinforceJob:
    def __init__(self, lm: "LM", train_kwargs: dict[str, Any] | None = None):
        self.lm = lm
        self.train_kwargs = train_kwargs or {}
        self.checkpoints = {}
        self.last_checkpoint = None

    @abstractmethod
    def initialize(self):
        raise NotImplementedError

    @abstractmethod
    def step(self, train_data: list[dict[str, Any]], train_data_format: TrainDataFormat | str | None = None):
        raise NotImplementedError

    @abstractmethod
    def terminate(self):
        raise NotImplementedError

    @abstractmethod
    def save_checkpoint(self, checkpoint_name: str):
        raise NotImplementedError

    def cancel(self):
        raise NotImplementedError

    def status(self) -> Any:
        raise NotImplementedError


class Provider:
    def __init__(self):
        self.finetunable = False
        self.reinforceable = False
        self.TrainingJob = TrainingJob
        self.ReinforceJob = ReinforceJob

    @staticmethod
    def is_provider_model(model: str) -> bool:
        return False

    @staticmethod
    def launch(lm: "LM", launch_kwargs: dict[str, Any] | None = None):
        pass

    @staticmethod
    def kill(lm: "LM", launch_kwargs: dict[str, Any] | None = None):
        pass

    @staticmethod
    def finetune(
        job: TrainingJob,
        model: str,
        train_data: list[dict[str, Any]],
        train_data_format: TrainDataFormat | str | None,
        train_kwargs: dict[str, Any] | None = None,
    ) -> str:
        raise NotImplementedError
