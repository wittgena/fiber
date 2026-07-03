# xphi.xor.opt.prompter.fewshot
## @lineage: xphi.reflect.dsp.opt.fewshot
import random
import threading
import tqdm

from bound.channel.compat.switch.dsp.settings import settings
from xphi.xor.dsp.model.prompter import Prompter
from xphi.xor.opt.exam.example import Example
from xphi.xor.dsp.hasher import Hasher

from watcher.plane.emitter import get_emitter

log = get_emitter(__name__)

class LabeledFewShot(Prompter):
    def __init__(self, k=16):
        self.k = k

    def compile(self, student, *, trainset, sample=True):
        self.student = student.reset_copy()
        self.trainset = trainset

        if len(self.trainset) == 0:
            return self.student

        rng = random.Random(0)

        for predictor in self.student.predictors():
            if sample:
                predictor.demos = rng.sample(self.trainset, min(self.k, len(self.trainset)))
            else:
                predictor.demos = self.trainset[: min(self.k, len(self.trainset))]

        return self.student

class BootstrapFewShot(Prompter):
    def __init__(
        self,
        metric=None,
        metric_threshold=None,
        teacher_settings: dict | None = None,
        max_bootstrapped_demos=4,
        max_labeled_demos=16,
        max_rounds=1,
        max_errors=None,
    ):
        self.metric = metric
        self.metric_threshold = metric_threshold
        self.teacher_settings = {} if teacher_settings is None else teacher_settings

        self.max_bootstrapped_demos = max_bootstrapped_demos
        self.max_labeled_demos = max_labeled_demos
        self.max_rounds = max_rounds
        self.max_errors = max_errors
        self.error_count = 0
        self.error_lock = threading.Lock()

    def compile(self, student, *, teacher=None, trainset):
        self.trainset = trainset

        self._prepare_student_and_teacher(student, teacher)
        self._prepare_predictor_mappings()
        self._bootstrap()

        self.student = self._train()
        self.student._compiled = True

        return self.student

    def _prepare_student_and_teacher(self, student, teacher):
        self.student = student.reset_copy()

        # NOTE: behavior change on Oct 28, 2024. Deep copy instead of reset copy for the student-as-teacher.
        self.teacher = teacher.deepcopy() if teacher is not None else student.deepcopy()

        assert getattr(self.student, "_compiled", False) is False, "Student must be uncompiled."

        if self.max_labeled_demos and getattr(self.teacher, "_compiled", False) is False:
            teleprompter = LabeledFewShot(k=self.max_labeled_demos)
            self.teacher = teleprompter.compile(self.teacher.reset_copy(), trainset=self.trainset)

    def _prepare_predictor_mappings(self):
        name2predictor, predictor2name = {}, {}
        student, teacher = self.student, self.teacher

        assert len(student.predictors()) == len(
            teacher.predictors(),
        ), "Student and teacher must have the same number of predictors."

        for (name1, predictor1), (name2, predictor2) in zip(
            student.named_predictors(), teacher.named_predictors(), strict=False
        ):
            assert name1 == name2, "Student and teacher must have the same program structure."
            if hasattr(predictor1.signature, "equals"):
                assert predictor1.signature.equals(
                    predictor2.signature,
                ), (
                    f"Student and teacher must have the same signatures. "
                    f"{type(predictor1.signature)} != {type(predictor2.signature)}"
                )
            else:
                # fallback in case if .equals is not implemented (e.g. dsp.Prompt)
                assert predictor1.signature == predictor2.signature, (
                    f"Student and teacher must have the same signatures. "
                    f"{type(predictor1.signature)} != {type(predictor2.signature)}"
                )
            assert id(predictor1) != id(predictor2), "Student and teacher must be different objects."

            name2predictor[name1] = None  # dict(student=predictor1, teacher=predictor2)
            predictor2name[id(predictor1)] = name1

            # FIXME(shangyint): This is an ugly hack to bind traces of
            # retry.module to retry
            # if isinstance(predictor1, Retry):
            #     predictor2name[id(predictor1.module)] = name1

            predictor2name[id(predictor2)] = name2

        self.name2predictor = name2predictor
        self.predictor2name = predictor2name

    def _bootstrap(self, *, max_bootstraps=None):
        max_bootstraps = max_bootstraps or self.max_bootstrapped_demos
        bootstrap_attempts = 0

        bootstrapped = {}
        self.name2traces = {name: [] for name in self.name2predictor}

        for example_idx, example in enumerate(tqdm.tqdm(self.trainset)):
            if len(bootstrapped) >= max_bootstraps:
                break

            for round_idx in range(self.max_rounds):
                bootstrap_attempts += 1

                if self._bootstrap_one_example(example, round_idx):
                    bootstrapped[example_idx] = True
                    break

        print(
            f"Bootstrapped {len(bootstrapped)} full traces after {example_idx} examples "
            f"for up to {self.max_rounds} rounds, amounting to {bootstrap_attempts} attempts."
        )

        # Unbootstrapped training examples

        self.validation = [x for idx, x in enumerate(self.trainset) if idx not in bootstrapped]
        random.Random(0).shuffle(self.validation)

        self.validation = self.validation

        # NOTE: Can't yet use evaluate because we need to trace *per example*
        # evaluate = Evaluate(program=self.teacher, metric=self.metric, num_threads=12)
        # score = evaluate(self.metric, display_table=False, display_progress=True)

    def _bootstrap_one_example(self, example, round_idx=0):
        name2traces = {}
        teacher = self.teacher
        predictor_cache = {}

        try:
            with settings.context(trace=[], **self.teacher_settings):
                lm = settings.lm
                # Use a fresh rollout with temperature=1.0 to bypass caches.
                lm = lm.copy(rollout_id=round_idx, temperature=1.0) if round_idx > 0 else lm
                new_settings = {"lm": lm} if round_idx > 0 else {}

                with settings.context(**new_settings):
                    for name, predictor in teacher.named_predictors():
                        predictor_cache[name] = predictor.demos
                        predictor.demos = [x for x in predictor.demos if x != example]

                    prediction = teacher(**example.inputs())
                    trace = settings.trace

                    for name, predictor in teacher.named_predictors():
                        predictor.demos = predictor_cache[name]

                if self.metric:
                    metric_val = self.metric(example, prediction, trace)
                    if self.metric_threshold:
                        success = metric_val >= self.metric_threshold
                    else:
                        success = metric_val
                else:
                    success = True
        except Exception as e:
            success = False
            with self.error_lock:
                self.error_count += 1
                current_error_count = self.error_count
            effective_max_errors = self.max_errors if self.max_errors is not None else settings.max_errors
            if current_error_count >= effective_max_errors:
                raise e
            log.error(f"Failed to run or to evaluate example {example} with {self.metric} due to {e}.")

        if success:
            for step in trace:
                predictor, inputs, outputs = step
                demo = Example(augmented=True, **inputs, **outputs)

                try:
                    predictor_name = self.predictor2name[id(predictor)]
                except KeyError:
                    continue  # FIXME: !

                name2traces[predictor_name] = name2traces.get(predictor_name, [])
                name2traces[predictor_name].append(demo)

            # Update the traces
            for name, demos in name2traces.items():
                # If there are multiple traces for the same predictor in the sample example,
                # sample 50/50 from the first N-1 traces or the last trace.
                if len(demos) > 1:
                    rng = random.Random(Hasher.hash(tuple(demos)))
                    demos = [rng.choice(demos[:-1]) if rng.random() < 0.5 else demos[-1]]
                self.name2traces[name].extend(demos)

        return success

    def _train(self):
        rng = random.Random(0)
        raw_demos = self.validation

        for name, predictor in self.student.named_predictors():
            augmented_demos = self.name2traces[name][: self.max_bootstrapped_demos]

            sample_size = min(self.max_labeled_demos - len(augmented_demos), len(raw_demos))
            sample_size = max(0, sample_size)

            raw_demos = rng.sample(raw_demos, sample_size)
            predictor.demos = augmented_demos + raw_demos

        return self.student
