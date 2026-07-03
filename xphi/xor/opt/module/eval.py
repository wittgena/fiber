# xphi.xor.opt.module.eval
import csv
import importlib
import json
import types
from typing import TYPE_CHECKING, Any, Callable
if TYPE_CHECKING:
    import pandas as pd

import tqdm

from bound.channel.compat.switch.dsp.settings import settings
from bound.channel.compat.switch.dsp.context import get_dspy_context_propagator
from arch.xor.manifold.sample import Prediction
from arch.xor.manifold.sample import Sample
from xphi.xor.opt.callback.base import with_callbacks

from arch.proto.wrapper.opt import OptExecutor
from watcher.plane.emitter import get_emitter

try:
    from IPython.display import HTML
    from IPython.display import display as display
except ImportError:
    def display(obj: Any):
        """
        Display the specified Python object in the console.

        :param obj: The Python object to display.
        """
        print(obj)

    def HTML(x: str) -> str:  # noqa: N802
        """
        Obtain the HTML representation of the specified string.
        """
        # NB: This method exists purely for code compatibility with the IPython HTML() function in
        # environments where IPython is not available. In such environments where IPython is not
        # available, this method will simply return the input string.
        return x

log = get_emitter("evaluator.evaluate")


class EvaluationResult(Prediction):
    """
    A class that represents the result of an evaluation.
    It is a subclass of `Prediction` that contains the following fields

    - score: An float value (e.g., 67.30) representing the overall performance
    - results: a list of (example, prediction, score) tuples for each example in devset
    """

    def __init__(self, score: float, results: list[tuple["Example", "Example", Any]]):
        super().__init__(score=score, results=results)

    def __repr__(self):
        return f"EvaluationResult(score={self.score}, results=<list of {len(self.results)} results>)"


class Evaluate:
    """DSP Evaluate class.

    This class is used to evaluate the performance of a DSP program. Users need to provide a evaluation dataset and
    a metric function in order to use this class. This class supports parallel evaluation on the provided dataset.
    """

    def __init__(
        self,
        *,
        devset: list["Example"],
        metric: Callable | None = None,
        num_threads: int | None = None,
        display_progress: bool = False,
        display_table: bool | int = False,
        max_errors: int | None = None,
        provide_traceback: bool | None = None,
        failure_score: float = 0.0,
        save_as_csv: str | None = None,
        save_as_json: str | None = None,
        **kwargs,
    ):
        self.devset = devset
        self.metric = metric
        self.num_threads = num_threads
        self.display_progress = display_progress
        self.display_table = display_table
        self.max_errors = max_errors
        self.provide_traceback = provide_traceback
        self.failure_score = failure_score
        self.save_as_csv = save_as_csv
        self.save_as_json = save_as_json

        if "return_outputs" in kwargs:
            raise ValueError("`return_outputs` is no longer supported. Results are always returned inside the `results` field of the `EvaluationResult` object.")

    @with_callbacks
    def __call__(
        self,
        program: "Module",
        metric: Callable | None = None,
        devset: list["Example"] | None = None,
        num_threads: int | None = None,
        display_progress: bool | None = None,
        display_table: bool | int | None = None,
        callback_metadata: dict[str, Any] | None = None,
        save_as_csv: str | None = None,
        save_as_json: str | None = None,
    ) -> EvaluationResult:
        metric = metric if metric is not None else self.metric
        devset = devset if devset is not None else self.devset
        num_threads = num_threads if num_threads is not None else self.num_threads
        display_progress = display_progress if display_progress is not None else self.display_progress
        display_table = display_table if display_table is not None else self.display_table
        save_as_csv = save_as_csv if save_as_csv is not None else self.save_as_csv
        save_as_json = save_as_json if save_as_json is not None else self.save_as_json

        if callback_metadata:
            log.debug(f"Evaluate is called with callback metadata: {callback_metadata}")

        tqdm.tqdm._instances.clear()

        # [핵심 변경] OptExecutor 직접 호출하되, 상태 전파기(Context Propagator)를 반드시 주입
        executor = OptExecutor(
            num_threads=num_threads,
            disable_progress_bar=not display_progress,
            max_errors=(self.max_errors if self.max_errors is not None else settings.max_errors),
            provide_traceback=self.provide_traceback,
            compare_results=True,
            context_propagator=get_dspy_context_propagator()  # <-- DSPy 컨텍스트의 안전한 하위 스레드 주입
        )

        def process_item(example):
            prediction = program(**example.inputs())
            score = metric(example, prediction)
            return prediction, score

        results = executor.execute(process_item, devset)
        assert len(devset) == len(results)

        results = [((Prediction(), self.failure_score) if r is None else r) for r in results]
        results = [(example, prediction, score) for example, (prediction, score) in zip(devset, results, strict=False)]
        ncorrect, ntotal = sum(score for *_, score in results), len(devset)

        log.info(f"Average Metric: {ncorrect} / {ntotal} ({round(100 * ncorrect / ntotal, 1)}%)")

        if display_table:
            if importlib.util.find_spec("pandas") is not None:
                # Rename the 'correct' column to the name of the metric object
                metric_name = metric.__name__ if isinstance(metric, types.FunctionType) else metric.__class__.__name__
                # Construct a pandas DataFrame from the results
                result_df = self._construct_result_table(results, metric_name)

                self._display_result_table(result_df, display_table, metric_name)
            else:
                log.warning("Skipping table display since `pandas` is not installed.")

        if save_as_csv:
            metric_name = (
                metric.__name__
                if isinstance(metric, types.FunctionType)
                else metric.__class__.__name__
            )
            data = self._prepare_results_output(results, metric_name)

            with open(save_as_csv, "w", newline="") as csvfile:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for row in data:
                    writer.writerow(row)
        if save_as_json:
            metric_name = (
                metric.__name__
                if isinstance(metric, types.FunctionType)
                else metric.__class__.__name__
            )
            data = self._prepare_results_output(results, metric_name)
            with open(
                    save_as_json,
                    "w",
            ) as f:
                json.dump(data, f)

        return EvaluationResult(
            score=round(100 * ncorrect / ntotal, 2),
            results=results,
        )

    @staticmethod
    def _prepare_results_output(
            results: list[tuple["Example", "Example", Any]], metric_name: str
    ):
        return [
            (
                merge_dicts(example, prediction) | {metric_name: score}
                if prediction_is_dictlike(prediction)
                else example.toDict() | {"prediction": prediction, metric_name: score}
            )
            for example, prediction, score in results
        ]

    def _construct_result_table(
        self, results: list[tuple["Example", "Example", Any]], metric_name: str
    ) -> "pd.DataFrame":
        """
        Construct a pandas DataFrame from the specified result list.
        Let's not try to change the name of this method as it may be patched by external tracing tools.

        Args:
            results: The list of results to construct the result DataFrame from.
            metric_name: The name of the metric used for evaluation.

        Returns:
            The constructed pandas DataFrame.
        """
        import pandas as pd

        data = self._prepare_results_output(results, metric_name)

        # Truncate every cell in the DataFrame (DataFrame.applymap was renamed to DataFrame.map in Pandas 2.1.0)
        result_df = pd.DataFrame(data)
        result_df = result_df.map(truncate_cell) if hasattr(result_df, "map") else result_df.applymap(truncate_cell)

        return result_df.rename(columns={"correct": metric_name})

    def _display_result_table(self, result_df: "pd.DataFrame", display_table: bool | int, metric_name: str):
        """
        Display the specified result DataFrame in a table format.

        Args:
            result_df: The result DataFrame to display.
            display_table: Whether to display the evaluation results in a table.
                If a number is passed, the evaluation results will be truncated to that number before displayed.
            metric_name: The name of the metric used for evaluation.
        """
        if isinstance(display_table, bool):
            df_to_display = result_df.copy()
            truncated_rows = 0
        else:
            df_to_display = result_df.head(display_table).copy()
            truncated_rows = len(result_df) - display_table

        df_to_display = stylize_metric_name(df_to_display, metric_name)

        display_dataframe(df_to_display)

        if truncated_rows > 0:
            # Simplified message about the truncated rows
            message = f"""
            <div style='
                text-align: center;
                font-size: 16px;
                font-weight: bold;
                color: #555;
                margin: 10px 0;'>
                ... {truncated_rows} more rows not displayed ...
            </div>
            """
            display(HTML(message))


def prediction_is_dictlike(prediction):
    # Downstream logic for displaying dictionary-like predictions depends solely on the predictions
    # having a method called `items()` for iterating through key/value pairs
    return hasattr(prediction, "items") and callable(prediction.items)


def merge_dicts(d1, d2) -> dict:
    # Convert to dict if objects have toDict method (e.g., Example objects)
    if hasattr(d1, "toDict"):
        d1 = d1.toDict()
    if hasattr(d2, "toDict"):
        d2 = d2.toDict()

    merged = {}
    for k, v in d1.items():
        if k in d2:
            merged[f"example_{k}"] = v
        else:
            merged[k] = v

    for k, v in d2.items():
        if k in d1:
            merged[f"pred_{k}"] = v
        else:
            merged[k] = v

    return merged


def truncate_cell(content) -> str:
    """Truncate content of a cell to 25 words."""
    words = str(content).split()
    if len(words) > 25:
        return " ".join(words[:25]) + "..."
    return content


def stylize_metric_name(df: "pd.DataFrame", metric_name: str) -> "pd.DataFrame":
    """
    Stylize the cell contents of a pandas DataFrame corresponding to the specified metric name.

    :param df: The pandas DataFrame for which to stylize cell contents.
    :param metric_name: The name of the metric for which to stylize DataFrame cell contents.
    """
    def format_metric(x):
        if isinstance(x, float):
            return f"✔️ [{x:.3f}]"
        elif x is not None:
            return f"✔️ [{x}]"
        else:
            return ""
    df[metric_name] = df[metric_name].apply(format_metric)
    return df


def display_dataframe(df: "pd.DataFrame"):
    """
    Display the specified Pandas DataFrame in the console.

    :param df: The Pandas DataFrame to display.
    """
    import pandas as pd

    if is_in_ipython_notebook_environment():
        display(configure_dataframe_for_ipython_notebook_display(df))
    else:
        # Pretty print the DataFrame to the console
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None
        ):  # more options can be specified also
            print(df)


def configure_dataframe_for_ipython_notebook_display(df: "pd.DataFrame") -> "pd.DataFrame":
    """Set various pandas display options for DataFrame in an IPython notebook environment."""
    import pandas as pd

    pd.options.display.max_colwidth = 70
    return df


def is_in_ipython_notebook_environment():
    """
    Check if the current environment is an IPython notebook environment.

    :return: True if the current environment is an IPython notebook environment, False otherwise.
    """
    try:
        from IPython import get_ipython
        return "IPKernelApp" in getattr(get_ipython(), "config", {})
    except ImportError:
        return False