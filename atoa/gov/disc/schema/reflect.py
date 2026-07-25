# atoa.gov.disc.schema.reflect
## @lineage: agent.atoa.disc.schema.reflect
## @lineage: atoa.agent.disc.schema.reflect
## @lineage: atoa.disc.schema.reflect
## @lineage: agent.disc.schema.reflect
## @lineage: agent.disc.reflect
import __future__
import abc
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Literal, Any, ClassVar
from pydantic import BaseModel, Field
from rich.text import Text

from eco.fiber.event.base import LLMConvertibleEvent
from arch.topos.bound.surge.disc import DiscMixin

class IterativeRefinementConfig(BaseModel):
    success_threshold: float = Field(
        default=0.6, 
        ge=0.0, 
        le=1.0,
        description="Minimum resonance threshold (0.0~1.0) required for topological convergence.",
    )
    max_iterations: int = Field(
        default=3, 
        ge=1,
        description="Maximum cyclic traversals before terminating the refinement manifold.",
    )

class ReflectorResult(BaseModel):
    ## @desc: Evaluated Topological State and Resonance Metrics
    THRESHOLD: ClassVar[float] = 0.5
    DISPLAY_THRESHOLD: ClassVar[float] = 0.2
    
    score: float = Field(
        description="Convergence probability vector (0.0 to 1.0) indicating goal proximity.",
        ge=0.0,
        le=1.0,
    )
    message: str | None = Field(description="Semantic translation of the underlying structural anomaly.")
    metadata: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Topological metadata of the evaluation space. "
            "Contains isolated event nodes and classified entropy features for projection."
        ),
    )

    @property
    def success(self) -> bool:
        ## @desc: Validates if the structural resonance breaches the minimum viability threshold
        return self.score >= ReflectorResult.THRESHOLD

    @staticmethod
    def _get_star_rating(score: float) -> str:
        filled_stars = round(score * 5)
        empty_stars = 5 - filled_stars
        return "★" * filled_stars + "☆" * empty_stars

    @staticmethod
    def _get_star_style(score: float) -> str:
        if score >= 0.6:
            return "green"
        elif score >= 0.4:
            return "yellow"
        else:
            return "red"

    @property
    def visualize(self) -> Text:
        ## @desc: Renders the topological status into a human-readable CLI projection
        content = Text()
        content.append("\n\n[RESONANCE] Convergence Likelihood: ", style="bold")

        stars = self._get_star_rating(self.score)
        style = self._get_star_style(self.score)
        percentage = self.score * 100
        content.append(stars, style=style)
        content.append(f" ({percentage:.1f}%)", style="dim")

        if self.metadata and "categorized_features" in self.metadata:
            categorized = self.metadata["categorized_features"]
            self._append_categorized_features(content, categorized)
        else:
            if self.message:
                content.append(f"\n  [ANALYSIS] {self.message}\n")
            else:
                content.append("\n")

        return content

    def _append_categorized_features(self, content: Text, categorized: dict[str, Any]) -> None:
        has_content = False
        
        agent_issues = categorized.get("agent_behavioral_issues", [])
        if agent_issues:
            content.append("\n  ")
            content.append("[ANOMALY] Structural Faults: ", style="bold")
            self._append_feature_list_inline(content, agent_issues)
            has_content = True

        user_patterns = categorized.get("user_followup_patterns", [])
        if user_patterns:
            content.append("\n  ")
            content.append("[PREDICTION] Trajectory Shifts: ", style="bold")
            self._append_feature_list_inline(content, user_patterns)
            has_content = True

        infra_issues = categorized.get("infrastructure_issues", [])
        if infra_issues:
            content.append("\n  ")
            content.append("[SUBSTRATE] Infra Integrity: ", style="bold")
            self._append_feature_list_inline(content, infra_issues)
            has_content = True

        other = categorized.get("other", [])
        if other:
            content.append("\n  ")
            content.append("[ENTROPY] Unclassified: ", style="bold")
            self._append_feature_list_inline(content, other, is_other=True)
            has_content = True

        if not has_content:
            content.append("\n")
        else:
            content.append("\n")

    def _append_feature_list_inline(
        self,
        content: Text,
        features: list[dict[str, Any]],
        is_other: bool = False,
    ) -> None:
        for i, feature in enumerate(features):
            display_name = feature.get("display_name", feature.get("name", "Unknown Node"))
            prob = feature.get("probability", 0.0)
            percentage = prob * 100

            if is_other:
                prob_style = "white"
            elif prob >= 0.7:
                prob_style = "red bold"
            elif prob >= 0.5:
                prob_style = "yellow"
            else:
                prob_style = "dim"

            if i > 0:
                content.append(" · ", style="dim")

            content.append(f"{display_name}", style="white")
            content.append(f" (prob {percentage:.0f}%)", style=prob_style)


class ReflectorBase(DiscMixin, abc.ABC):
    ## @desc: Abstract Topological Evaluator / Reflection Boundary
    mode: Literal["finish_and_message", "all_actions"] = Field(
        default="finish_and_message",
        description="Evaluation trigger phase: 'finish_and_message' (terminal node only) or 'all_actions' (continuous trajectory mapping).",
    )

    iterative_refinement: IterativeRefinementConfig | None = Field(
        default=None,
        description="Autonomous structural refinement loop. Triggers trajectory realignment if resonance falls below threshold.",
    )

    @abc.abstractmethod
    def evaluate(self, events: Sequence["LLMConvertibleEvent"], git_patch: str | None = None) -> ReflectorResult:
        pass

    def get_followup_prompt(self, reflector_result: ReflectorResult, iteration: int) -> str:
        ## @desc: Inject structural stimulus to realign the agent's divergent trajectory
        return (
            f"[SYSTEM] Trajectory Divergence Detected (Cycle: {iteration}, Resonance: {reflector_result.score:.2f}).\n"
            "Re-evaluate structural dependencies, isolate fractured nodes, and project a corrected trajectory."
        )