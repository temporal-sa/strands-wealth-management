"""Model configuration for the Temporal + Strands worker.

``StrandsPlugin(models=MODELS)`` registers these factories on the worker. Each
factory is called lazily on first use (outside the workflow sandbox) and cached
for the worker's lifetime. ``TemporalAgent(model="gemini", ...)`` selects it.
"""

import os
from datetime import timedelta
from typing import Any, AsyncGenerator, Optional

from strands.models.gemini import GeminiModel
from strands.types.exceptions import ModelThrottledException
from strands.types.streaming import StreamEvent
from temporalio.exceptions import ApplicationError

from common.chaos_manager import ChaosFlag
from temporal_supervisor.chaos import inject_failure

MODEL_NAME = "gemini"
MODEL_ID = "gemini-2.5-flash"

TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "wealth-management-strands")
# Model calls run on their own task queue so they can be rate limited (and scaled)
# independently of the tools. See run_worker.py. TEMPORAL_TASK_QUEUE_LLM matches the
# other ports; TEMPORAL_LLM_TASK_QUEUE is still accepted for existing setups.
LLM_TASK_QUEUE = (
    os.getenv("TEMPORAL_TASK_QUEUE_LLM")
    or os.getenv("TEMPORAL_LLM_TASK_QUEUE")
    or f"{TASK_QUEUE}-llm"
)
# Task queue the open-account child workflow runs on. Defaults to the main queue,
# where run_worker.py registers OpenInvestmentAccountWorkflow. The worker start
# scripts set it from their optional argument to hand the child workflow to a
# separate worker (e.g. the Java worker in java/); run_worker.py then doesn't
# register the Python version.
OPEN_ACCOUNT_TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT") or TASK_QUEUE


def _optional_float(name: str) -> Optional[float]:
    value = os.getenv(name)
    return float(value) if value else None


# Proactive rate limiting: the most model calls per second the Temporal service
# will dispatch from LLM_TASK_QUEUE, across every worker. Unset means no limit.
LLM_MAX_CALLS_PER_SECOND = _optional_float("LLM_MAX_CALLS_PER_SECOND")

# How long a rate-limited model call waits before its next attempt, as if the
# provider had returned a Retry-After header.
RATE_LIMIT_RETRY_AFTER = timedelta(seconds=15)


def _rate_limited(message: str) -> ApplicationError:
    return ApplicationError(
        message,
        type="ModelRateLimited",
        next_retry_delay=RATE_LIMIT_RETRY_AFTER,
    )


class ChaosAwareGeminiModel(GeminiModel):
    """GeminiModel that honors the demo's chaos flags and handles throttling.

    Runs inside the plugin's model activity, so raising here fails the activity
    and Temporal retries the model call according to LLM_RETRY_POLICY.
    """

    async def stream(self, *args: Any, **kwargs: Any) -> AsyncGenerator[StreamEvent, None]:
        await inject_failure(ChaosFlag.LLM_DOWN, ConnectionError("Gemini API unavailable"))
        await inject_failure(
            ChaosFlag.LLM_RATE_LIMITED, _rate_limited("Gemini API rate limit exceeded (HTTP 429)")
        )
        try:
            async for event in super().stream(*args, **kwargs):
                yield event
        except ModelThrottledException as e:
            # A real 429 / RESOURCE_EXHAUSTED from Gemini gets the same treatment.
            raise _rate_limited(f"Gemini API throttled the request: {e}") from e


def gemini_factory() -> GeminiModel:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Run `source ./setgeminikey.sh` (see setgeminikey.example)."
        )
    return ChaosAwareGeminiModel(
        client_args={"api_key": api_key},
        model_id=MODEL_ID,
        params={"temperature": 0.2},
    )


MODELS = {MODEL_NAME: gemini_factory}
