"""Model configuration for the Temporal + Strands worker.

``StrandsPlugin(models=MODELS)`` registers these factories on the worker. Each
factory is called lazily on first use (outside the workflow sandbox) and cached
for the worker's lifetime. ``TemporalAgent(model="gemini", ...)`` selects it.
"""

import os

from strands.models.gemini import GeminiModel

MODEL_NAME = "gemini"
MODEL_ID = "gemini-2.5-flash"

TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "wealth-management-strands")


def gemini_factory() -> GeminiModel:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Run `source ./setgeminikey.sh` (see setgeminikey.example)."
        )
    return GeminiModel(
        client_args={"api_key": api_key},
        model_id=MODEL_ID,
        params={"temperature": 0.2},
    )


MODELS = {MODEL_NAME: gemini_factory}
