"""Strands agent definitions for the wealth management demo.

Mirrors the ADK demo's supervisor/sub-agent hierarchy using Strands' idiomatic
"agents as tools" pattern: the supervisor is an orchestrator agent whose tools
are the specialized agents themselves. The supervisor collects the client ID
once and delegates each request to the appropriate specialist, passing the
client ID along.
"""

import os
from functools import lru_cache

from strands import Agent, tool
from strands.models.gemini import GeminiModel

from strands_supervisor.approval import ConfirmDestructive

from common.agent_constants import (
    BENE_INSTRUCTIONS,
    INVEST_INSTRUCTIONS,
    SUPERVISOR_INSTRUCTIONS,
)
from strands_supervisor import tools as wm_tools

MODEL_ID = "gemini-2.5-flash"


@lru_cache(maxsize=1)
def build_model() -> GeminiModel:
    """Build (and cache) the Gemini model used by every agent."""
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


# ---------------------------------------------------------------------------
# Specialized agents exposed as tools (the "agents as tools" pattern)
# ---------------------------------------------------------------------------

@tool
def beneficiary_assistant(client_id: str, request: str) -> str:
    """Handle a beneficiary-related request (list, add, or delete beneficiaries).

    Args:
        client_id: The customer's client ID.
        request: A natural-language description of what the customer wants done with their beneficiaries.

    Returns:
        The beneficiary agent's response.
    """
    agent = Agent(
        model=build_model(),
        system_prompt=BENE_INSTRUCTIONS,
        tools=[
            wm_tools.list_beneficiaries,
            wm_tools.add_beneficiary,
            wm_tools.delete_beneficiary,
        ],
        callback_handler=None,
        # Everything runs freely except deleting a beneficiary, which prompts the
        # human for y/n confirmation on the CLI before proceeding.
        interventions=[
            ConfirmDestructive(allowed_tools=["*", "!delete_beneficiary"], ask="stdio"),
        ],
    )
    result = agent(f"The client ID is {client_id}. {request}")
    return str(result)


@tool
def investment_assistant(client_id: str, request: str) -> str:
    """Handle an investment-account request (list, open, or close investment accounts).

    Args:
        client_id: The customer's client ID.
        request: A natural-language description of what the customer wants done with their investment accounts.

    Returns:
        The investment agent's response.
    """
    agent = Agent(
        model=build_model(),
        system_prompt=INVEST_INSTRUCTIONS,
        tools=[
            wm_tools.list_investments,
            wm_tools.open_investment,
            wm_tools.close_investment,
        ],
        callback_handler=None,
        # Everything runs freely except closing an account, which prompts the
        # human for y/n confirmation on the CLI before proceeding.
        interventions=[
            ConfirmDestructive(allowed_tools=["*", "!close_investment"], ask="stdio"),
        ],
    )
    result = agent(f"The client ID is {client_id}. {request}")
    return str(result)


# ---------------------------------------------------------------------------
# Supervisor (orchestrator) agent
# ---------------------------------------------------------------------------

def build_supervisor() -> Agent:
    """Build the supervisor agent that delegates to the specialized agents."""
    return Agent(
        model=build_model(),
        system_prompt=SUPERVISOR_INSTRUCTIONS,
        tools=[beneficiary_assistant, investment_assistant],
        callback_handler=None,
    )
